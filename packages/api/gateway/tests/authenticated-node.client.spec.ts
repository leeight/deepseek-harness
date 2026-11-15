/** Non-page Clients use one credential across unary HTTP and the Remote stream mux. */
import { once } from 'node:events'
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http'
import { Context } from '@deepseek-ai/cordis'
import { installConnection, MUX_CLOSED, MUX_CONNECTING, MUX_OPEN, type ConnectionHandle } from '@deepseek-ai/dsh-client-connection/client'
import TypertRegistry from '@deepseek-ai/dsh-typert-registry'
import type { RemoteResult, RemoteStreamHandle, TypertRemoteNamespace } from '@deepseek-ai/dsh-typert-protocol'
import { describe, expect, it, vi } from 'vitest'
import { z } from 'zod'
import { apply, inject } from '../src/client/index.ts'
import { encodeRemoteStreamFrame, parseRemoteStreamClientMessage } from '../src/stream-protocol.ts'

declare module '@deepseek-ai/dsh-typert-protocol' {
  interface TypertRemoteMap {
    'nodeProbe/read': () => Promise<RemoteResult<string>>
    'nodeProbe/watch': () => RemoteStreamHandle<string, never>
  }
  interface TypertRemoteNamespaceMap {
    nodeProbe: TypertRemoteNamespace<'nodeProbe'>
  }
}

/** Session id the fixture downlink advertises and accepts on its uplink. */
const SESSION = 'node-session'

describe('authenticated Node Client', () => {
  it.each(['none', 'answer'] as const)('finishes unary HTTP while the %s mux downlink is still connecting', async (interactions) => {
    const fixture = await nodeFixture(interactions)
    try {
      await fixture.upgrading.promise
      await expect(fixture.ctx.remote.nodeProbe.read()).resolves.toEqual({ ok: true, value: 'authenticated' })
      expect(fixture.downlink.readyState).toBe(MUX_CONNECTING)
      expect(fixture.cookies).toEqual(['fixture=credential', 'fixture=credential'])
      expect(fixture.endpoints).toEqual([])
      fixture.upgrade()
      if (interactions === 'answer') {
        await vi.waitFor(() => { expect(fixture.connection.generation.getSnapshot()?.host.home).toBe('/home/node') })
        expect(fixture.downlink.readyState).toBe(MUX_OPEN)
        expect(fixture.endpoints).toEqual(['$events'])
      } else {
        expect(fixture.connection.generation.getSnapshot()).toBeUndefined()
        expect(() => fixture.ctx.remote.$on('fixture/approval' as never, () => {})).toThrow('interactions=answer')
        const open = vi.fn(() => fixture.ctx.remote.nodeProbe.watch())
        expect(() => fixture.ctx.remote.$stream({ name: 'passive', open, ended: () => new Error('ended') }))
          .toThrow('$stream requires interactions=answer')
        expect(open).not.toHaveBeenCalled()
        expect(fixture.endpoints).toEqual([])
      }
      const stream = fixture.ctx.remote.nodeProbe.watch()
      const iterator = stream[Symbol.asyncIterator]()
      await expect(iterator.next()).resolves.toEqual({ done: false, value: 'stream item' })
      expect(fixture.downlink.readyState).toBe(MUX_OPEN)
      if (interactions === 'none') {
        fixture.downlink.close()
        await expect(iterator.next()).rejects.toThrow('api gateway: Remote stream carrier closed')
        await expect(fixture.ctx.remote.nodeProbe.watch()[Symbol.asyncIterator]().next())
          .rejects.toThrow('api gateway: Remote stream carrier closed')
      } else {
        stream.dispose()
        await iterator.return?.()
      }
    } finally {
      await fixture.close()
    }
  })

  it('finishes unary with no mux downlink and contains Client cancellation until physical close', async () => {
    const fixture = await nodeFixture('none')
    try {
      await fixture.upgrading.promise
      await expect(fixture.ctx.remote.nodeProbe.read()).resolves.toEqual({ ok: true, value: 'authenticated' })
      expect(fixture.downlink.readyState).toBe(MUX_CONNECTING)
      // Disposing the Client cancels the withheld downlink; the carrier contains that
      // cancellation, so nothing escapes as an unhandled rejection.
      await fixture.ctx.fiber.dispose()
      await vi.waitFor(() => { expect(fixture.downlink.readyState).toBe(MUX_CLOSED) })
      expect(fixture.endpoints).toEqual([])
    } finally {
      await fixture.close()
    }
  })

  it('contains a failed transport without blocking unary HTTP', async () => {
    const ctx = new Context()
    installConnection(ctx, {
      transport: {
        streamBaseUrl: 'http://127.0.0.1/',
        fetch: async () => { throw new Error('downlink factory failed') },
        rpc: { call: async () => ({ ok: true, value: 'local HTTP substitute' }) },
      },
    })
    try {
      await ctx.plugin(TypertRegistry)
      await ctx.plugin({ apply, inject })
      await mount(ctx)
      await expect(ctx.remote.nodeProbe.read()).resolves.toEqual({ ok: true, value: 'local HTTP substitute' })
      // The carrier reports the refused downlink as a retryable carrier failure rather than
      // the transport's own error, so domains decide whether a retry is worthwhile.
      await expect(ctx.remote.nodeProbe.watch()[Symbol.asyncIterator]().next()).rejects.toMatchObject({
        name: 'RemoteStreamCarrierError',
        message: 'api gateway: Remote stream carrier failed to open',
        code: 'gateway/carrier-lost',
      })
    } finally {
      await ctx.fiber.dispose()
    }
  })
})

/** Downlink state a spec observes while the fixture withholds or ends the response. */
interface DownlinkFixture {
  /** Carrier state mirroring `MUX_CONNECTING`, `MUX_OPEN` and `MUX_CLOSED`. */
  readyState: number
  /** End the downlink as a transport loss, as a dropped connection does. */
  close(): void
}

async function nodeFixture(interactions: 'none' | 'answer') {
  const ctx = new Context()
  const cookies: Array<string | undefined> = []
  const endpoints: string[] = []
  const upgrading = Promise.withResolvers<undefined>()
  const downlink: DownlinkFixture = {
    readyState: MUX_CONNECTING,
    close() {
      if (held === undefined) open?.end()
      else held.response.destroy()
      downlink.readyState = MUX_CLOSED
    },
  }
  let held: { readonly request: IncomingMessage; readonly response: ServerResponse } | undefined
  let open: ServerResponse | undefined
  let upgrade = (): void => { throw new Error('fixture downlink has not arrived') }
  const server = createServer((request, response) => {
    cookies.push(request.headers.cookie)
    const target = new URL(request.url ?? '/', 'http://dsh.invalid')
    if (target.pathname.endsWith('/api/remote.mux')) {
      if (request.method === 'GET') withhold(request, response)
      else void uplink(request, response, target).catch((error: unknown) => { response.destroy(asError(error)) })
      return
    }
    void unary(request, response).catch((error: unknown) => { response.destroy(asError(error)) })
  })
  const withhold = (request: IncomingMessage, response: ServerResponse): void => {
    held = { request, response }
    response.once('close', () => {
      if (held?.response === response) held = undefined
      downlink.readyState = MUX_CLOSED
    })
    upgrade = () => {
      const accepted = held
      if (accepted === undefined) throw new Error('fixture downlink has not arrived')
      held = undefined
      accepted.response.writeHead(200, {
        'cache-control': 'no-store',
        'content-type': 'text/event-stream',
        'x-dsh-mux-session': SESSION,
      })
      accepted.response.flushHeaders()
      open = accepted.response
      downlink.readyState = MUX_OPEN
    }
    upgrading.resolve(undefined)
  }
  const uplink = async (request: IncomingMessage, response: ServerResponse, target: URL): Promise<void> => {
    const body = await read(request)
    const accepted = open
    if (target.searchParams.get('session') !== SESSION || accepted === undefined) {
      response.writeHead(404)
      response.end()
      return
    }
    for (const text of body.split('\n')) {
      if (text === '') continue
      const frame = parseRemoteStreamClientMessage(text)
      if (frame.type !== 'open') continue
      endpoints.push(frame.endpoint)
      accepted.write(encodeRemoteStreamFrame(JSON.stringify({
        type: 'item',
        streamId: frame.streamId,
        value: frame.endpoint === '$events'
          ? { type: 'ready', clientId: 'node-client', host: { home: '/home/node' } }
          : 'stream item',
      })))
    }
    response.writeHead(204)
    response.end()
  }
  const unary = async (request: IncomingMessage, response: ServerResponse): Promise<void> => {
    const envelope = z.object({ rpcId: z.string() }).parse(JSON.parse(await read(request)))
    response.setHeader('content-type', 'application/json')
    response.end(JSON.stringify({ type: 'server-response', rpcId: envelope.rpcId, result: { ok: true, value: 'authenticated' } }))
  }
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  const address = server.address()
  if (address === null || typeof address === 'string') throw new Error('fixture did not acquire a port')
  const base = `http://127.0.0.1:${address.port}/mount/`
  const close = async (): Promise<void> => {
    await ctx.fiber.dispose()
    downlink.close()
    server.closeAllConnections()
    await new Promise<void>((resolve, reject) => {
      server.close((error) => { if (error === undefined) resolve(); else reject(error) })
    })
  }
  try {
    installConnection(ctx, {
      interactions,
      transport: {
        streamBaseUrl: base,
        fetch: (input, init) => fetch(new URL(input, base), {
          ...init, headers: { ...Object.fromEntries(new Headers(init.headers)), cookie: 'fixture=credential' },
        }),
      },
    })
    await ctx.plugin(TypertRegistry)
    await ctx.plugin({ apply, inject })
    await mount(ctx)
    return {
      ctx, connection: ctx.get('connection') as ConnectionHandle, cookies, endpoints, upgrading, downlink,
      upgrade: () => { upgrade() }, close,
    }
  } catch (error) {
    await close()
    throw error
  }
}

async function read(request: IncomingMessage): Promise<string> {
  let body = ''
  for await (const chunk of request) body += String(chunk)
  return body
}

function asError(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}

async function mount(ctx: Context): Promise<void> {
  await ctx.remote.$mount({
    package: '@fixture/node', descriptors: ['read', 'watch'].map(method => ({
      id: `@fixture/node#nodeProbe/${method}`, service: 'nodeProbe', namespace: 'nodeProbe', method,
      invocation: { kind: 'direct' }, parameters: [], ...(method === 'watch' ? { mode: 'stream' as const } : {}),
      result: { mode: 'strict', typeSymbol: '@fixture#String', create: () => z.string() },
    })),
  })
}
