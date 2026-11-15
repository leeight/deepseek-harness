/** Connect lifecycle and event ownership through real Loader, HTTP and the Remote stream mux. */
import { randomUUID } from 'node:crypto'
import { once } from 'node:events'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import type { IncomingMessage } from 'node:http'
import type { Duplex } from 'node:stream'
import { tmpdir } from 'node:os'
import { basename, dirname, isAbsolute, join, resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { Context } from '@deepseek-ai/cordis'
import Loader from '@deepseek-ai/cordis-plugin-loader'
import Include from '@deepseek-ai/cordis-plugin-include'
import * as Connection from '@deepseek-ai/dsh-client-connection'
import * as Token from '@deepseek-ai/dsh-host-authentication-token'
import { BrowserAuth } from '@deepseek-ai/dsh-host-authentication-token/src/browser-auth.ts'
import LocalCredentials from '@deepseek-ai/dsh-credentials-local'
import WebServer from '@deepseek-ai/dsh-host-webserver'
import * as FrontendStatic from '@deepseek-ai/dsh-host-frontend-static'
import TypertRegistry from '@deepseek-ai/dsh-typert-registry'
import TypertGateway from '@deepseek-ai/dsh-api-gateway'
import RemoteAccess from '@deepseek-ai/dsh-api-remote-access'
import { afterEach, describe, expect, it, vi } from 'vitest'
import WebSocket, { WebSocketServer } from 'ws'
import { z } from 'zod'
import { parseRemoteStreamServerMessage, type RemoteStreamServerMessage } from '../src/stream-protocol.ts'
import { RemoteStreamMuxServer } from '../src/stream-server.ts'
import { MuxSocket } from './mux-socket.ts'
import OperatorConnectFixture from './operator-connect-fixture.ts'

interface SocketClient {
  readonly socket: MuxSocket
  readonly closed: Promise<void>
  readonly frames: RemoteStreamServerMessage[]
  readonly errors: Error[]
}
interface UpgradeClient {
  readonly socket: WebSocket
  readonly closed: Promise<void>
  readonly errors: Error[]
}
interface Host {
  readonly ctx: Context
  readonly origin: string
  readonly fixture: OperatorConnectFixture
}
const roots: string[] = []
const hosts: Host[] = []
const clients: SocketClient[] = []
const upgrades: UpgradeClient[] = []
const readySchema = z.object({ type: z.literal('ready'), clientId: z.string(), host: z.object({ home: z.string() }) })
const deliverySchema = z.object({
  type: z.literal('waterfall'), eventId: z.string(), request: z.object({ prompt: z.string() }),
})

afterEach(async () => {
  for (const host of hosts) {
    host.fixture.streamCleanup.resolve(undefined)
    for (const finish of host.fixture.holds.values()) finish()
  }
  for (const client of clients.splice(0)) {
    if (client.socket.readyState !== MuxSocket.CLOSED) client.socket.terminate()
    await client.closed
  }
  for (const client of upgrades.splice(0)) {
    if (client.socket.readyState !== WebSocket.CLOSED) client.socket.terminate()
    await client.closed
  }
  await Promise.all(hosts.splice(0).map(host => host.ctx.fiber.dispose()))
  for (const root of roots.splice(0)) {
    if (!isAbsolute(root) || dirname(root) !== resolve(tmpdir()) || !basename(root).startsWith('dsh-operator-connect-')) {
      throw new Error(`refusing unexpected fixture cleanup path: ${root}`)
    }
    await rm(root, { recursive: true, force: true })
  }
})

describe('operator connect through Loader composition', () => {
  it('admits HTTP before attachment and releases the Operator before slow stream cleanup', async () => {
    const host = await boot()
    const cookie = await exchange(host, 'none')
    await success(host, cookie, 'fixture/identity', { label: 'before' })
    expect(call(host, 'before').peer.interactions).toBe('none')
    expect(call(host, 'before').peer === host.ctx.connection.browserPeer).toBe(false)
    const client = await mux(host, cookie)
    expect(await refusedMux(host, cookie)).toBe(409)
    await success(host, cookie, 'fixture/identity', { label: 'owner-alive' })
    host.fixture.delayStreamCleanup = true
    open(client, 'follow', 'fixture/follow', { label: 'follow' })
    await frame(client, 'follow', 'item')
    await close(client)
    await vi.waitFor(() => { expect(call(host, 'follow').signal.aborted).toBe(true) })
    expect(call(host, 'follow').finished).toBe(false)
    expect((await rpc(host, cookie, 'fixture/identity', { label: 'after' })).status).toBe(401)
    expect(await refusedMux(host, cookie)).toBe(401)
    expect(host.fixture.calls.has('after')).toBe(false)
    await vi.waitFor(() => { expect(host.fixture.effects.size).toBe(0) })
    host.fixture.streamCleanup.resolve(undefined)
    await vi.waitFor(() => { expect(call(host, 'follow').finished).toBe(true) })
  })

  it('does not claim or revoke an API Peer for non-mux WebSocket upgrades', async () => {
    const host = await boot()
    const inspector = new WebSocketServer({ noServer: true })
    host.ctx.effect(() => () => new Promise<void>((resolve, reject) => {
      inspector.close((error) => {
        if (error) reject(error)
        else resolve()
      })
    }), 'fixture: non-mux WebSocket server')
    const peers: Array<Connection.PeerScope | undefined> = []
    host.ctx.effect(() => host.ctx.webServer.registerUpgrade({
      path: '/inspector/devtools/cdp', auth: 'required',
      handler(request, socket, head, peer) {
        peers.push(peer)
        inspector.handleUpgrade(request, socket, head, () => {})
      },
    }), 'fixture: non-mux upgrade route')
    const cookie = await exchange(host, 'none')
    await success(host, cookie, 'fixture/identity', { label: 'non-mux-peer' })
    const peer = call(host, 'non-mux-peer').peer

    const first = upgradeClient(host, cookie, '/inspector/devtools/cdp')
    await once(first.socket, 'open')
    const api = await mux(host, cookie, '/api/remote.mux?fixture=scope')
    await close(first)
    expect(peer.signal.aborted).toBe(false)
    await success(host, cookie, 'fixture/identity', { label: 'after-cdp-close' })

    const second = upgradeClient(host, cookie, '/inspector/devtools/cdp')
    await once(second.socket, 'open')
    await close(second)
    expect(peers).toEqual([peer, peer])
    expect(peer.signal.aborted).toBe(false)
    expect(await refusedMux(host, cookie)).toBe(409)
    await close(api)
    expect((await rpc(host, cookie, 'fixture/identity', { label: 'after-mux-close' })).status).toBe(401)
  })

  it('uses the Peer WebServer authenticated without a second provider pass', async () => {
    const host = await boot()
    const peer = host.ctx.connection.createPeer({ principal: { kind: 'owner' } })
    const authenticate = vi.fn(() => peer)
    const unregister = host.ctx.connection.authentication.register({ authenticate })
    try {
      const client = await mux(host, 'fixture=credential')
      expect(authenticate).toHaveBeenCalledOnce()
      await close(client)
      expect(peer.signal.aborted).toBe(false)
      await peer.dispose()
      expect(peer.signal.aborted).toBe(true)
    } finally { await unregister() }
  })

  it('refuses the downlink with 503 when authentication providers fail', async () => {
    const host = await boot()
    const failure = new Error('fixture authentication unavailable')
    const authenticate = vi.fn(() => { throw failure })
    const unregister = host.ctx.connection.authentication.register({ authenticate })
    try {
      const response = await fetch(`${host.origin}/api/remote.mux`, {
        headers: { cookie: 'unrecognized=credential', accept: 'text/event-stream' },
      })
      expect(response.status).toBe(503)
      expect(response.headers.get('cache-control')).toBe('no-store')
      expect(response.headers.get('x-dsh-mux-session')).toBeNull()
      expect(await response.text()).toBe('authentication unavailable')
      expect(authenticate).toHaveBeenCalledOnce()
      expect(host.fixture.calls.size).toBe(0)
    } finally {
      await unregister()
    }
    expect(await refusedMux(host, 'unrecognized=credential')).toBe(401)
    await close(await mux(host, await exchange(host, 'none')))
  })

  it('reports failed Operator release without retaining its credential or blocking shutdown', async () => {
    const host = await boot()
    const cookie = await exchange(host, 'none')
    await success(host, cookie, 'fixture/identity', { label: 'release-failure' })
    const peer = call(host, 'release-failure').peer
    const dispose = peer.dispose.bind(peer)
    const failure = new Error('fixture Operator disposal failure')
    const disposing = vi.spyOn(peer, 'dispose').mockImplementationOnce(async () => {
      await dispose()
      throw failure
    })
    const report = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      await close(await mux(host, cookie))
      await vi.waitFor(() => {
        expect(report).toHaveBeenCalledWith('[host-authentication-token] Peer release failed:', failure)
      })
      expect((await rpc(host, cookie, 'fixture/identity', { label: 'refused' })).status).toBe(401)
      expect(host.fixture.calls.has('refused')).toBe(false)
      expect(host.fixture.effects.size).toBe(0)
      await host.ctx.fiber.dispose()
      expect(disposing).toHaveBeenCalledOnce()
    } finally {
      disposing.mockRestore()
      report.mockRestore()
    }
  })

  it.each([false, true])('frees the mux session when the request socket dies before the downlink is claimed (already closed: %s)', async (alreadyClosed) => {
    const host = await boot()
    const cookie = await exchange(host, 'none')
    await success(host, cookie, 'fixture/identity', { label: 'before-destroy' })
    const first = call(host, 'before-destroy').peer
    let armed = true
    const remove = host.ctx.on('webserver/request', async (request, _response, next) => {
      if (!armed || request.url?.startsWith('/api/remote.mux') !== true) {
        await next()
        return
      }
      armed = false
      expect(host.ctx.connection.admit({ headers: { host: new URL(host.origin).host, cookie } }))
        .toMatchObject({ peer: first })
      const closed = new Promise<void>((resolve) => { request.socket.once('close', () => { resolve() }) })
      request.socket.destroy()
      if (alreadyClosed) await closed
      await next()
    }, { prepend: true })
    const downlink = vi.spyOn(RemoteStreamMuxServer.prototype, 'handleDownlink')
    try {
      const client = socketClient(host, cookie)
      await client.closed
      expect(client.socket.readyState).toBe(MuxSocket.CLOSED)
      expect(client.socket.sessionId).toBeUndefined()
      expect(downlink).not.toHaveBeenCalled()
      // A request that never opened a downlink owns no Peer, so the credential stays admitted
      // and the freed session slot accepts the next downlink.
      await success(host, cookie, 'fixture/identity', { label: 'after' })
      await close(await mux(host, cookie))
    } finally {
      remove()
      downlink.mockRestore()
    }
  })

  it('delegates an anonymous upgrade without claiming its socket', async () => {
    const host = await boot()
    const handler = vi.fn((_request: IncomingMessage, socket: Duplex, _head: Buffer, peer?: Connection.PeerScope) => {
      expect(peer).toBeUndefined()
      socket.destroy()
    })
    const remove = host.ctx.webServer.registerUpgrade({ path: '/anonymous-upgrade', auth: 'anonymous', handler })
    try {
      const client = upgradeClient(host, '', '/anonymous-upgrade')
      await client.closed
      expect(handler).toHaveBeenCalledOnce()
    } finally { remove() }
  })

  it('refuses the downlink and keeps the credential when the stream handler throws', async () => {
    const host = await boot()
    const cookie = await exchange(host, 'none')
    const failure = new Error('fixture Remote stream downlink failure')
    const downlink = vi.spyOn(RemoteStreamMuxServer.prototype, 'handleDownlink').mockImplementationOnce(() => {
      throw failure
    })
    const report = vi.fn()
    const remove = host.ctx.logger.exporter({ export: report })
    try {
      const client = socketClient(host, cookie)
      await client.closed
      expect(client.socket.readyState).toBe(MuxSocket.CLOSED)
      expect(client.socket.sessionId).toBeUndefined()
      expect(client.errors.map(error => error.message)).toEqual(['fixture mux downlink refused with status 400'])
      await vi.waitFor(() => { expect(report).toHaveBeenCalledWith(expect.objectContaining({ type: 'warn', args: [failure] })) })
      // A refused downlink never owned the Peer, so the credential stays admitted.
      await success(host, cookie, 'fixture/identity', { label: 'after' })
      await close(await mux(host, await exchange(host, 'none')))
    } finally {
      downlink.mockRestore()
      await remove()
    }
  })

  it('refuses admission while a failed downlink is still releasing its Peer', async () => {
    const host = await boot()
    const cookie = await exchange(host, 'none')
    await success(host, cookie, 'fixture/identity', { label: 'error-owner' })
    const peer = call(host, 'error-owner').peer
    const first = await mux(host, cookie)
    const finish = Promise.withResolvers<undefined>()
    const dispose = peer.dispose.bind(peer)
    const disposing = vi.spyOn(peer, 'dispose').mockImplementation(async () => {
      await finish.promise
      await dispose()
    })
    try {
      await close(first)
      await vi.waitFor(() => {
        expect(host.ctx.connection.admit({ headers: { host: new URL(host.origin).host, cookie } }))
          .toEqual({ rejection: 401 })
      })
      expect(peer.signal.aborted).toBe(false)
      expect(disposing).toHaveBeenCalledOnce()
    } finally {
      finish.resolve(undefined)
      disposing.mockRestore()
    }
    await vi.waitFor(() => { expect(peer.signal.aborted).toBe(true) })
  })

  it('awaits Token-owned downlink cleanup on unload without claiming another provider downlink', async () => {
    const host = await boot()
    const cookie = await exchange(host, 'none')
    await success(host, cookie, 'fixture/identity', { label: 'unload-owner' })
    const peer = call(host, 'unload-owner').peer
    const owned = await mux(host, cookie)
    const external = host.ctx.connection.createPeer({ principal: { kind: 'owner' } })
    Object.defineProperty(external, 'id', { value: peer.id })
    const unregister = host.ctx.connection.authentication.register({ authenticate: () => external })
    const other = await mux(host, 'fixture=external')
    await unregister()
    const finish = Promise.withResolvers<undefined>()
    const dispose = peer.dispose.bind(peer)
    const disposing = vi.spyOn(peer, 'dispose').mockImplementation(async () => {
      await finish.promise
      await dispose()
    })
    const token = [...host.ctx.loader.entries()].find(entry => entry.options.name === 'cordis:@deepseek-ai/dsh-host-authentication-token')
    if (token?.fiber === undefined) throw new Error('Token provider did not activate')
    const entered = Promise.withResolvers<undefined>()
    const resume = Promise.withResolvers<undefined>()
    const remove = host.ctx.on('webserver/request', async (request, _response, next) => {
      if (request.url?.startsWith('/api/remote.mux') !== true) {
        await next()
        return
      }
      entered.resolve(undefined)
      await resume.promise
      await next()
    }, { prepend: true })
    const late = socketClient(host, cookie)
    await entered.promise
    let settled = false
    const unloading = token.fiber.dispose().then(() => { settled = true })
    try {
      await owned.closed
      expect(settled).toBe(false)
      resume.resolve(undefined)
      await late.closed
      expect(late.socket.readyState).toBe(MuxSocket.CLOSED)
      expect(late.socket.sessionId).toBeUndefined()
      expect(other.socket.readyState).toBe(MuxSocket.OPEN)
      expect(external.signal.aborted).toBe(false)
      expect(host.ctx.connection.admit({ headers: { host: new URL(host.origin).host, cookie } }))
        .toEqual({ rejection: 401 })
    } finally {
      resume.resolve(undefined)
      finish.resolve(undefined)
      remove()
      await unloading
      disposing.mockRestore()
    }
    expect(peer.signal.aborted).toBe(true)
    await close(other)
    await external.dispose()
  })

  it('lets an admitted unary finish after its lifecycle downlink closes', async () => {
    const host = await boot()
    const cookie = await exchange(host, 'none')
    const client = await mux(host, cookie)
    const pending = rpc(host, cookie, 'fixture/hold', { label: 'inflight' })
    await vi.waitFor(() => { expect(host.fixture.holds.has('inflight')).toBe(true) })
    await close(client)
    expect((await rpc(host, cookie, 'fixture/identity', { label: 'refused' })).status).toBe(401)
    expect(call(host, 'inflight').signal.aborted).toBe(false)
    expect(call(host, 'inflight').finished).toBe(false)
    host.fixture.holds.get('inflight')?.()
    await expect(pending).resolves.toMatchObject({ status: 200, body: { result: { ok: true, value: 'completed' } } })
    expect(call(host, 'inflight').signal.aborted).toBe(false)
  })

  it('forbids passive subscription and results, enforces stream ownership, and accepts an answering page', async () => {
    const host = await boot()
    const passiveCookie = await exchange(host, 'none')
    const passive = await mux(host, passiveCookie)
    const pageCookie = await exchange(host, 'html')
    const page = await mux(host, pageCookie)
    await success(host, pageCookie, 'fixture/identity', { label: 'browser' })
    expect(call(host, 'browser').peer === host.ctx.connection.browserPeer).toBe(false)
    expect(host.ctx.connection.authentication.authenticate({ headers: { host: new URL(host.origin).host, cookie: pageCookie } }))
      .toEqual({ peer: call(host, 'browser').peer })
    const ready = await events(page)
    const otherCookie = await exchange(host, 'answer')
    const other = await mux(host, otherCookie)
    await events(other)
    open(passive, 'forbidden', '$events', {})
    expect(await frame(passive, 'forbidden', 'error')).toMatchObject({ error: { code: 'gateway/interactions-forbidden' } })
    const pending = host.fixture.pending('approval')
    const delivery = await invocation(page, 'approval')
    const args = { clientId: ready.clientId, eventId: delivery.eventId, outcome: { kind: 'result', value: 'allowed' } }
    for (const cookie of [passiveCookie, otherCookie]) {
      expect(await rpc(host, cookie, '$events/result', args)).toMatchObject({
        body: { result: { ok: false, error: { code: 'gateway/interactions-forbidden' } } },
      })
    }
    await success(host, pageCookie, '$events/result', args)
    await expect(pending).resolves.toEqual({ kind: 'result', value: 'allowed' })
    expect(passive.frames.filter(item => item.type === 'item')).toEqual([])
  })

})

async function boot(): Promise<Host> {
  const root = await mkdtemp(join(tmpdir(), 'dsh-operator-connect-'))
  roots.push(root)
  const distIndex = join(root, 'index.html')
  await writeFile(distIndex, '<head></head><body>operator fixture</body>')
  const configPath = join(root, 'cordis.yml')
  const rows = [
    { name: '@deepseek-ai/dsh-credentials-local', config: { path: join(root, '.credentials.yaml'), watch: false } },
    { name: '@deepseek-ai/dsh-host-webserver', config: { host: '127.0.0.1', port: 0 } },
    { name: '@deepseek-ai/dsh-client-connection' },
    { name: '@deepseek-ai/dsh-host-authentication-token' },
    { name: '@deepseek-ai/dsh-host-frontend-static', config: { distIndex } },
    { name: '@deepseek-ai/dsh-typert-registry' },
    { name: '@deepseek-ai/dsh-api-remote-access' },
    { name: '@deepseek-ai/dsh-api-gateway' },
    { name: '@fixture/operator-connect' },
  ]
  await writeFile(configPath, rows.map(row => `- ${JSON.stringify({ ...row, name: `cordis:${row.name}` })}`).join('\n') + '\n')
  const ctx = new Context()
  ctx.baseUrl = pathToFileURL(root).href + '/'
  try {
    await ctx.plugin(Loader)
    Object.assign(ctx.loader.builtins, {
      include: Include,
      '@deepseek-ai/dsh-credentials-local': LocalCredentials,
      '@deepseek-ai/dsh-host-webserver': WebServer,
      '@deepseek-ai/dsh-client-connection': Connection,
      '@deepseek-ai/dsh-host-authentication-token': Token,
      '@deepseek-ai/dsh-host-frontend-static': FrontendStatic,
      '@deepseek-ai/dsh-typert-registry': TypertRegistry,
      '@deepseek-ai/dsh-api-remote-access': RemoteAccess,
      '@deepseek-ai/dsh-api-gateway': TypertGateway,
      '@fixture/operator-connect': OperatorConnectFixture,
    })
    await ctx.loader.create({ name: 'cordis:include', config: { path: pathToFileURL(configPath).href } })
    await ctx.loader.await()
    expect(ctx.webServer.port).toBeGreaterThan(0)
    const host = { ctx, origin: `http://127.0.0.1:${ctx.webServer.port}`, fixture: ctx.operatorConnectFixture }
    hosts.push(host)
    return host
  } catch (error) {
    await ctx.fiber.dispose()
    throw error
  }
}

async function exchange(host: Host, mode: 'html' | 'none' | 'answer'): Promise<string> {
  const auth = await BrowserAuth.create(host.ctx[Context.root], host.ctx.credentials, 30)
  const target = new URL(auth.authenticatedUrl(`${host.origin}${mode === 'html' ? '/' : '/connect'}`))
  if (mode !== 'html') target.searchParams.set('interactions', mode)
  const response = await fetch(target, { redirect: 'manual', headers: { accept: mode === 'html' ? 'text/html' : 'application/json' } })
  expect(response.status).toBe(mode === 'html' ? 303 : 302)
  const cookie = response.headers.get('set-cookie')
  await response.text()
  if (cookie === null) throw new Error('connect omitted its signed cookie')
  return cookie.split(';', 1)[0]!
}

function socketClient(host: Host, cookie: string, path = '/api/remote.mux'): SocketClient {
  const socket = new MuxSocket(`${host.origin}${path}`, { headers: { cookie } })
  const closed = new Promise<void>((resolve) => { socket.once('close', () => { resolve() }) })
  const frames: RemoteStreamServerMessage[] = []
  const errors: Error[] = []
  socket.on('error', (error: Error) => errors.push(error))
  socket.on('message', (text: string) => { frames.push(parseRemoteStreamServerMessage(text)) })
  const client = { socket, closed, frames, errors }
  clients.push(client)
  return client
}

function upgradeClient(host: Host, cookie: string, path = '/api/remote.mux'): UpgradeClient {
  const socket = new WebSocket(`${host.origin.replace('http:', 'ws:')}${path}`, { headers: { cookie } })
  const closed = new Promise<void>((resolve) => { socket.once('close', () => { resolve() }) })
  const errors: Error[] = []
  socket.on('error', (error: Error) => errors.push(error))
  const client = { socket, closed, errors }
  upgrades.push(client)
  return client
}

async function mux(host: Host, cookie: string, path = '/api/remote.mux'): Promise<SocketClient> {
  const client = socketClient(host, cookie, path)
  await once(client.socket, 'open')
  return client
}

async function refusedMux(host: Host, cookie: string): Promise<number> {
  const client = socketClient(host, cookie)
  const status = await new Promise<number>((resolve, reject) => {
    client.socket.once('open', () => { reject(new Error('forbidden Mux was admitted')) })
    client.socket.once('unexpected-response', (_request: unknown, response: { resume(): void; statusCode?: number }) => {
      response.resume()
      resolve(response.statusCode ?? 0)
      client.socket.terminate()
    })
    client.socket.once('error', reject)
  })
  await client.closed
  return status
}

async function close(client: SocketClient | UpgradeClient): Promise<void> {
  client.socket.close()
  await client.closed
  expect(client.errors).toEqual([])
}

function open(client: SocketClient, streamId: string, endpoint: string, args: object): void {
  client.socket.send(JSON.stringify({ type: 'open', streamId, endpoint, payload: { args } }))
}

async function frame(client: SocketClient, streamId: string, type: RemoteStreamServerMessage['type']): Promise<RemoteStreamServerMessage> {
  await vi.waitFor(() => { expect(client.frames.some(frame => frame.streamId === streamId && frame.type === type)).toBe(true) })
  return client.frames.find(frame => frame.streamId === streamId && frame.type === type)!
}

async function events(client: SocketClient): Promise<z.infer<typeof readySchema>> {
  open(client, 'events', '$events', {})
  const ready = await frame(client, 'events', 'item')
  if (ready.type !== 'item') throw new Error('event stream failed to open')
  return readySchema.parse(ready.value)
}

async function invocation(client: SocketClient, label: string): Promise<z.infer<typeof deliverySchema>> {
  let delivery: z.infer<typeof deliverySchema> | undefined
  await vi.waitFor(() => {
    for (const frame of client.frames) {
      if (frame.type !== 'item') continue
      const parsed = deliverySchema.safeParse(frame.value)
      if (parsed.success && parsed.data.request.prompt === label) delivery = parsed.data
    }
    expect(delivery).toBeDefined()
  })
  return delivery!
}

async function rpc(
  host: Host, cookie: string, endpoint: string, args: object,
): Promise<{ status: number; body: unknown }> {
  const response = await fetch(`${host.origin}/api/${endpoint}`, {
    method: 'POST', headers: { cookie, 'content-type': 'application/json' },
    body: JSON.stringify({ type: 'client-request', rpcId: randomUUID(), method: endpoint, payload: { args } }),
  })
  return { status: response.status, body: response.status === 200 ? await response.json() : await response.text() }
}

async function success(host: Host, cookie: string, endpoint: string, args: object): Promise<void> {
  expect(await rpc(host, cookie, endpoint, args)).toMatchObject({ status: 200, body: { result: { ok: true } } })
}

function call(host: Host, label: string) {
  const observation = host.fixture.calls.get(label)
  if (observation === undefined) throw new Error(`fixture call was not admitted: ${label}`)
  return observation
}
