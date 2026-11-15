import { once } from 'node:events'
import { createServer, type ServerResponse } from 'node:http'
import { expect, it, onTestFinished, vi } from 'vitest'
import { accountView, desktopAccountBackend } from '../src/account-backend.ts'

it('projects only safe account fields and refuses non-browser authorization schemes', () => {
  const state = { links: { usageUrl: 'http://localhost/usage', topUpUrl: 'http://localhost/top_up' }, status: 'signed-out', token: 'not-for-the-renderer',
    attempt: { id: 'test', phase: 'waiting-browser', authorizeUrl: 'https://platform.deepseek.com/dsh/authorize', codeVerifier: 'private' } }
  expect(accountView(state)).toEqual({ links: { usageUrl: 'http://localhost/usage', topUpUrl: 'http://localhost/top_up' }, status: 'signed-out', attempt: {
    id: 'test', phase: 'waiting-browser', authorizeUrl: 'https://platform.deepseek.com/dsh/authorize',
  } })
  for (const authorizeUrl of ['file:///tmp/example', 'javascript:alert(1)', 'http://example.com/login']) {
    expect(() => accountView({ ...state, attempt: { ...state.attempt, authorizeUrl } })).toThrow()
  }
  expect(() => accountView({ ...state, links: { usageUrl: 'file:///tmp/example', topUpUrl: 'https://example.com/top_up' } })).toThrow()
  expect(() => accountView({ ...state, attempt: { ...state.attempt, expiresAt: 'tomorrow' } })).toThrow()
  expect(() => accountView({ ...state, attempt: { ...state.attempt, errorCode: 'raw-server-message' } })).toThrow()
})

it('accepts a no-response login failure without discarding the account projection', () => {
  const state = { status: 'signed-out', attempt: { id: 'test', phase: 'failed', errorCode: 'no-response' },
    links: { usageUrl: 'https://platform.deepseek.com/usage', topUpUrl: 'https://platform.deepseek.com/top_up' } }
  expect(accountView(state)).toEqual(state)
})

it('uses account Remote commands without returning additional wire fields', async () => {
  const requests: unknown[] = []
  const backend = desktopAccountBackend('http://127.0.0.1:1234', (request) => {
    requests.push(request)
    return Promise.resolve({ links: { usageUrl: 'http://localhost/usage', topUpUrl: 'http://localhost/top_up' }, status: 'signed-out', attempt: null, token: 'private' })
  }, '')
  const client = { version: '1.2.3', locale: 'en', timezoneOffsetSeconds: 28_800 }
  expect(await backend.start(client)).toEqual({ links: { usageUrl: 'http://localhost/usage', topUpUrl: 'http://localhost/top_up' }, status: 'signed-out', attempt: null })
  expect(await backend.signOut(client)).toMatchObject({ status: 'signed-out' })
  expect(requests).toEqual([
    { namespace: 'account', method: 'startSignIn', args: { client, callbackOrigin: 'http://127.0.0.1:1234', loginSource: 'desktop' } },
    { namespace: 'account', method: 'signOut', args: { client } },
  ])
})

it('does not replay an expiry reason from an account snapshot', () => {
  const state = { status: 'signed-out', attempt: null,
    links: { usageUrl: 'https://platform.deepseek.com/usage', topUpUrl: 'https://platform.deepseek.com/top_up' } }
  expect(accountView({ ...state, signOutReason: 'expired' })).toEqual(state)
})

it.each([
  ['snapshot', 'invalid'],
  ['credentials-changed', 'invalid'],
  ['snapshot', ['snapshot']],
  ['credentials-changed', ['credentials-changed']],
] as const)('receives %s state updates separately from live expiry and rejects type %j', async (type, invalidType) => {
  interface UplinkFrame { readonly type: string; readonly endpoint?: string; readonly streamId: string }
  const cookies: Array<string | undefined> = []
  const streams = new Map<string, string>()
  const connected = Promise.withResolvers<undefined>()
  const opened = Promise.withResolvers<undefined>()
  let downlink: ServerResponse | undefined
  const server = createServer((request, response) => {
    const url = new URL(request.url ?? '/', 'http://127.0.0.1')
    cookies.push(request.headers.cookie)
    if (request.method === 'GET' && url.pathname === '/api/remote.mux') {
      if (!String(request.headers.accept).split(',').includes('text/event-stream')) {
        response.writeHead(406).end('events required')
        return
      }
      response.writeHead(200, {
        'cache-control': 'no-store',
        'content-type': 'text/event-stream',
        'x-dsh-mux-session': 'desktop-session',
      })
      response.flushHeaders()
      downlink = response
      connected.resolve(undefined)
      return
    }
    if (request.method === 'POST' && url.pathname === '/api/remote.mux') {
      if (url.searchParams.get('session') !== 'desktop-session') {
        response.writeHead(404).end('session not found')
        return
      }
      const chunks: Buffer[] = []
      request.on('data', (chunk: Buffer) => { chunks.push(chunk) })
      request.on('end', () => {
        const text = Buffer.concat(chunks).toString('utf8')
        for (const line of text.split('\n').filter(frame => frame !== '')) {
          const frame = JSON.parse(line) as UplinkFrame
          if (frame.type === 'open' && frame.endpoint !== undefined) streams.set(frame.endpoint, frame.streamId)
        }
        response.writeHead(204).end()
        if (streams.size === 3) opened.resolve(undefined)
      })
      return
    }
    response.writeHead(404).end()
  })
  server.listen(0, '127.0.0.1')
  await once(server, 'listening')
  onTestFinished(async () => {
    downlink?.destroy()
    server.closeAllConnections()
    await new Promise<void>((resolve, reject) => { server.close((error) => { if (error) reject(error); else resolve() }) })
  })
  const address = server.address()
  if (typeof address === 'string' || address === null) throw new Error('missing server address')
  const expired = vi.fn()
  const onAnalyticsEnabledChanged = vi.fn()
  const snapshot = { status: 'signed-out', attempt: null,
    links: { usageUrl: 'https://platform.deepseek.com/usage', topUpUrl: 'https://platform.deepseek.com/top_up' } }
  const received = Promise.withResolvers<undefined>()
  const backend = desktopAccountBackend(`http://127.0.0.1:${address.port}`, () => Promise.resolve(snapshot), 'dsh-auth-fixture=signed')
  const failed = vi.fn()
  const stop = backend.watch(
    (value) => { expect(value).toEqual(snapshot); received.resolve(undefined) }, failed, expired, onAnalyticsEnabledChanged,
  )
  onTestFinished(stop)
  await connected.promise
  await opened.promise
  expect(cookies[0]).toBe('dsh-auth-fixture=signed')
  const send = (frame: object): void => { downlink?.write(`data: ${JSON.stringify(frame)}\n\n`) }
  // Logical streams share the downlink; their opening frames named them above.
  send({ type: 'item', streamId: streams.get('account/watch'), value: { type, state: snapshot } })
  await received.promise
  send({ type: 'item', streamId: streams.get('productAnalytics/watchPolicy'), value: true })
  await vi.waitFor(() => { expect(onAnalyticsEnabledChanged).toHaveBeenLastCalledWith(true) })
  send({ type: 'item', streamId: streams.get('productAnalytics/watchPolicy'), value: false })
  await vi.waitFor(() => { expect(onAnalyticsEnabledChanged).toHaveBeenLastCalledWith(false) })
  send({ type: 'item', streamId: streams.get('productAnalytics/watchPolicy'), value: true })
  await vi.waitFor(() => { expect(onAnalyticsEnabledChanged).toHaveBeenLastCalledWith(true) })
  send({ type: 'item', streamId: streams.get('productAnalytics/watchPolicy'), value: 'invalid policy' })
  await vi.waitFor(() => { expect(onAnalyticsEnabledChanged).toHaveBeenLastCalledWith(false) })
  expect(expired).not.toHaveBeenCalled()
  send({ type: 'item', streamId: streams.get('account/watchExpiry'), value: 'session-expired' })
  await vi.waitFor(() => { expect(expired).toHaveBeenCalledOnce() })
  send({ type: 'item', streamId: streams.get('account/watch'), value: { type: invalidType, state: snapshot } })
  await vi.waitFor(() => { expect(failed).toHaveBeenCalledOnce() })
  stop()
})
