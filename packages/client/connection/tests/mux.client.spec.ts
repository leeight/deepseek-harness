/** HTTP mux carrier: Host mount resolution, SSE decoding, and uplink framing. */

import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  MUX_CLOSED,
  MUX_CONNECTING,
  MUX_OPEN,
  openHttpStreamMux,
  type ConnectionMuxCarrier,
  type ConnectionMuxEventMap,
} from '../src/client/mux.ts'
import type { RpcFetch } from '../src/client/rpc.ts'

const SESSION = { header: 'x-dsh-mux-session', query: 'session' }
const PATH = '/api/remote.mux'
const BASE = 'https://host.example/tools/dsh/'

interface RecordedRequest {
  readonly url: string
  readonly init: RequestInit
}

const carriers = new Set<ConnectionMuxCarrier>()

afterEach(() => {
  for (const carrier of carriers) carrier.close()
  carriers.clear()
  vi.unstubAllGlobals()
})

function open(fetch: RpcFetch, baseUrl: string | null = BASE): ConnectionMuxCarrier {
  const carrier = openHttpStreamMux({
    path: PATH,
    session: SESSION,
    fetch,
    ...baseUrl === null ? {} : { baseUrl },
  })
  carriers.add(carrier)
  return carrier
}

function once<Event extends keyof ConnectionMuxEventMap>(
  carrier: ConnectionMuxCarrier,
  event: Event,
): Promise<ConnectionMuxEventMap[Event]> {
  return new Promise((resolve) => {
    carrier.addEventListener(event, (payload) => { resolve(payload) }, { once: true })
  })
}

function streamOf(chunks: readonly string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder()
  let index = 0
  return new ReadableStream<Uint8Array>({
    pull(controller) {
      const chunk = chunks[index++]
      if (chunk === undefined) {
        controller.close()
        return
      }
      controller.enqueue(encoder.encode(chunk))
    },
  })
}

function downlink(chunks: readonly string[] = [], session = 'session-1'): Response {
  return new Response(streamOf(chunks), {
    status: 200,
    headers: { 'x-dsh-mux-session': session },
  })
}

/** A downlink that stays open so the carrier keeps accepting uplink frames. */
function liveDownlink(session = 'session-1'): Response {
  return new Response(new ReadableStream<Uint8Array>({ start() {} }), {
    status: 200,
    headers: { 'x-dsh-mux-session': session },
  })
}

describe('HTTP mux carrier', () => {
  it('requires an explicit Host base outside a document before starting the downlink', () => {
    const fetch = vi.fn<RpcFetch>()

    expect(typeof document).toBe('undefined')
    expect(() => openHttpStreamMux({ path: PATH, session: SESSION, fetch }))
      .toThrow('connection: HTTP mux carrier requires a Host base URL outside a document')
    expect(fetch).not.toHaveBeenCalled()
  })

  it('resolves the channel beneath the explicit Host base and subscribes to the event stream', async () => {
    const calls: RecordedRequest[] = []
    const fetch: RpcFetch = async (input, init) => {
      calls.push({ url: String(input), init })
      return downlink()
    }
    const carrier = open(fetch)

    expect(carrier.readyState).toBe(MUX_CONNECTING)
    await once(carrier, 'open')
    expect(calls).toHaveLength(1)
    expect(calls[0]?.url).toBe('https://host.example/tools/dsh/api/remote.mux')
    expect(calls[0]?.init.method).toBe('GET')
    expect(calls[0]?.init.headers).toEqual({ accept: 'text/event-stream' })
    expect(calls[0]?.init.cache).toBe('no-store')
    expect(carrier.readyState).toBe(MUX_OPEN)
  })

  it('uses the document base when no Host base is supplied', async () => {
    const calls: RecordedRequest[] = []
    const fetch: RpcFetch = async (input, init) => {
      calls.push({ url: String(input), init })
      return downlink()
    }
    vi.stubGlobal('document', { baseURI: 'https://browser.example/mounted/' } satisfies Pick<Document, 'baseURI'>)
    const carrier = open(fetch, null)
    await once(carrier, 'open')

    expect(calls.map(call => call.url)).toEqual(['https://browser.example/mounted/api/remote.mux'])
  })

  it('decodes frames split across chunks and ignores heartbeat comments', async () => {
    const fetch: RpcFetch = async () => downlink([
      'data: {"a":1}\n\ndata: {"b',
      '":2}\n\n: heartbeat\n\ndata: {"c":3}\r\n\r\n',
    ])
    const carrier = open(fetch)
    const messages: string[] = []
    carrier.addEventListener('message', (event) => { messages.push(event.data) })
    const closed = once(carrier, 'close')

    await closed
    expect(messages).toEqual(['{"a":1}', '{"b":2}', '{"c":3}'])
    expect(carrier.readyState).toBe(MUX_CLOSED)
  })

  it('posts queued frames to the session URL in arrival order', async () => {
    const uplinks: RecordedRequest[] = []
    const fetch: RpcFetch = async (input, init) => {
      if (init.method === 'POST') {
        uplinks.push({ url: String(input), init })
        return new Response(null, { status: 204 })
      }
      return liveDownlink()
    }
    const carrier = open(fetch)
    await once(carrier, 'open')

    expect(carrier.readyState).toBe(MUX_OPEN)
    carrier.send('first')
    carrier.send('second')
    carrier.send('third')
    await vi.waitFor(() => { expect(uplinks.flatMap(decodedFrames)).toEqual(['first', 'second', 'third']) })

    expect(new Set(uplinks.map(call => call.url)))
      .toEqual(new Set(['https://host.example/tools/dsh/api/remote.mux?session=session-1']))
    for (const uplink of uplinks) {
      expect(uplink.init.method).toBe('POST')
      expect(uplink.init.headers).toEqual({ 'content-type': 'application/x-ndjson' })
    }
  })

  it('throws when sending before the downlink opens', async () => {
    let release: (response: Response) => void = () => undefined
    const pending = new Promise<Response>((resolve) => { release = resolve })
    const fetch: RpcFetch = () => pending
    const carrier = open(fetch)

    expect(carrier.readyState).toBe(MUX_CONNECTING)
    expect(() => { carrier.send('early') }).toThrow('connection: HTTP mux carrier is not open')
    release(liveDownlink())
    await once(carrier, 'open')
    expect(carrier.readyState).toBe(MUX_OPEN)
  })

  it('refuses a downlink without a session header and reports the status', async () => {
    const fetch: RpcFetch = async () => new Response(streamOf([]), { status: 200 })
    const carrier = open(fetch)
    const error = await once(carrier, 'error')

    expect(error.error).toEqual(new Error('connection: HTTP mux downlink was refused with status 200'))
    expect(carrier.readyState).toBe(MUX_CLOSED)
  })

  it('reports a refused downlink status', async () => {
    const fetch: RpcFetch = async () => new Response('unavailable', { status: 503 })
    const carrier = open(fetch)
    const error = await once(carrier, 'error')

    expect(error.error).toEqual(new Error('connection: HTTP mux downlink was refused with status 503'))
    expect(carrier.readyState).toBe(MUX_CLOSED)
  })

  it('reports a refused uplink and closes the carrier', async () => {
    const fetch: RpcFetch = async (_input, init) => init.method === 'POST'
      ? new Response('unavailable', { status: 500 })
      : liveDownlink()
    const carrier = open(fetch)
    await once(carrier, 'open')
    const closed = once(carrier, 'close')
    const errors: unknown[] = []
    carrier.addEventListener('error', (event) => { errors.push(event.error) })

    carrier.send('first')
    await closed
    expect(errors).toEqual([new Error('connection: HTTP mux uplink was refused with status 500')])
    expect(carrier.readyState).toBe(MUX_CLOSED)
    expect(() => { carrier.send('late') }).toThrow('connection: HTTP mux carrier is not open')
  })

  it('reports a transport that throws before returning a response', async () => {
    const fetch: RpcFetch = () => { throw new Error('connection refused') }
    const carrier = open(fetch)
    const errors: unknown[] = []
    carrier.addEventListener('error', (event) => { errors.push(event.error) })
    const closed = once(carrier, 'close')

    await closed
    expect(errors).toEqual([new Error('connection refused')])
    expect(carrier.readyState).toBe(MUX_CLOSED)
  })

  it('closes both directions once on close()', async () => {
    const fetch: RpcFetch = async () => liveDownlink()
    const carrier = open(fetch)
    await once(carrier, 'open')
    const closes: object[] = []
    carrier.addEventListener('close', (event) => { closes.push(event) })

    carrier.close()
    carrier.close()
    expect(carrier.readyState).toBe(MUX_CLOSED)
    expect(closes).toHaveLength(1)
    expect(() => { carrier.send('late') }).toThrow('connection: HTTP mux carrier is not open')
  })
})

/** @param uplink - recorded uplink request whose newline-joined body holds its frames. */
function decodedFrames(uplink: RecordedRequest): string[] {
  const body = uplink.init.body
  return typeof body === 'string' ? body.split('\n') : []
}
