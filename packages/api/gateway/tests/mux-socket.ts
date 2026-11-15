/**
 * Test client for the HTTP Remote stream mux.
 *
 * Exposes the `ws` surface the Gateway specs were written against — `send`,
 * `message` events, `close`, `terminate`, `unexpected-response`, `readyState` —
 * over the SSE downlink and the NDJSON uplink POSTs, so a spec that exercises
 * transport behavior keeps its assertions and only swaps the constructor.
 */
import { EventEmitter } from 'node:events'

/** Options accepted by a test mux client. */
export interface MuxSocketOptions {
  /** Request headers, including the browser cookie a spec authenticates with. */
  readonly headers?: Record<string, string>
  /** Answer server liveness pings; `false` simulates a peer that stopped reading. */
  readonly answerPings?: boolean
}

/** Response facts a refused downlink reports to a waiting spec. */
export interface MuxRefusal {
  readonly statusCode: number
  readonly statusMessage: string
  readonly headers: Headers
  /** Matches the `ws` unexpected-response consumer, which drains the response. */
  resume(): void
}

/**
 * One mux session from the client side.
 *
 * A spec drives the downlink through `sent` and `message`, and reproduces a
 * transport fault with `close()` or `terminate()`.
 */
export class MuxSocket extends EventEmitter {
  /** Socket is still opening. */
  static readonly CONNECTING = 0
  /** Socket accepted the downlink. */
  static readonly OPEN = 1
  /** Socket is closing. */
  static readonly CLOSING = 2
  /** Socket is closed. */
  static readonly CLOSED = 3
  /** Every socket created since the process started, in connection order. */
  static readonly sockets: MuxSocket[] = []

  /** Uplink frames this client sent, in order. */
  readonly sent: string[] = []
  /** Close reasons this client was asked to report. */
  readonly closedWith: { readonly code?: number; readonly reason?: string }[] = []
  /** Negotiated extension; the HTTP mux never negotiates one. */
  readonly extensions = ''
  /** Current lifecycle state. */
  readyState: number = MuxSocket.CONNECTING
  /** Session id assigned by the downlink, once accepted. */
  sessionId: string | undefined

  private readonly abort = new AbortController()
  private writes: Promise<void> = Promise.resolve()
  private closed = false

  /**
   * @param url - mux URL; `ws:`/`wss:` are rewritten to `http:`/`https:`.
   * @param options - request headers and liveness behavior.
   */
  constructor(
    private readonly url: string,
    private readonly options: MuxSocketOptions = {},
  ) {
    super()
    MuxSocket.sockets.push(this)
    void this.connect().catch(() => {})
  }

  /**
   * Queue one JSON frame for the uplink.
   * @param data - frame text.
   */
  send(data: string): void {
    if (this.readyState !== MuxSocket.OPEN) throw new Error('fixture mux socket is not open')
    this.sent.push(data)
    this.writes = this.writes.then(async () => { await this.post([data]) })
  }

  /**
   * Close the downlink, as a Client that leaves does.
   * @param code - close code reported to `close` listeners.
   * @param reason - close reason reported to `close` listeners.
   */
  close(code?: number, reason?: string): void {
    this.closedWith.push({
      ...(code === undefined ? {} : { code }),
      ...(reason === undefined ? {} : { reason }),
    })
    this.finish(code, reason)
  }

  /**
   * Close the downlink without a graceful frame, as a transport failure does.
   */
  terminate(): void {
    this.finish()
  }

  private finish(code?: number, reason?: string): void {
    if (this.closed) return
    this.closed = true
    this.readyState = MuxSocket.CLOSED
    this.abort.abort()
    setImmediate(() => {
      this.emit('close', code ?? 1006, reason === undefined ? '' : reason)
    })
  }

  private async connect(): Promise<void> {
    const downlink = httpUrl(this.url)
    let response: Response
    try {
      response = await fetch(downlink, {
        headers: { ...this.options.headers, accept: 'text/event-stream' },
        signal: this.abort.signal,
      })
    } catch (error) {
      if (!this.closed) this.fail(error)
      return
    }
    if (!response.ok) {
      const refusal: MuxRefusal = {
        statusCode: response.status,
        statusMessage: response.statusText,
        headers: response.headers,
        resume: () => {},
      }
      if (this.listenerCount('unexpected-response') === 0) {
        this.fail(new Error(`fixture mux downlink refused with status ${String(response.status)}`))
        return
      }
      this.emit('unexpected-response', { abort: () => { this.finish() } }, refusal)
      this.finish()
      return
    }
    const session = response.headers.get('x-dsh-mux-session')
    if (session === null) {
      this.fail(new Error('fixture mux downlink is missing its session id'))
      return
    }
    this.sessionId = session
    this.readyState = MuxSocket.OPEN
    this.emit('open')
    if (response.body === null) return
    const reader = response.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    try {
      for (;;) {
        const chunk = await reader.read()
        if (chunk.done) break
        buffer += decoder.decode(chunk.value, { stream: true })
        const frames = drainFrames(buffer)
        buffer = frames.rest
        for (const frame of frames.frames) this.receive(frame)
      }
    } catch (error) {
      if (!this.closed) this.fail(error)
      return
    }
    this.finish()
  }

  private receive(text: string): void {
    let transport: unknown
    try { transport = JSON.parse(text) } catch { transport = undefined }
    if (isPing(transport)) {
      this.emit('ping', Buffer.alloc(0))
      if (this.options.answerPings !== false) this.writes = this.writes.then(async () => { await this.post(['{"type":"pong"}']) })
      return
    }
    this.emit('message', text)
  }

  private fail(error: unknown): void {
    this.readyState = MuxSocket.CLOSED
    if (this.listenerCount('error') > 0) this.emit('error', error)
    this.finish()
  }

  private async post(frames: readonly string[]): Promise<void> {
    if (this.sessionId === undefined) return
    const url = httpUrl(this.url)
    url.searchParams.set('session', this.sessionId)
    try {
      await fetch(url, {
        method: 'POST',
        headers: { 'content-type': 'application/x-ndjson', ...this.options.headers },
        body: frames.join('\n'),
        signal: this.abort.signal,
      })
    } catch (error) {
      // A closed fixture socket fails its remaining writes, exactly as a real
      // Client does; the downlink close already reports the fault.
      if (this.closed) return
      this.fail(error)
    }
  }
}

function httpUrl(url: string): URL {
  const parsed = new URL(url)
  parsed.protocol = parsed.protocol === 'wss:' ? 'https:' : 'http:'
  return parsed
}

function isPing(value: unknown): boolean {
  return typeof value === 'object' && value !== null && Reflect.get(value, 'type') === 'ping'
}

/** SSE frames decoded from one chunk plus the unterminated remainder. */
function drainFrames(buffer: string): { readonly frames: string[]; readonly rest: string } {
  const frames: string[] = []
  for (;;) {
    const end = buffer.indexOf('\n\n')
    if (end < 0) return { frames, rest: buffer }
    const block = buffer.slice(0, end)
    buffer = buffer.slice(end + 2)
    const data = block.split('\n')
      .filter(line => line.startsWith('data:'))
      .map(line => line.slice('data:'.length).replace(/^ /u, ''))
    if (data.length > 0) frames.push(data.join('\n'))
  }
}
