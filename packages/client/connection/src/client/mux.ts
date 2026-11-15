/**
 * HTTP duplex carrier for one multiplexed logical channel: a `text/event-stream`
 * downlink plus session-addressed `POST` uplink frames. The carrier moves opaque
 * text frames; it knows nothing about the frames' meaning.
 */
import type { RpcFetch } from './rpc.ts'

/** Carrier ready states, numbered like the WebSocket API they replace. */
export const MUX_CONNECTING = 0
/** Downlink open and uplink frames accepted. */
export const MUX_OPEN = 1
/** Terminal: no downlink, no uplink, and every waiter failed. */
export const MUX_CLOSED = 3

/** Carrier events consumed by the channel owner on browser and Node carriers. */
export interface ConnectionMuxEventMap {
  readonly open: object
  readonly error: { readonly error: unknown }
  readonly close: object
  readonly message: { readonly data: string }
}

/**
 * Text duplex over HTTP. A consumer opens one carrier per physical attempt,
 * sends whole frames, and owns its own framing and closing.
 */
export interface ConnectionMuxCarrier {
  /** One of {@link MUX_CONNECTING}, {@link MUX_OPEN}, or {@link MUX_CLOSED}. */
  readonly readyState: number
  /**
   * Queue one complete text frame for the uplink.
   * @param data - frame text, delivered in call order.
   * @throws {Error} before the downlink opens or after the carrier closes.
   */
  send(data: string): void
  /**
   * Stop both directions.
   * @param code - accepted for parity with socket carriers; HTTP transmits no code.
   * @param reason - accepted for parity with socket carriers; HTTP transmits no reason.
   */
  close(code?: number, reason?: string): void
  /**
   * @param event - carrier event name.
   * @param listener - callback for that event.
   * @param options - whether the callback is removed after its first delivery.
   */
  addEventListener<Event extends keyof ConnectionMuxEventMap>(
    event: Event,
    listener: (event: ConnectionMuxEventMap[Event]) => void,
    options?: { readonly once?: boolean },
  ): void
  /**
   * @param event - carrier event name.
   * @param listener - previously registered callback.
   */
  removeEventListener<Event extends keyof ConnectionMuxEventMap>(
    event: Event,
    listener: (event: ConnectionMuxEventMap[Event]) => void,
  ): void
}

/**
 * Session addressing the Host publishes on its downlink and requires on every
 * uplink frame; the channel owner supplies its protocol's field names.
 */
export interface ConnectionMuxSession {
  /** Response header carrying the id of the session the downlink opened. */
  readonly header: string
  /** Query parameter naming that session on every uplink request. */
  readonly query: string
}

/** Inputs for one physical carrier attempt. */
export interface ConnectionMuxRequest {
  /** Absolute logical channel path, resolved beneath the Host mount. */
  readonly path: string
  /** Session field names the Host publishes and requires. */
  readonly session: ConnectionMuxSession
  /** HTTP transport; omission uses the page's `fetch` and its cookie jar. */
  readonly fetch?: RpcFetch | undefined
  /** Host base URL; omission resolves against the page's document base. */
  readonly baseUrl?: string | undefined
}

type MuxListeners = { [Event in keyof ConnectionMuxEventMap]: (event: ConnectionMuxEventMap[Event]) => void }

/**
 * Start one physical attempt: subscribe first, then read the downlink.
 * @param request - logical path, session field names, and transport.
 * @returns a carrier that is connecting when returned and emits `open` or `close`.
 */
export function openHttpStreamMux(request: ConnectionMuxRequest): ConnectionMuxCarrier {
  const base = request.baseUrl ?? (typeof document === 'undefined' ? undefined : document.baseURI)
  if (base === undefined) throw new Error('connection: HTTP mux carrier requires a Host base URL outside a document')
  const fetch = request.fetch ?? ((input, init) => globalThis.fetch(input, init))
  return new HttpStreamMuxCarrier(new URL(request.path.slice(1), base), request.session, fetch)
}

/** One downlink's incremental `text/event-stream` decoder. */
class EventStreamDecoder {
  private buffer = ''
  private data: string[] = []

  /**
   * Decode one body chunk into the frame bodies it completed.
   * @param chunk - next decoded text of the response body.
   * @returns frame bodies completed by this chunk, in arrival order.
   */
  push(chunk: string): string[] {
    this.buffer += chunk
    const frames: string[] = []
    let lineEnd = this.buffer.indexOf('\n')
    while (lineEnd !== -1) {
      const line = this.buffer.slice(0, lineEnd).replace(/\r$/, '')
      this.buffer = this.buffer.slice(lineEnd + 1)
      lineEnd = this.buffer.indexOf('\n')
      if (line === '') {
        if (this.data.length > 0) frames.push(this.data.join('\n'))
        this.data = []
        continue
      }
      // A leading colon is a comment; the Host sends heartbeats this way.
      if (line.startsWith(':')) continue
      const colon = line.indexOf(':')
      const field = colon === -1 ? line : line.slice(0, colon)
      const value = colon === -1 ? '' : line.slice(colon + 1).replace(/^ /, '')
      if (field === 'data') this.data.push(value)
    }
    return frames
  }
}

class HttpStreamMuxCarrier implements ConnectionMuxCarrier {
  private readonly listeners: { [Event in keyof ConnectionMuxEventMap]: Set<MuxListeners[Event]> } = {
    open: new Set(),
    error: new Set(),
    close: new Set(),
    message: new Set(),
  }
  private readonly abort = new AbortController()
  private readonly uplinks: string[] = []
  private state: number = MUX_CONNECTING
  private sessionId: string | undefined
  private pumping = false
  private reader: ReadableStreamDefaultReader<Uint8Array> | undefined

  /**
   * @param downlink - resolved Host URL of the logical channel.
   * @param session - session field names the Host publishes and requires.
   * @param fetch - HTTP transport for the downlink and every uplink request.
   */
  constructor(
    private readonly downlink: URL,
    private readonly session: ConnectionMuxSession,
    private readonly fetch: RpcFetch,
  ) {
    // The first tick runs after the caller subscribes, so a transport that
    // throws synchronously still reports through `error` and `close`.
    queueMicrotask(() => { void this.connect() })
  }

  get readyState(): number {
    return this.state
  }

  send(data: string): void {
    if (this.state !== MUX_OPEN) throw new Error('connection: HTTP mux carrier is not open')
    this.uplinks.push(data)
    void this.pumpUplinks()
  }

  close(code?: number, reason?: string): void {
    void code
    void reason
    this.finish(undefined)
  }

  addEventListener<Event extends keyof ConnectionMuxEventMap>(
    event: Event,
    listener: (event: ConnectionMuxEventMap[Event]) => void,
    options?: { readonly once?: boolean },
  ): void {
    const registered = options?.once === true
      ? (payload: ConnectionMuxEventMap[Event]): void => {
        this.removeEventListener(event, registered)
        listener(payload)
      }
      : listener
    this.listeners[event].add(registered as MuxListeners[Event])
  }

  removeEventListener<Event extends keyof ConnectionMuxEventMap>(
    event: Event,
    listener: (event: ConnectionMuxEventMap[Event]) => void,
  ): void {
    this.listeners[event].delete(listener as MuxListeners[Event])
  }

  /** Read the downlink: the session header admits the session, then frames stream in. */
  private async connect(): Promise<void> {
    if (this.state === MUX_CLOSED) return
    try {
      const response = await this.fetch(this.downlink.href, {
        method: 'GET',
        headers: { accept: 'text/event-stream' },
        cache: 'no-store',
        signal: this.abort.signal,
      })
      const sessionId = response.headers.get(this.session.header)
      if (!response.ok || response.body === null || sessionId === null) {
        throw new Error(`connection: HTTP mux downlink was refused with status ${response.status}`)
      }
      this.sessionId = sessionId
      this.reader = response.body.getReader()
      this.state = MUX_OPEN
      this.emit('open', {})
      await this.read(this.reader)
    } catch (error) {
      this.finish(error)
    }
  }

  /** @param reader - downlink body reader owned by this attempt. */
  private async read(reader: ReadableStreamDefaultReader<Uint8Array>): Promise<void> {
    const decoder = new TextDecoder()
    const frames = new EventStreamDecoder()
    while (true) {
      const { value, done } = await reader.read()
      if (done) break
      for (const frame of frames.push(decoder.decode(value, { stream: true }))) this.emit('message', { data: frame })
    }
    this.finish(undefined)
  }

  private isOpen(): boolean {
    return this.state === MUX_OPEN
  }

  /**
   * Send the queued frames in arrival order; one request carries every frame
   * queued while the previous request was in flight.
   */
  private async pumpUplinks(): Promise<void> {
    const sessionId = this.sessionId
    if (this.pumping || this.state !== MUX_OPEN || sessionId === undefined) return
    this.pumping = true
    try {
      while (this.uplinks.length > 0 && this.isOpen()) {
        const frames = this.uplinks.splice(0, this.uplinks.length)
        const response = await this.fetch(this.uplinkUrl(sessionId), {
          method: 'POST',
          headers: { 'content-type': 'application/x-ndjson' },
          body: frames.join('\n'),
          signal: this.abort.signal,
        })
        if (!response.ok) {
          throw new Error(`connection: HTTP mux uplink was refused with status ${response.status}`)
        }
      }
    } catch (error) {
      this.finish(error)
    } finally {
      this.pumping = false
    }
  }

  /** @param sessionId - session the downlink opened, named on every uplink request. */
  private uplinkUrl(sessionId: string): string {
    const url = new URL(this.downlink)
    url.searchParams.set(this.session.query, sessionId)
    return url.href
  }

  /**
   * Close both directions once, reporting the failure that caused it.
   * @param error - failure that closed the carrier; omitted for a clean close.
   */
  private finish(error: unknown): void {
    if (this.state === MUX_CLOSED) return
    this.state = MUX_CLOSED
    this.uplinks.length = 0
    this.abort.abort(error)
    void this.reader?.cancel().catch(() => undefined)
    this.reader = undefined
    if (error !== undefined) this.emit('error', { error })
    this.emit('close', {})
  }

  private emit<Event extends keyof ConnectionMuxEventMap>(event: Event, payload: ConnectionMuxEventMap[Event]): void {
    for (const listener of [...this.listeners[event]]) listener(payload)
  }
}
