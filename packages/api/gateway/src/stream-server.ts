/** Host HTTP owner for multiplexed Typert Remote streams. */

import { randomUUID } from 'node:crypto'
import type { IncomingMessage, ServerResponse } from 'node:http'
import { Deque } from '@deepseek-ai/dsh-deque'
import { RemoteError, remoteErrorOf, type PeerScope, type RemoteUplinkWindow } from '@deepseek-ai/dsh-typert-protocol'
import {
  encodeRemoteStreamFrame,
  parseRemoteStreamClientMessage,
  parseRemoteStreamTransportFrame,
  REMOTE_STREAM_MUX_MEDIA_TYPE,
  REMOTE_STREAM_SESSION_HEADER,
  REMOTE_STREAM_SESSION_PARAM,
  REMOTE_STREAM_UPLINK_MAX_BYTES,
  type RemoteStreamClientMessage,
  type RemoteStreamFailure,
  type RemoteStreamServerMessage,
} from './stream-protocol.ts'

/**
 * Open one validated Remote stream for a decoded wire request on behalf of one
 * Peer. `uplink` carries the Client's items; `control` cancels the logical
 * stream, and aborting it with a Remote failure as the reason delivers that
 * failure to the Client. `window` describes this stream's complete-frame inbox budget.
 */
export type RemoteStreamOpener = (
  endpoint: string,
  payload: unknown,
  uplink: AsyncIterable<unknown>,
  peer: PeerScope,
  control: AbortController,
  window: RemoteUplinkWindow,
) => Promise<AsyncIterable<unknown>>

/** The opener one session uses: its Peer is fixed when the downlink is admitted. */
type BoundStreamOpener = (
  endpoint: string,
  payload: unknown,
  uplink: AsyncIterable<unknown>,
  control: AbortController,
  window: RemoteUplinkWindow,
) => Promise<AsyncIterable<unknown>>

/** Convert an invocation or carrier failure to a stable wire value. */
export type RemoteStreamFailureMapper = (error: unknown) => RemoteStreamFailure

const MAX_MISSED_HEARTBEATS = 2

/** Carrier options for {@link RemoteStreamMuxServer}. */
export interface RemoteStreamMuxServerOptions {
  /** Interval between downlink liveness frames. */
  readonly heartbeatIntervalMs: number
  /** Buffered uplink frame bytes one logical stream may hold before it fails. */
  readonly streamInboxBytes: number
}

/** One open downlink and every logical stream it carries. */
interface MuxSession {
  readonly response: ServerResponse
  readonly peer: PeerScope
  readonly connection: RemoteStreamMuxConnection
  /** Withdraws the Peer binding that destroys {@link response}. */
  readonly release: () => Promise<void> | void
  missedHeartbeats: number
}

/** Own every open mux downlink and the uplink route that feeds them. */
export class RemoteStreamMuxServer {
  private readonly sessions = new Map<string, MuxSession>()
  private readonly running = new Set<Promise<void>>()
  private readonly heartbeatIntervalMs: number
  private readonly streamInboxBytes: number
  private heartbeatTimer: NodeJS.Timeout | undefined

  /**
   * @param open - Gateway stream dispatcher.
   * @param failure - Gateway error-to-wire mapper.
   * @param options - heartbeat interval and uplink inbox bound.
   */
  constructor(
    private readonly open: RemoteStreamOpener,
    private readonly failure: RemoteStreamFailureMapper,
    options: RemoteStreamMuxServerOptions,
  ) {
    this.heartbeatIntervalMs = options.heartbeatIntervalMs
    this.streamInboxBytes = options.streamInboxBytes
  }

  /**
   * Open one mux session on an admitted `GET`. Every stream the session's
   * uplinks open speaks for the Peer admitted with the request, and the
   * downlink ends when that Peer's scope is disposed.
   * @param req - authenticated downlink request.
   * @param res - response the session writes one event per frame to.
   * @param peer - Peer the downlink was admitted as.
   */
  handleDownlink(req: IncomingMessage, res: ServerResponse, peer: PeerScope): void {
    if (!acceptsEvents(req)) {
      writeText(res, 406, 'events required')
      return
    }
    let release: (() => Promise<void> | void) | undefined
    try {
      release = peer.ctx.effect(() => () => { res.destroy() }, 'api-gateway: Remote stream session bound to its Peer')
    } catch {
      // A disposed scope leaves no Peer for the session to speak for.
      res.destroy()
      return
    }
    const bound: BoundStreamOpener = (endpoint, payload, uplink, control, window) =>
      this.open(endpoint, payload, uplink, peer, control, window)
    const connection = new RemoteStreamMuxConnection(res, bound, this.failure, this.streamInboxBytes)
    const id = randomUUID()
    res.writeHead(200, {
      'cache-control': 'no-store',
      'content-type': REMOTE_STREAM_MUX_MEDIA_TYPE,
      // Reverse proxies must not buffer an event stream behind a full buffer.
      'x-accel-buffering': 'no',
      [REMOTE_STREAM_SESSION_HEADER]: id,
    })
    res.flushHeaders()
    const session: MuxSession = { response: res, peer, connection, release, missedHeartbeats: 0 }
    this.sessions.set(id, session)
    connection.bind(session)
    this.startHeartbeat()
    const done = connection.run().catch((error: unknown) => {
      console.error('[api-gateway] Remote stream downlink failed:', error)
    }).then(() => {
      // The response may already be gone; releasing the Peer binding must not
      // depend on the downlink still being writable.
      this.sessions.delete(id)
      connection.unbind()
      void release()
      if (!res.writableEnded) res.end()
    })
    this.running.add(done)
    void done.then(() => { this.running.delete(done) })
  }

  /**
   * Accept one uplink frame posted for an open session. The frame is written by
   * the Client, so a body above {@link REMOTE_STREAM_UPLINK_MAX_BYTES} or one
   * that fails the stream protocol ends that session's carrier.
   * @param req - authenticated uplink request.
   * @param res - response answering the single frame.
   * @param peer - Peer the request was admitted as.
   */
  async handleUplink(req: IncomingMessage, res: ServerResponse, peer: PeerScope): Promise<void> {
    const id = new URL(req.url ?? '/', 'http://dsh.invalid').searchParams.get(REMOTE_STREAM_SESSION_PARAM)
    const session = id === null ? undefined : this.sessions.get(id)
    // A session id is unguessable, and one Peer's frames never reach another's session.
    if (session === undefined || session.peer !== peer) {
      writeText(res, 404, 'session not found')
      return
    }
    let text: string
    try {
      text = await readUplink(req)
    } catch (error) {
      writeText(res, error instanceof UplinkTooLargeError ? 413 : 400, 'invalid uplink frame')
      return
    }
    try {
      for (const frame of text.split('\n')) {
        // One request may carry several frames; a pong answers a liveness ping
        // and never reaches a logical stream.
        if (frame === '') continue
        if (parseRemoteStreamTransportFrame(frame) !== undefined) {
          session.missedHeartbeats = 0
          continue
        }
        session.connection.receive(frame)
      }
    } catch {
      // A frame that fails the stream protocol leaves the session ambiguous, so
      // it ends the same way a closed socket did.
      writeText(res, 400, 'invalid uplink frame')
      session.connection.stop(new Error('Remote stream protocol violation'))
      return
    }
    writeText(res, 204, '')
  }

  /** End every session and wait until each one released its Peer. */
  async close(): Promise<void> {
    clearInterval(this.heartbeatTimer)
    this.heartbeatTimer = undefined
    for (const session of [...this.sessions.values()]) session.response.destroy()
    await Promise.all(this.running)
  }

  /** Start one `unref()` timer after the first session; it spans idle periods until close(). */
  private startHeartbeat(): void {
    if (this.heartbeatTimer !== undefined) return
    this.heartbeatTimer = setInterval(() => {
      for (const session of [...this.sessions.values()]) {
        const missed = session.missedHeartbeats
        if (missed >= MAX_MISSED_HEARTBEATS) {
          setImmediate(() => {
            if (session.missedHeartbeats >= MAX_MISSED_HEARTBEATS) session.response.destroy()
          })
          continue
        }
        session.missedHeartbeats = missed + 1
        void session.connection.ping().catch(() => { session.response.destroy() })
      }
    }, this.heartbeatIntervalMs)
    this.heartbeatTimer.unref()
  }
}
interface ActiveStream {
  readonly abort: AbortController
  readonly inbox: UplinkInbox
  /** Cancel the logical stream and end any Host read still waiting on its uplink. */
  readonly stop: (reason: Error) => void
  done: Promise<void>
}

class RemoteStreamMuxConnection {
  private readonly streams = new Map<string, ActiveStream>()
  private writes = Promise.resolve()
  private session: MuxSession | undefined

  constructor(
    private readonly response: ServerResponse,
    private readonly open: BoundStreamOpener,
    private readonly failure: RemoteStreamFailureMapper,
    private readonly streamInboxBytes: number,
  ) {}

  /** Attach the session this connection writes for, so uplink activity can mark it alive. */
  bind(session: MuxSession): void {
    this.session = session
  }

  /** Detach the session once its downlink is gone. */
  unbind(): void {
    this.session = undefined
  }

  /**
   * Serve frames until the response closes. Since the transport has no control
   * frames, every uplink post and every downlink write that had to wait for
   * the response to drain counts as peer activity.
   */
  async run(): Promise<void> {
    const closed = new Promise<void>((resolve) => {
      this.response.once('close', resolve)
      this.response.once('error', () => { this.response.destroy() })
    })
    await closed
    const active = [...this.streams.values()]
    for (const stream of active) stream.stop(new Error('Remote stream downlink closed'))
    await Promise.all(active.map(stream => stream.done))
  }

  /** Write one liveness frame; a downlink that cannot take it is gone. */
  async ping(): Promise<void> {
    await this.write('{"type":"ping"}')
  }

  /** End the carrier now; the Client sees its downlink close and reconnects. */
  stop(reason: Error): void {
    this.response.destroy(reason)
  }

  /**
   * Dispatch one frame. `item`, `end`, and `cancel` for a stream this connection
   * no longer owns are dropped: a finished stream leaves the table while the
   * Client's in-flight frames are still arriving. A duplicate `open` is the one
   * protocol violation that ends the session.
   */
  receive(text: string): void {
    this.sessionActivity()
    const message = parseRemoteStreamClientMessage(text)
    switch (message.type) {
      case 'open': {
        this.openStream(message)
        return
      }
      case 'item': {
        this.streams.get(message.streamId)?.inbox.push(message.value, Buffer.byteLength(text, 'utf8'))
        return
      }
      case 'end': {
        this.streams.get(message.streamId)?.inbox.end()
        return
      }
      case 'cancel': {
        this.streams.get(message.streamId)?.stop(new Error('Remote stream cancelled'))
        return
      }
      /* v8 ignore next 4 -- parseRemoteStreamClientMessage admits only the four frame types above. */
      default: {
        const unknown: never = message
        throw new Error(`api gateway: unknown Remote stream client message ${JSON.stringify(unknown)}`)
      }
    }
  }

  private sessionActivity(): void {
    if (this.session !== undefined) this.session.missedHeartbeats = 0
  }

  private openStream(message: Extract<RemoteStreamClientMessage, { readonly type: 'open' }>): void {
    if (this.streams.has(message.streamId)) {
      throw new Error(`api gateway: duplicate Remote stream id ${JSON.stringify(message.streamId)}`)
    }
    const abort = new AbortController()
    // Created before the opener resolves so items the Client sends right
    // after `open` wait in the inbox instead of being lost.
    const inbox = new UplinkInbox(this.streamInboxBytes, message.endpoint, (error) => { abort.abort(error) })
    const active: ActiveStream = {
      abort,
      inbox,
      stop: (reason) => {
        abort.abort(reason)
        inbox.fail(reason)
      },
      done: Promise.resolve(),
    }
    this.streams.set(message.streamId, active)
    const done = this.pump(message.streamId, message.endpoint, message.payload, active)
    active.done = done
    const remove = (): void => { this.streams.delete(message.streamId) }
    void done.then(remove, remove)
  }

  private async pump(
    streamId: string,
    endpoint: string,
    payload: unknown,
    active: ActiveStream,
  ): Promise<void> {
    let outcome: { readonly failed: false } | { readonly failed: true; readonly error: unknown }
    try {
      const source = await this.open(endpoint, payload, active.inbox, active.abort, {
        maxBytes: this.streamInboxBytes,
        frameOverheadBytes: Buffer.byteLength(JSON.stringify({ type: 'item', streamId, value: null }), 'utf8') - 'null'.length,
      })
      for await (const value of source) {
        await this.send({ type: 'item', streamId, value })
      }
      outcome = { failed: false }
    } catch (error) {
      outcome = { failed: true, error }
    }
    // The downlink has settled; later uplink frames cannot change the outcome.
    active.inbox.fail(new Error('Remote stream ended'))
    if (active.abort.signal.aborted) {
      // A Remote failure as the abort reason is the Gateway or this mux failing
      // the stream (a rejected or overflowing uplink item, or an item after
      // end); the Client is still waiting for that terminal frame. Any other
      // abort is a cancellation.
      const reason: unknown = active.abort.signal.reason
      if (remoteErrorOf(reason) !== undefined) await this.sendFailure(streamId, reason)
      return
    }
    if (outcome.failed) {
      await this.sendFailure(streamId, outcome.error)
      return
    }
    try {
      await this.send({ type: 'end', streamId })
    } catch (error) {
      await this.sendFailure(streamId, error)
    }
  }

  private async sendFailure(streamId: string, error: unknown): Promise<void> {
    if (this.response.destroyed || this.response.writableEnded) return
    try {
      await this.send({ type: 'error', streamId, error: this.failure(error) })
    } catch {
      // A terminal frame that cannot be encoded or written leaves the
      // logical stream ambiguous, so fail the physical generation.
      this.stop(new Error('Remote stream failure could not be delivered'))
    }
  }

  private send(message: RemoteStreamServerMessage): Promise<void> {
    let text: string
    try {
      text = JSON.stringify(message)
    } catch (cause) {
      return Promise.reject(new Error('api gateway: Remote stream item is not JSON serializable', { cause }))
    }
    return this.write(text)
  }

  private write(text: string): Promise<void> {
    const frame = encodeRemoteStreamFrame(text)
    const delivery = this.writes.then(() => this.writeFrame(frame))
    this.writes = delivery.catch(() => undefined)
    return delivery
  }

  /**
   * Write one complete event to the response. A write the response accepted
   * without waiting counts nothing, because it proves only that the local
   * buffer had room; a write that had to drain proves the Client was reading.
   */
  private writeFrame(frame: string): Promise<void> {
    const response = this.response
    if (response.destroyed || response.writableEnded) {
      return Promise.reject(new Error('api gateway: Remote stream downlink is closed'))
    }
    if (response.write(frame)) return Promise.resolve()
    return new Promise<void>((resolve, reject) => {
      const drain = (): void => {
        response.off('close', closed)
        response.off('error', closed)
        this.sessionActivity()
        resolve()
      }
      const closed = (): void => {
        response.off('drain', drain)
        response.off('error', closed)
        reject(new Error('api gateway: Remote stream downlink is closed'))
      }
      response.once('drain', drain)
      response.once('close', closed)
      response.once('error', closed)
    })
  }
}

interface UplinkEntry {
  readonly value: unknown
  readonly bytes: number
}

const UPLINK_DONE: IteratorReturnResult<undefined> = { value: undefined, done: true }

/**
 * Bounded single-consumer uplink queue of one logical stream, the source the
 * Host method reads through `invocation.uplink()`. Buffered frame bytes are
 * capped: overflow, and an item after the Client's `end`, fail the queue and
 * report a Remote failure that the connection uses to fail the logical stream.
 */
class UplinkInbox implements AsyncIterable<unknown>, AsyncIterator<unknown> {
  private readonly queue = new Deque<UplinkEntry>()
  private bytes = 0
  private ended = false
  private closed = false
  private taken = false
  private failure: Error | undefined
  private wake: (() => void) | undefined

  constructor(
    private readonly maxBytes: number,
    private readonly endpoint: string,
    private readonly onViolation: (error: RemoteError<'gateway/protocol' | 'gateway/uplink-overflow'>) => void,
  ) {}

  push(value: unknown, frameBytes: number): void {
    if (this.failure !== undefined || this.closed) return
    if (this.ended) {
      this.violate(new RemoteError(
        'gateway/protocol',
        'api gateway: Remote stream uplink item after end',
        { endpoint: this.endpoint },
      ))
      return
    }
    if (this.bytes + frameBytes > this.maxBytes) {
      this.violate(new RemoteError(
        'gateway/uplink-overflow',
        `api gateway: Remote stream uplink exceeded ${String(this.maxBytes)} buffered bytes`,
        { endpoint: this.endpoint },
      ))
      return
    }
    this.queue.pushBack({ value, bytes: frameBytes })
    this.bytes += frameBytes
    this.signal()
  }

  /** Client half-close; idempotent. */
  end(): void {
    if (this.ended) return
    this.ended = true
    this.signal()
  }

  /** End the consumer's next read with `error`; idempotent, drops buffered items. */
  fail(error: Error): void {
    if (this.failure !== undefined) return
    this.failure = error
    this.queue.clear()
    this.bytes = 0
    this.signal()
  }

  [Symbol.asyncIterator](): AsyncIterator<unknown> {
    if (this.taken) throw new Error('api gateway: Remote stream uplink inbox already has a consumer')
    this.taken = true
    return this
  }

  async next(): Promise<IteratorResult<unknown>> {
    while (true) {
      if (this.closed) return UPLINK_DONE
      const entry = this.queue.popFront()
      if (entry !== undefined) {
        this.bytes -= entry.bytes
        return { value: entry.value, done: false }
      }
      if (this.failure !== undefined) throw this.failure
      if (this.ended) return UPLINK_DONE
      if (this.wake !== undefined) throw new Error('api gateway: Remote stream uplink inbox has one pending read')
      await new Promise<void>((resolve) => { this.wake = resolve })
    }
  }

  /** Consumer stopped reading: later items are dropped, a pending read ends. */
  return(): Promise<IteratorResult<undefined>> {
    this.closed = true
    this.queue.clear()
    this.bytes = 0
    this.signal()
    return Promise.resolve(UPLINK_DONE)
  }

  private violate(error: RemoteError<'gateway/protocol' | 'gateway/uplink-overflow'>): void {
    this.fail(error)
    this.onViolation(error)
  }

  private signal(): void {
    const wake = this.wake
    this.wake = undefined
    wake?.()
  }
}

class UplinkTooLargeError extends Error {}

/** Whether the Client asked for an event stream, so a plain navigation never opens one. */
function acceptsEvents(req: IncomingMessage): boolean {
  const accept = req.headers.accept
  return typeof accept === 'string'
    && accept.split(',').some(type => type.split(';', 1)[0]?.trim().toLowerCase() === REMOTE_STREAM_MUX_MEDIA_TYPE)
}

/** Read one complete uplink body, rejecting the session's largest permitted frame. */
async function readUplink(req: IncomingMessage): Promise<string> {
  const declared = req.headers['content-length']
  if (declared !== undefined && Number(declared) > REMOTE_STREAM_UPLINK_MAX_BYTES) throw new UplinkTooLargeError()
  const chunks: Buffer[] = []
  let bytes = 0
  for await (const chunk of req) {
    const buffer = chunk as Buffer
    bytes += buffer.byteLength
    if (bytes > REMOTE_STREAM_UPLINK_MAX_BYTES) throw new UplinkTooLargeError()
    chunks.push(buffer)
  }
  return Buffer.concat(chunks).toString('utf8')
}

/** Answer one transport-level outcome without keeping the exchange open. */
function writeText(res: ServerResponse, status: number, body: string): void {
  if (res.destroyed || res.writableEnded) return
  const bytes = Buffer.byteLength(body)
  res.writeHead(status, {
    'cache-control': 'no-store',
    'content-type': 'text/plain; charset=utf-8',
    'content-length': String(bytes),
  })
  res.end(body)
}
