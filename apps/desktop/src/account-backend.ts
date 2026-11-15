/** Native account commands and Gateway state stream; no renderer receives credentials. */
import { randomUUID } from 'node:crypto'
import {
  parseRemoteStreamServerMessage,
  parseRemoteStreamTransportFrame,
  REMOTE_STREAM_MUX_PATH,
  REMOTE_STREAM_SESSION_HEADER,
  REMOTE_STREAM_SESSION_PARAM,
  type RemoteStreamPongFrame,
} from '@deepseek-ai/dsh-api-gateway/stream-protocol'
import type { AccountClientMetadata, AccountView, SignInAttemptId } from '@deepseek-ai/dsh-deepseek-account/types'

/** Authenticated unary caller shared with native onboarding. */
export type AccountInvoke = (request: { namespace: string; method: string; args: Record<string, unknown> }) => Promise<unknown>

/** Decode the UI-safe state received across HTTP or WebSocket. @param value - wire value. @returns account projection. */
export function accountView(value: unknown): AccountView {
  if (typeof value !== 'object' || value === null || !('status' in value)
    || !['signed-out', 'credential-stored'].includes(String(value.status)) || !('attempt' in value)) {
    throw new Error('desktop account: invalid state')
  }
  if (!('links' in value) || typeof value.links !== 'object' || value.links === null
    || !('usageUrl' in value.links) || typeof value.links.usageUrl !== 'string'
    || !('topUpUrl' in value.links) || typeof value.links.topUpUrl !== 'string') {
    throw new Error('desktop account: invalid platform links')
  }
  validateBrowserDestination(value.links.usageUrl)
  validateBrowserDestination(value.links.topUpUrl)
  const attempt = value.attempt
  if (attempt !== null && (typeof attempt !== 'object' || !('id' in attempt) || typeof attempt.id !== 'string'
    || !('phase' in attempt) || !['initializing', 'waiting-browser', 'exchanging', 'committing', 'succeeded', 'cancelled', 'expired', 'failed'].includes(String(attempt.phase))
    || ('authorizeUrl' in attempt && typeof attempt.authorizeUrl !== 'string')
    || ('expiresAt' in attempt && (typeof attempt.expiresAt !== 'number' || !Number.isFinite(attempt.expiresAt)))
    || ('errorCode' in attempt && !['no-response', 'network', 'protocol', 'expired', 'storage'].includes(String(attempt.errorCode))))) {
    throw new Error('desktop account: invalid attempt')
  }
  if (attempt !== null && 'authorizeUrl' in attempt) {
    validateBrowserDestination(String(attempt.authorizeUrl))
  }
  const parsed = value as AccountView
  return {
    status: parsed.status,
    links: { usageUrl: parsed.links.usageUrl, topUpUrl: parsed.links.topUpUrl },
    attempt: parsed.attempt === null ? null : {
      id: parsed.attempt.id, phase: parsed.attempt.phase,
      ...parsed.attempt.authorizeUrl === undefined ? {} : { authorizeUrl: parsed.attempt.authorizeUrl },
      ...parsed.attempt.expiresAt === undefined ? {} : { expiresAt: parsed.attempt.expiresAt },
      ...parsed.attempt.errorCode === undefined ? {} : { errorCode: parsed.attempt.errorCode },
    },
  }
}

/** Only HTTP loopback or HTTPS destinations can leave the native app. */
function validateBrowserDestination(value: string): void {
  const url = new URL(value)
  const loopback = ['localhost', '127.0.0.1', '[::1]'].includes(url.hostname)
  if (url.username || url.password || !(url.protocol === 'https:' || (loopback && url.protocol === 'http:'))) {
    throw new Error('desktop account: invalid browser destination')
  }
}

/** Native account operations and explicitly owned stream lifetime. */
export interface DesktopAccountBackend {
  /** @returns current account snapshot. */
  state(): Promise<AccountView>
  /** @param client - this window's identity and current language. @returns new or already-running login attempt. */
  start(client: AccountClientMetadata): Promise<AccountView>
  /** @param id - attempt to cancel. @returns cancellation or completed commit state. */
  cancel(id: SignInAttemptId): Promise<AccountView>
  /** @param client - this window's identity. @returns state after local sign-out. */
  signOut(client: AccountClientMetadata): Promise<AccountView>
  /**
   * @param listener - state recipient.
   * @param failed - stream failure recipient.
   * @param expired - live credential-expiry recipient.
   * @param onAnalyticsEnabledChanged - optional Desktop collection-policy recipient; disconnected streams publish false.
   * @returns stream disposer.
   */
  watch(
    listener: (state: AccountView) => void, failed: () => void, expired: () => void,
    onAnalyticsEnabledChanged?: (enabled: boolean) => void,
  ): () => void
}

/**
 * Connect native account operations to the standard authenticated Web backend.
 * @param origin - Host Web origin.
 * @param invoke - validated unary RPC caller.
 * @param cookie - Host authentication Cookie header for the account mux session.
 * @returns account operations; watch callers own their subscriptions.
 */
export function desktopAccountBackend(origin: string, invoke: AccountInvoke, cookie: string): DesktopAccountBackend {
  const call = async (method: string, args: Record<string, unknown> = {}): Promise<AccountView> =>
    accountView(await invoke({ namespace: 'account', method, args }))
  return {
    state: () => call('getState'),
    start: client => call('startSignIn', { client, callbackOrigin: new URL(origin).origin, loginSource: 'desktop' }),
    cancel: attemptId => call('cancelSignIn', { attemptId }), signOut: client => call('signOut', { client }),
    watch(listener, failed, expired, onAnalyticsEnabledChanged) {
      let closed = false
      let attempt: AbortController | undefined
      let retry: ReturnType<typeof setTimeout> | undefined
      const watchMux = async (signal: AbortSignal): Promise<void> => {
        const streamId = randomUUID()
        const expiryStreamId = randomUUID()
        const analyticsPolicyStreamId = randomUUID()
        const downlink = new URL(REMOTE_STREAM_MUX_PATH, origin)
        const response = await fetch(downlink, { headers: { accept: 'text/event-stream', cookie }, signal })
        const session = response.headers.get(REMOTE_STREAM_SESSION_HEADER)
        if (!response.ok || response.body === null || session === null) {
          throw new Error(`desktop account: mux downlink was refused with status ${String(response.status)}`)
        }
        const uplink = new URL(downlink)
        uplink.searchParams.set(REMOTE_STREAM_SESSION_PARAM, session)
        // Frames leave in one request per batch, so their order survives.
        const post = async (frames: readonly object[]): Promise<void> => {
          if (frames.length === 0) return
          const posted = await fetch(uplink, {
            method: 'POST',
            headers: { cookie, 'content-type': 'application/x-ndjson' },
            body: frames.map(frame => JSON.stringify(frame)).join('\n'),
            signal,
          })
          if (!posted.ok) throw new Error(`desktop account: mux uplink was refused with status ${String(posted.status)}`)
        }
        // Logical streams share this session; open them in one batch before reading.
        await post([
          ...(onAnalyticsEnabledChanged === undefined ? [] : [
            { type: 'open', streamId: analyticsPolicyStreamId, endpoint: 'productAnalytics/watchPolicy', payload: { args: {} } },
          ]),
          { type: 'open', streamId: expiryStreamId, endpoint: 'account/watchExpiry', payload: { args: {} } },
          { type: 'open', streamId, endpoint: 'account/watch', payload: { args: {} } },
        ])
        for await (const text of muxFrames(response.body)) {
          const transport = parseRemoteStreamTransportFrame(text)
          if (transport !== undefined) {
            // Answering a liveness ping over the uplink keeps the session open.
            if (transport.type === 'ping') {
              const pong: RemoteStreamPongFrame = { type: 'pong' }
              await post([pong])
            }
            continue
          }
          const frame = parseRemoteStreamServerMessage(text)
          if (frame.streamId === analyticsPolicyStreamId) {
            if (frame.type === 'item' && typeof frame.value === 'boolean') onAnalyticsEnabledChanged?.(frame.value)
            else onAnalyticsEnabledChanged?.(false)
            continue
          }
          if (frame.streamId === expiryStreamId && frame.type === 'item' && frame.value === 'session-expired') { expired(); continue }
          if (frame.streamId !== streamId) throw new Error('desktop account: unexpected stream')
          if (frame.type !== 'item') break
          const update = frame.value
          if (typeof update !== 'object' || update === null || !('type' in update)
            || (update.type !== 'snapshot' && update.type !== 'credentials-changed') || !('state' in update)) {
            throw new Error('desktop account: invalid stream update')
          }
          listener(accountView(update.state))
        }
        throw new Error('desktop account: mux session ended')
      }
      const connect = (): void => {
        const controller = new AbortController()
        attempt = controller
        void watchMux(controller.signal).catch(() => {
          // A lost session fails the subscription once and reconnects on a timer;
          // every logical stream reopens on the next attempt.
          if (closed) return
          onAnalyticsEnabledChanged?.(false)
          failed()
          retry = setTimeout(connect, 1000)
        })
      }
      connect()
      return () => { closed = true; onAnalyticsEnabledChanged?.(false); clearTimeout(retry); attempt?.abort() }
    },
  }
}

/**
 * Decode one `text/event-stream` downlink body into frame bodies.
 * @param body - downlink body reader source.
 * @returns every complete frame body, in arrival order.
 */
async function* muxFrames(body: ReadableStream<Uint8Array>): AsyncGenerator<string> {
  const decoder = new TextDecoder()
  const reader = body.getReader()
  const data: string[] = []
  let pending = ''
  const drain = (text: string): string[] => {
    pending += text
    const frames: string[] = []
    let end = pending.indexOf('\n')
    while (end !== -1) {
      const line = pending.slice(0, end).replace(/\r$/, '')
      pending = pending.slice(end + 1)
      end = pending.indexOf('\n')
      // A blank line dispatches the fields collected for this event.
      if (line === '') {
        if (data.length > 0) frames.push(data.join('\n'))
        data.length = 0
        continue
      }
      // A leading colon is a comment, the shape Host heartbeats take as events.
      if (line.startsWith(':')) continue
      if (line.startsWith('data:')) data.push(line.slice('data:'.length).replace(/^ /, ''))
    }
    return frames
  }
  try {
    while (true) {
      const { value, done } = await reader.read()
      if (done) return
      for (const frame of drain(decoder.decode(value, { stream: true }))) yield frame
    }
  } finally {
    void reader.cancel().catch(() => undefined)
  }
}
