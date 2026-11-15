/** Node client for the HTTP Remote mux: SSE downlink plus session-addressed uplink. */

/** Header the Host sets on every mux downlink, and the uplink addresses its session through. */
const SESSION_HEADER = 'x-dsh-mux-session'

/** One open mux carrier. */
export interface MuxCarrier {
  /** Posts frames as one uplink body. */
  send(frames: readonly string[]): void
  /** Registers a downlink frame listener. */
  onFrame(listener: (frame: string) => void): void
  /** Registers a listener for the downlink ending. */
  onClose(listener: () => void): void
  /** Registers a listener for a downlink failure. */
  onError(listener: (error: Error) => void): void
  /** Closes the downlink. */
  close(): Promise<void>
}

/**
 * Opens the mux downlink and answers server pings.
 * @param url - Mux URL on the Host origin.
 * @param headers - Request headers, including any session cookie.
 * @returns The carrier, once the Host accepted the downlink.
 */
export async function openMuxCarrier(url: string, headers: Record<string, string>): Promise<MuxCarrier> {
  const abort = new AbortController()
  const response = await fetch(url, {
    method: 'GET',
    headers: { ...headers, accept: 'text/event-stream' },
    signal: abort.signal,
  })
  const session = response.headers.get(SESSION_HEADER)
  const body = response.body
  if (!response.ok || body === null || session === null) {
    abort.abort()
    throw new Error(`mux downlink refused: ${String(response.status)}`)
  }
  const uplink = new URL(url)
  uplink.searchParams.set('session', session)
  const frameListeners: ((frame: string) => void)[] = []
  const closeListeners: (() => void)[] = []
  const errorListeners: ((error: Error) => void)[] = []
  const drained = (async () => {
    try {
      await forEachFrame(body, (frame) => {
        for (const listener of frameListeners) listener(frame)
      })
    } catch (error) {
      if (!abort.signal.aborted) {
        for (const listener of errorListeners) listener(error instanceof Error ? error : new Error(String(error)))
      }
    } finally {
      for (const listener of closeListeners) listener()
    }
  })()
  return {
    send: (frames) => {
      void fetch(uplink, {
        method: 'POST',
        headers: { ...headers, 'content-type': 'application/x-ndjson' },
        body: frames.join('\n'),
        signal: abort.signal,
      }).catch((error: unknown) => {
        for (const listener of errorListeners) listener(error instanceof Error ? error : new Error(String(error)))
      })
    },
    onFrame: (listener) => { frameListeners.push(listener) },
    onClose: (listener) => { closeListeners.push(listener) },
    onError: (listener) => { errorListeners.push(listener) },
    close: async () => {
      abort.abort()
      await drained
    },
  }
}

/**
 * Reads SSE `data:` frames from a mux downlink.
 * @param body - The downlink body.
 * @param visit - Called for every frame, in order.
 */
async function forEachFrame(body: ReadableStream<Uint8Array>, visit: (frame: string) => void): Promise<void> {
  const reader = body.getReader()
  const decoder = new TextDecoder()
  let buffer = ''
  let data: string[] = []
  for (;;) {
    const { done, value } = await reader.read()
    if (done) return
    buffer += decoder.decode(value, { stream: true })
    for (;;) {
      const newline = buffer.indexOf('\n')
      if (newline < 0) break
      const line = buffer.slice(0, newline).replace(/\r$/u, '')
      buffer = buffer.slice(newline + 1)
      if (line === '') {
        if (data.length === 0) continue
        const frame = data.join('\n')
        data = []
        visit(frame)
      } else if (line.startsWith('data:')) {
        data.push(line.slice('data:'.length).replace(/^ /u, ''))
      }
    }
  }
}
