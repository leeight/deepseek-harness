// Shared plumbing for the HTTP Remote mux (`/api/remote.mux`) in the web e2e lane.
import type { Page } from 'playwright'

/** Session header the Host sets on every mux downlink. */
const MUX_SESSION_HEADER = 'x-dsh-mux-session'

/** One downlink frame the page observed. */
export interface ObservedMuxFrame {
  /** Arrival order across every carrier the page opened. */
  readonly seq: number
  /** The SSE `data:` payload, still JSON text. */
  readonly frame: string
}

/** Everything the in-page probe recorded up to the call. */
export interface MuxProbeSnapshot {
  /** Downlink frames in arrival order, including frames currently held. */
  readonly downlink: readonly string[]
  /** Uplink frames in send order. */
  readonly uplink: readonly string[]
  /** Downlink attempts, including attempts refused by the probe. */
  readonly connections: number
  /** Frames the gate held back from the application. */
  readonly held: readonly ObservedMuxFrame[]
}

/** Decides whether an observed downlink frame reaches the application now or stays held. */
export type MuxGateDecision = 'pass' | 'hold'

/** Options for {@link installMuxProbe}. */
export interface MuxProbeOptions {
  /**
   * Intercepts every downlink frame before the application reads it.
   *
   * Awaited per frame in arrival order, so a slow decision delays later frames but never reorders them. The `seq`
   * argument matches the sequence number {@link MuxProbe.release} takes.
   */
  readonly gate?: (frame: string, session: string, seq: number) => MuxGateDecision | Promise<MuxGateDecision>
}

/** Test-side control over the page's mux carriers. */
export interface MuxProbe {
  /** Frames observed so far. */
  snapshot(): Promise<MuxProbeSnapshot>
  /** Downlink attempts so far, including refused ones. */
  connections(): Promise<number>
  /** Frames the gate is holding back from the application. */
  held(): Promise<readonly ObservedMuxFrame[]>
  /**
   * Hands held frames to the application.
   * @param frames - Seqs to release; omit to release everything held.
   * @returns How many frames remain held.
   */
  release(frames?: readonly number[]): Promise<number>
  /**
   * Fails every open downlink without refusing later attempts.
   * @param blocked - `true` refuses every downlink attempt until reset to `false`.
   */
  refuse(blocked: boolean): Promise<void>
  /**
   * Answer every later downlink attempt with a silent carrier that never sends a frame.
   *
   * The application sees an open carrier, so only the retry timer or {@link MuxProbe.kill} replaces it.
   * @param blocked - `true` stops reaching the Host on new attempts until reset to `false`.
   */
  hold(blocked: boolean): Promise<void>
  /** Closes every open downlink the way a lost carrier would. */
  kill(): Promise<void>
}

/** The probe's in-page API. */
interface PageProbe {
  snapshot(): MuxProbeSnapshot
  release(frames?: readonly number[]): number
  refuse(blocked: boolean): void
  hold(blocked: boolean): void
  kill(): void
}

/**
 * Wraps `window.fetch` to observe and steer the Remote mux.
 *
 * Installed through `page.addInitScript`, so it must stay self-contained. Downlink frames are re-encoded into a
 * stream the probe owns, which lets a test hold frames past their arrival and release them later.
 * @param options - Whether a gate binding decides frame delivery.
 */
function installPageProbe(options: { readonly gate: boolean }): void {
  const muxPath = '/api/remote.mux'
  const sessionHeader = 'x-dsh-mux-session'
  const encoder = new TextEncoder()
  const nativeFetch = globalThis.fetch.bind(globalThis)

  interface HeldFrame {
    readonly seq: number
    readonly frame: string
    readonly deliver: () => void
  }
  interface State {
    readonly downlink: string[]
    readonly uplink: string[]
    connections: number
    seq: number
    held: HeldFrame[]
  }
  const state: State = { downlink: [], uplink: [], connections: 0, seq: 0, held: [] }
  const carriers = new Set<AbortController>()
  /** Sessions the probe answered itself, so their uplinks never reach the Host. */
  const mockSessions = new Set<string>()
  let mockSessionsMinted = 0
  let refused = false
  let holding = false

  const muxUrl = (input: RequestInfo | URL): URL | undefined => {
    const raw = typeof input === 'string' ? input : input instanceof URL ? input.href : undefined
    if (raw === undefined) return undefined
    const url = new URL(raw, location.href)
    return url.pathname.endsWith(muxPath) ? url : undefined
  }

  /** Reads SSE `data:` frames, awaiting each visit before decoding the next line group. */
  const forEachFrame = async (
    body: ReadableStream<Uint8Array>,
    visit: (frame: string) => void | Promise<void>,
  ): Promise<void> => {
    const reader = body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    let data: string[] = []
    try {
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
            await visit(frame)
          } else if (line.startsWith('data:')) {
            data.push(line.slice('data:'.length).replace(/^ /u, ''))
          }
        }
      }
    } finally {
      reader.releaseLock()
    }
  }

  /**
   * Hands back an open carrier that never carries a frame, so the application waits on it.
   *
   * {@link MuxProbe.kill} errors its body, which is what the application reports as a lost carrier.
   */
  const holdCarrier = (): Response => {
    mockSessionsMinted += 1
    const session = `mock-mux-${mockSessionsMinted}`
    mockSessions.add(session)
    const kill = new AbortController()
    carriers.add(kill)
    let cancelled = false
    let controller: ReadableStreamDefaultController<Uint8Array> | undefined
    const stream = new ReadableStream<Uint8Array>({
      start: (next) => { controller = next },
      cancel: () => {
        cancelled = true
        carriers.delete(kill)
        kill.abort()
      },
    })
    kill.signal.addEventListener('abort', () => {
      carriers.delete(kill)
      if (!cancelled) controller?.error(new TypeError('mux carrier closed'))
    }, { once: true })
    return new Response(stream, {
      status: 200,
      statusText: 'OK',
      headers: { 'cache-control': 'no-store', 'content-type': 'text/event-stream', [sessionHeader]: session },
    })
  }

  const openDownlink = async (url: URL, init: RequestInit | undefined): Promise<Response> => {
    state.connections += 1
    if (refused) throw new TypeError('mux downlink refused')
    if (holding) return holdCarrier()
    const kill = new AbortController()
    carriers.add(kill)
    const signal = init?.signal == null ? kill.signal : AbortSignal.any([init.signal, kill.signal])
    let response: Response
    try {
      response = await nativeFetch(url, { ...init, signal })
    } catch (error) {
      carriers.delete(kill)
      throw error
    }
    const body = response.body
    if (!response.ok || body === null) {
      carriers.delete(kill)
      return response
    }
    const session = response.headers.get(sessionHeader) ?? ''
    let applicationClosed = false
    let controller: ReadableStreamDefaultController<Uint8Array> | undefined
    const stream = new ReadableStream<Uint8Array>({
      start: (next) => { controller = next },
      cancel: () => {
        applicationClosed = true
        kill.abort()
      },
    })
    const deliver = (frame: string): void => {
      try {
        controller?.enqueue(encoder.encode(`data: ${frame}\n\n`))
      } catch {
        // The carrier the frame belongs to is already gone; the application's reader saw its end.
      }
    }
    void (async () => {
      let failure: Error | undefined
      try {
        await forEachFrame(body, async (frame) => {
          state.downlink.push(frame)
          const seq = state.seq++
          const gate = options.gate ? Reflect.get(globalThis, '__dshMuxGate') : undefined
          if (gate === undefined) {
            deliver(frame)
            return
          }
          const decision = await gate({ frame, session, seq }) as MuxGateDecision
          if (decision === 'hold') state.held.push({ seq, frame, deliver: () => { deliver(frame) } })
          else deliver(frame)
        })
      } catch (error) {
        if (!applicationClosed && !kill.signal.aborted && init?.signal?.aborted !== true) {
          failure = error instanceof Error ? error : new Error(String(error))
        }
      } finally {
        carriers.delete(kill)
        try {
          if (failure === undefined) controller?.close()
          else controller?.error(failure)
        } catch {
          // The application cancelled the carrier first.
        }
      }
    })()
    return new Response(stream, { status: response.status, statusText: response.statusText, headers: response.headers })
  }

  globalThis.fetch = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const url = muxUrl(input)
    if (url === undefined) return await nativeFetch(input, init)
    const method = (init?.method ?? 'GET').toUpperCase()
    if (method === 'GET') return await openDownlink(url, init)
    const body = init?.body
    if (typeof body === 'string' && body !== '') {
      for (const frame of body.split('\n')) if (frame !== '') state.uplink.push(frame)
    }
    const session = url.searchParams.get('session')
    if (session !== null && mockSessions.has(session)) return new Response(null, { status: 204 })
    return await nativeFetch(input, init)
  }

  const probe: PageProbe = {
    snapshot: () => ({
      downlink: [...state.downlink],
      uplink: [...state.uplink],
      connections: state.connections,
      held: state.held.map(({ seq, frame }) => ({ seq, frame })),
    }),
    release: (frames) => {
      const wanted = frames === undefined ? undefined : new Set(frames)
      const kept: HeldFrame[] = []
      for (const held of state.held) {
        if (wanted === undefined || wanted.has(held.seq)) held.deliver()
        else kept.push(held)
      }
      state.held = kept
      return kept.length
    },
    refuse: (blocked) => { refused = blocked },
    hold: (blocked) => { holding = blocked },
    kill: () => {
      for (const carrier of carriers) carrier.abort()
      carriers.clear()
    },
  }
  Reflect.set(globalThis, '__dshMuxProbe', probe)
}

/**
 * Installs the in-page mux probe.
 *
 * Call before navigating: the wrapper has to exist before the client opens its first carrier.
 * @param page - Page that will load the Web client.
 * @param options - Optional per-frame gate.
 * @returns Control over the page's mux carriers.
 */
export async function installMuxProbe(page: Page, options: MuxProbeOptions = {}): Promise<MuxProbe> {
  const gate = options.gate
  if (gate !== undefined) {
    await page.exposeBinding(
      '__dshMuxGate',
      async (_source, observed: { readonly frame: string; readonly session: string; readonly seq: number }) =>
        await gate(observed.frame, observed.session, observed.seq),
    )
  }
  await page.addInitScript(installPageProbe, { gate: gate !== undefined })
  return {
    snapshot: async () => await page.evaluate(
      () => (Reflect.get(globalThis, '__dshMuxProbe') as PageProbe).snapshot(),
    ),
    connections: async () => await page.evaluate(
      () => (Reflect.get(globalThis, '__dshMuxProbe') as PageProbe).snapshot().connections,
    ),
    held: async () => await page.evaluate(
      () => (Reflect.get(globalThis, '__dshMuxProbe') as PageProbe).snapshot().held,
    ),
    release: async frames => await page.evaluate(
      selected => (Reflect.get(globalThis, '__dshMuxProbe') as PageProbe).release(selected),
      frames,
    ),
    refuse: async (blocked) => {
      await page.evaluate(
        (value) => { (Reflect.get(globalThis, '__dshMuxProbe') as PageProbe).refuse(value) },
        blocked,
      )
    },
    hold: async (blocked) => {
      await page.evaluate(
        (value) => { (Reflect.get(globalThis, '__dshMuxProbe') as PageProbe).hold(value) },
        blocked,
      )
    },
    kill: async () => {
      await page.evaluate(() => { (Reflect.get(globalThis, '__dshMuxProbe') as PageProbe).kill() })
    },
  }
}

/** Frames the recovery scenarios tally while a carrier is open and after it is replaced. */
export interface MuxStreamCounts {
  /** `ready` items across every stream. */
  readonly ready: number
  /** `session/follow` snapshots, i.e. one per carrier that reached the baseline endpoint. */
  readonly sessionBaselines: number
  /** Error payloads the Host sent on any stream. */
  readonly errors: readonly unknown[]
}

/**
 * Tallies a mux transcript: uplink `open` frames name the endpoint its streamId addresses.
 * @param observed - Frames recorded by {@link MuxProbe.snapshot}.
 * @returns Counts the recovery scenarios poll.
 */
export function countMuxStreams(observed: Pick<MuxProbeSnapshot, 'downlink' | 'uplink'>): MuxStreamCounts {
  const endpoints = new Map<string, string>()
  const counts = { ready: 0, sessionBaselines: 0, errors: [] as unknown[] }
  for (const text of observed.uplink) {
    const frame = JSON.parse(text) as { type?: string; streamId?: string; endpoint?: string }
    if (frame.type === 'open' && frame.streamId !== undefined && frame.endpoint !== undefined) {
      endpoints.set(frame.streamId, frame.endpoint)
    }
  }
  for (const text of observed.downlink) {
    const message = JSON.parse(text) as {
      type?: string
      streamId?: string
      value?: { type?: string }
      error?: unknown
    }
    if (message.type === 'error') counts.errors.push(message.error)
    if (message.type === 'item' && message.value?.type === 'ready') counts.ready += 1
    if (message.type === 'item' && message.streamId !== undefined
      && endpoints.get(message.streamId) === 'session/follow' && message.value?.type === 'snapshot') {
      counts.sessionBaselines += 1
    }
  }
  return counts
}

/**
 * One open mux carrier as a Node client sees it: SSE downlink plus session-addressed uplink.
 *
 * Node-side specs need this because they talk to the Host without a browser.
 */
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
  const session = response.headers.get(MUX_SESSION_HEADER)
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
      await forEachMuxFrame(body, (frame) => {
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
async function forEachMuxFrame(body: ReadableStream<Uint8Array>, visit: (frame: string) => void): Promise<void> {
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
