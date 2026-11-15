import { createServer, request as httpRequest, type IncomingHttpHeaders, type Server, type ServerResponse } from 'node:http'
import { Context } from '@deepseek-ai/cordis'
import { remoteErrorOf, type PeerId, type PeerScope } from '@deepseek-ai/dsh-typert-protocol'
import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  RemoteStreamMuxServer,
  type RemoteStreamFailureMapper,
  type RemoteStreamOpener,
} from '../src/stream-server.ts'
import {
  REMOTE_STREAM_MUX_MEDIA_TYPE,
  REMOTE_STREAM_SESSION_HEADER,
  REMOTE_STREAM_SESSION_PARAM,
  REMOTE_STREAM_UPLINK_MAX_BYTES,
} from '../src/stream-protocol.ts'

interface RunningMux {
  readonly http: Server
  readonly mux: RemoteStreamMuxServer
  /** Absolute mux URL on the fixture server. */
  readonly url: string
  /** Peer every downlink is admitted as. */
  readonly admitted: PeerScope
  /** Downlink responses the mux wrote, in request order, so a spec can destroy one. */
  readonly downlinks: ServerResponse[]
  /** Admit later uplinks as `peer` instead of {@link admitted}, as a foreign Client would be. */
  admitUplinkAs(peer: PeerScope | undefined): void
}

const running = new Set<RunningMux>()

afterEach(async () => {
  await Promise.all([...running].map(async (entry) => {
    running.delete(entry)
    await entry.mux.close().catch(() => undefined)
    await closeHttp(entry.http)
  }))
})

describe('Remote stream mux server downlink', () => {
  it('opens an event stream with a session id and no caching or buffering', async () => {
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => waitForAbort(control.signal))
    const downlink = await openMux(entry)

    expect(downlink.status).toBe(200)
    expect(downlink.headers['content-type']).toBe(REMOTE_STREAM_MUX_MEDIA_TYPE)
    expect(downlink.headers['cache-control']).toBe('no-store')
    expect(downlink.headers['x-accel-buffering']).toBe('no')
    expect(downlink.session).toMatch(/^[0-9a-f-]{36}$/u)

    downlink.close()
    await downlink.closed
  })

  it('refuses a request that does not accept an event stream', async () => {
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => waitForAbort(control.signal))
    const downlink = await openMux(entry, { accept: 'application/json' })

    await downlink.closed
    expect(downlink.status).toBe(406)
    expect(downlink.text()).toBe('events required')
    expect(downlink.session).toBeUndefined()
  })

  it('writes liveness pings and no application frame on an idle stream', async () => {
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => waitForAbort(control.signal), 20)
    const downlink = await openMux(entry, { answerPings: false })

    await vi.waitFor(() => { expect(downlink.pings()).toBeGreaterThanOrEqual(2) })
    expect(downlink.frames).toEqual([])

    downlink.close()
    await downlink.closed
  })

  it('drops a peer that stops answering after two missed heartbeats', async () => {
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => waitForAbort(control.signal), 20)
    const downlink = await openMux(entry, { answerPings: false })

    await downlink.closed
    expect(downlink.pings()).toBeGreaterThanOrEqual(2)
  })

  it('keeps a peer that answers every ping and never surfaces pongs as frames', async () => {
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => waitForAbort(control.signal), 120)
    const downlink = await openMux(entry)

    await vi.waitFor(() => { expect(downlink.pings()).toBeGreaterThanOrEqual(4) })
    await delay(150)
    expect(downlink.ended()).toBe(false)
    expect(downlink.frames).toEqual([])

    downlink.close()
    await downlink.closed
  })

  it('counts any uplink frame as liveness', async () => {
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => waitForAbort(control.signal), 40)
    const downlink = await openMux(entry, { answerPings: false })
    const session = sessionOf(downlink)
    const traffic = setInterval(() => {
      void postUplink(entry, session, cancelFrame('absent')).catch(() => undefined)
    }, 5)

    try {
      await vi.waitFor(() => { expect(downlink.pings()).toBeGreaterThanOrEqual(4) })
      expect(downlink.ended()).toBe(false)
    } finally {
      clearInterval(traffic)
    }

    await downlink.closed
  })

  it('counts a downlink write that waited for the response to drain as liveness', async () => {
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => waitForAbort(control.signal), 20)
    const downlink = await openMux(entry, { answerPings: false })
    const response = downlinkResponse(entry)
    // A write the response cannot take is the only downlink evidence that the
    // peer still reads, so it counts only once the response drains.
    const write = vi.spyOn(response, 'write').mockImplementation(() => false)
    const drain = setInterval(() => { response.emit('drain') }, 5)

    try {
      await vi.waitFor(() => { expect(write.mock.calls.length).toBeGreaterThanOrEqual(3) })
      expect(downlink.ended()).toBe(false)
    } finally {
      clearInterval(drain)
      write.mockRestore()
    }

    await downlink.closed
  })

  it('does not count a downlink write that completed at once as liveness', async () => {
    const stop = Promise.withResolvers<undefined>()
    const entry = await startMux(async () => steadyItems(stop.promise), 20)
    const downlink = await openMux(entry, { answerPings: false })

    try {
      expect((await postUplink(entry, sessionOf(downlink), openFrame('flowing'))).status).toBe(204)
      await downlink.closed
      expect(downlink.frames.some(frame => frame.type === 'item')).toBe(true)
      expect(downlink.frames.some(frame => frame.type === 'end')).toBe(false)
    } finally {
      stop.resolve(undefined)
    }
  })

  it('keeps the session when a pong lands before the final liveness check', async () => {
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => waitForAbort(control.signal), 100)
    // Captured before the first tick, so the check a missed-interval streak
    // schedules is the one this test runs by hand.
    const check = captureNextImmediate()
    const downlink = await openMux(entry, { answerPings: false })

    try {
      const runCheck = await check.next
      await postUplink(entry, sessionOf(downlink), '{"type":"pong"}')
      runCheck()
      await delay(120)
      expect(downlink.ended()).toBe(false)
    } finally {
      check.restore()
      downlink.close()
    }

    await downlink.closed
  })

  it('rejects malformed, duplicate, and unknown uplink frames and ends that session', async () => {
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => waitForAbort(control.signal))
    const batches = [
      'not json',
      [openFrame('same'), openFrame('same')],
      [openFrame('typed'), JSON.stringify({ type: 'input', streamId: 'typed', value: 'unexpected' })],
    ]

    for (const batch of batches) {
      const downlink = await openMux(entry)
      const reply = await postUplink(entry, sessionOf(downlink), batch)
      expect(reply.status).toBe(400)
      expect(reply.text).toBe('invalid uplink frame')
      await downlink.closed
    }
  })

  it('does not write an end frame after a clean source cancellation', async () => {
    const opened = Promise.withResolvers<undefined>()
    const returned = Promise.withResolvers<undefined>()
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => {
      opened.resolve(undefined)
      return cleanlyCancelled(control.signal, () => { returned.resolve(undefined) })
    })
    const downlink = await openMux(entry)
    const session = sessionOf(downlink)

    expect((await postUplink(entry, session, openFrame('cancelled'))).status).toBe(204)
    await opened.promise
    expect((await postUplink(entry, session, cancelFrame('cancelled'))).status).toBe(204)
    await returned.promise
    // The cancellation path awaits nothing before it inspects the stream, so one
    // macrotask hop drains every microtask it can schedule.
    await new Promise<void>((resolve) => { setImmediate(resolve) })
    expect(downlink.frames).toEqual([])

    downlink.close()
    await downlink.closed
  })

  it('ends the downlink when its response is destroyed while an item is pending', async () => {
    const released = Promise.withResolvers<undefined>()
    const opened = Promise.withResolvers<undefined>()
    const returned = Promise.withResolvers<undefined>()
    const entry = await startMux(async () => delayedItem(
      released.promise,
      () => { opened.resolve(undefined) },
      () => { returned.resolve(undefined) },
    ))
    const downlink = await openMux(entry)

    expect((await postUplink(entry, sessionOf(downlink), openFrame('write-failure'))).status).toBe(204)
    await opened.promise
    downlinkResponse(entry).destroy()
    released.resolve(undefined)

    await downlink.closed
    await returned.promise
  })

  it('leaves a failed stream silent once its downlink is gone', async () => {
    const opened = Promise.withResolvers<undefined>()
    const entry = await startMux(async () => {
      downlinkResponse(entry).destroy()
      opened.resolve(undefined)
      throw new Error('fixture opener failure')
    })
    const downlink = await openMux(entry)

    expect((await postUplink(entry, sessionOf(downlink), openFrame('silent'))).status).toBe(204)
    await opened.promise
    await downlink.closed
    expect(downlink.frames).toEqual([])
  })

  it('contains an item produced after its downlink closed', async () => {
    const released = Promise.withResolvers<undefined>()
    const opened = Promise.withResolvers<undefined>()
    const returned = Promise.withResolvers<undefined>()
    const entry = await startMux(async () => delayedItem(
      released.promise,
      () => { opened.resolve(undefined) },
      () => { returned.resolve(undefined) },
    ))
    const downlink = await openMux(entry)

    expect((await postUplink(entry, sessionOf(downlink), openFrame('late-item'))).status).toBe(204)
    await opened.promise
    downlink.close()
    await downlink.closed
    released.resolve(undefined)

    await returned.promise
  })

  it('ends every open downlink on close and tolerates a repeated close', async () => {
    const opened = Promise.withResolvers<undefined>()
    const returned = Promise.withResolvers<undefined>()
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => {
      opened.resolve(undefined)
      return cleanlyCancelled(control.signal, () => { returned.resolve(undefined) })
    })
    const active = await openMux(entry)
    const idle = await openMux(entry)

    expect((await postUplink(entry, sessionOf(active), openFrame('active'))).status).toBe(204)
    await opened.promise
    await entry.mux.close()
    await Promise.all([active.closed, idle.closed])
    await returned.promise
    await expect(entry.mux.close()).resolves.toBeUndefined()
  })
})

describe('Remote stream mux server sessions', () => {
  it('keeps two sessions and their stream tables apart', async () => {
    const entry = await startMux(async (_endpoint, _payload, uplink) => echoUplink(uplink))
    const left = await openMux(entry)
    const right = await openMux(entry)

    expect(sessionOf(left)).not.toBe(sessionOf(right))
    await postUplink(entry, sessionOf(left), [openFrame('shared'), itemFrame('shared', 'left')])
    await postUplink(entry, sessionOf(right), [openFrame('shared'), itemFrame('shared', 'right')])
    await vi.waitFor(() => {
      expect(left.frames).toEqual([{ type: 'item', streamId: 'shared', value: 'left' }])
      expect(right.frames).toEqual([{ type: 'item', streamId: 'shared', value: 'right' }])
    })

    left.close()
    right.close()
    await Promise.all([left.closed, right.closed])
  })

  it('refuses an uplink for a session it never opened', async () => {
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => waitForAbort(control.signal))
    const reply = await postUplink(entry, 'not-a-session', openFrame('unknown'))

    expect(reply.status).toBe(404)
    expect(reply.text).toBe('session not found')
  })

  it('refuses an uplink a different Peer posts', async () => {
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => waitForAbort(control.signal))
    const downlink = await openMux(entry)
    const session = sessionOf(downlink)
    const foreign = await fixturePeer()

    try {
      entry.admitUplinkAs(foreign)
      const foreignReply = await postUplink(entry, session, openFrame('foreign'))
      expect(foreignReply.status).toBe(404)
      expect(foreignReply.text).toBe('session not found')

      entry.admitUplinkAs(undefined)
      expect((await postUplink(entry, session, openFrame('own'))).status).toBe(204)
    } finally {
      entry.admitUplinkAs(undefined)
      await foreign.dispose()
    }

    downlink.close()
    await downlink.closed
  })

  it('rejects an uplink body above the frame limit and keeps the session', async () => {
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => waitForAbort(control.signal))
    const downlink = await openMux(entry)
    const session = sessionOf(downlink)

    const oversized = await postUplink(
      entry,
      session,
      'x'.repeat(REMOTE_STREAM_UPLINK_MAX_BYTES + 1),
      { chunked: true },
    )
    expect(oversized.status).toBe(413)
    expect(oversized.text).toBe('invalid uplink frame')

    expect((await postUplink(entry, session, cancelFrame('absent'))).status).toBe(204)
    expect(downlink.ended()).toBe(false)

    downlink.close()
    await downlink.closed
  })
})

describe('Remote stream mux server Peer binding', () => {
  it('refuses a downlink at once when the admitted Peer scope is already disposed', async () => {
    const peer = await fixturePeer()
    await peer.dispose()
    const opened = vi.fn()
    const entry = await startMux(async (_endpoint, _payload, _uplink, _peer, control) => {
      opened()
      return waitForAbort(control.signal)
    }, 2_000, 262_144, peer)

    await expect(refusedDownlink(entry)).resolves.toBe(true)
    expect(opened).not.toHaveBeenCalled()
  })

  it('hands the admitted Peer to every opener and ends the downlink when its scope is disposed', async () => {
    const peer = await fixturePeer()
    const seen: PeerScope[] = []
    const reasons: unknown[] = []
    const entry = await startMux(async (_endpoint, _payload, _uplink, opened, control) => {
      seen.push(opened)
      return cleanlyCancelled(control.signal, () => { reasons.push(control.signal.reason) })
    }, 2_000, 262_144, peer)
    const downlink = await openMux(entry)

    expect((await postUplink(entry, sessionOf(downlink), openFrame('bound'))).status).toBe(204)
    await vi.waitFor(() => { expect(seen).toHaveLength(1) })
    expect(seen[0]).toBe(peer)

    await peer.dispose()
    await downlink.closed
    await vi.waitFor(() => { expect(reasons).toEqual([new Error('Remote stream downlink closed')]) })
  })
})

describe('Remote stream mux server uplink', () => {
  it('buffers uplink frames sent before the opener resolves and half-closes on end', async () => {
    const release = Promise.withResolvers<undefined>()
    const entry = await startMux(async (_endpoint, _payload, uplink) => {
      await release.promise
      return echoUplink(uplink)
    })
    const downlink = await openMux(entry)
    const session = sessionOf(downlink)

    const buffered = await postUplink(entry, session, [openFrame('echo'), itemFrame('echo', 'one'), itemFrame('echo')])
    expect(buffered.status).toBe(204)
    expect(downlink.frames).toEqual([])

    release.resolve(undefined)
    await vi.waitFor(() => {
      expect(downlink.frames).toEqual([
        { type: 'item', streamId: 'echo', value: 'one' },
        { type: 'item', streamId: 'echo' },
      ])
    })

    await postUplink(entry, session, endFrame('echo'))
    await vi.waitFor(() => { expect(downlink.frames.at(-1)).toEqual({ type: 'end', streamId: 'echo' }) })
    expect(downlink.frames).toHaveLength(3)

    downlink.close()
    await downlink.closed
  })

  it('fails a stream on an item after end and keeps the session open', async () => {
    const entry = await startMux(async (endpoint, _payload, uplink, _peer, control) =>
      endpoint === 'fixture/echo' ? echoUplink(uplink) : waitForAbort(control.signal))
    const downlink = await openMux(entry)
    const session = sessionOf(downlink)

    await postUplink(entry, session, [openFrame('held'), endFrame('held'), endFrame('held'), itemFrame('held', 'late')])
    await vi.waitFor(() => {
      expect(downlink.frames).toEqual([{
        type: 'error',
        streamId: 'held',
        error: {
          code: 'gateway/protocol',
          message: 'api gateway: Remote stream uplink item after end',
          details: { endpoint: 'fixture/follow' },
        },
      }])
    })
    expect(downlink.ended()).toBe(false)

    await postUplink(entry, session, [openFrame('again', 'fixture/echo'), itemFrame('again', 'still served')])
    await vi.waitFor(() => {
      expect(downlink.frames.at(-1)).toEqual({ type: 'item', streamId: 'again', value: 'still served' })
    })

    downlink.close()
    await downlink.closed
  })

  it('fails a stream whose buffered uplink exceeds the configured bytes and drops later items', async () => {
    const release = Promise.withResolvers<undefined>()
    const entry = await startMux(async () => (async function *(): AsyncIterable<never> {
      await release.promise
    })(), 2_000, 48)
    const downlink = await openMux(entry)
    const session = sessionOf(downlink)

    const posted = await postUplink(entry, session, [
      openFrame('big'),
      itemFrame('big', 'x'.repeat(64)),
      itemFrame('big', 'dropped'),
      endFrame('big'),
    ])
    expect(posted.status).toBe(204)
    expect(downlink.frames).toEqual([])

    release.resolve(undefined)
    await vi.waitFor(() => {
      expect(downlink.frames).toEqual([{
        type: 'error',
        streamId: 'big',
        error: {
          code: 'gateway/uplink-overflow',
          message: 'api gateway: Remote stream uplink exceeded 48 buffered bytes',
          details: { endpoint: 'fixture/follow' },
        },
      }])
    })

    downlink.close()
    await downlink.closed
  })

  it('drops uplink frames for a stream it no longer owns and keeps the session open', async () => {
    const entry = await startMux(async (endpoint, _payload, uplink, _peer, control) =>
      endpoint === 'fixture/echo' ? echoUplink(uplink) : waitForAbort(control.signal))
    const downlink = await openMux(entry)

    const reply = await postUplink(entry, sessionOf(downlink), [
      itemFrame('absent', 1),
      endFrame('absent'),
      openFrame('live', 'fixture/echo'),
      itemFrame('live', 'served'),
    ])
    expect(reply.status).toBe(204)
    await vi.waitFor(() => {
      expect(downlink.frames).toEqual([{ type: 'item', streamId: 'live', value: 'served' }])
    })
    expect(downlink.ended()).toBe(false)

    downlink.close()
    await downlink.closed
  })

  it('ends a pending uplink read when the downlink closes', async () => {
    const read = Promise.withResolvers<unknown>()
    const opened = Promise.withResolvers<undefined>()
    const entry = await startMux(async (_endpoint, _payload, uplink, _peer, control) => {
      const iterator = uplink[Symbol.asyncIterator]()
      iterator.next().then(read.resolve, read.resolve)
      opened.resolve(undefined)
      return waitForAbort(control.signal)
    })
    const downlink = await openMux(entry)

    expect((await postUplink(entry, sessionOf(downlink), openFrame('pending'))).status).toBe(204)
    await opened.promise
    downlink.close()
    await downlink.closed

    await expect(read.promise).resolves.toMatchObject({ message: 'Remote stream downlink closed' })
  })

  it('drops uplink items after the Host stops reading', async () => {
    const release = Promise.withResolvers<undefined>()
    const returned = Promise.withResolvers<undefined>()
    const entry = await startMux(async (_endpoint, _payload, uplink) => {
      const iterator = uplink[Symbol.asyncIterator]()
      const first: IteratorResult<unknown, undefined> = await iterator.next()
      await iterator.return?.()
      returned.resolve(undefined)
      return (async function *(): AsyncIterable<unknown> {
        await release.promise
        yield { first: first.value, afterReturn: (await iterator.next()).done }
      })()
    })
    const downlink = await openMux(entry)
    const session = sessionOf(downlink)

    await postUplink(entry, session, [openFrame('closed'), itemFrame('closed', 'one')])
    await returned.promise
    await postUplink(entry, session, [itemFrame('closed', 'two'), endFrame('closed'), itemFrame('closed', 'three')])

    release.resolve(undefined)
    await vi.waitFor(() => {
      expect(downlink.frames).toEqual([
        { type: 'item', streamId: 'closed', value: { first: 'one', afterReturn: true } },
        { type: 'end', streamId: 'closed' },
      ])
    })

    downlink.close()
    await downlink.closed
  })

  it('reports a second uplink consumer and a second pending read as stream failures', async () => {
    const entry = await startMux(async (endpoint, _payload, uplink) => {
      const iterator = uplink[Symbol.asyncIterator]()
      if (endpoint === 'fixture/twice') uplink[Symbol.asyncIterator]()
      void iterator.next().catch(() => undefined)
      await iterator.next()
      return echoUplink(uplink)
    })
    const downlink = await openMux(entry)
    const session = sessionOf(downlink)

    await postUplink(entry, session, [openFrame('twice', 'fixture/twice'), openFrame('pending', 'fixture/pending')])
    await vi.waitFor(() => {
      expect(downlink.frames).toEqual(expect.arrayContaining([
        {
          type: 'error',
          streamId: 'twice',
          error: { code: 'internal', message: 'api gateway: Remote stream uplink inbox already has a consumer', details: {} },
        },
        {
          type: 'error',
          streamId: 'pending',
          error: { code: 'internal', message: 'api gateway: Remote stream uplink inbox has one pending read', details: {} },
        },
      ]))
    })
    expect(downlink.frames).toHaveLength(2)

    downlink.close()
    await downlink.closed
  })
})

describe('Remote stream mux server framing', () => {
  it('round-trips frames through one logical stream', async () => {
    const entry = await startMux(async (_endpoint, _payload, uplink) => echoUplink(uplink))
    const downlink = await openMux(entry)
    const session = sessionOf(downlink)
    const value = 'x'.repeat(2_048)

    await postUplink(entry, session, [openFrame('round'), itemFrame('round', value)])
    await vi.waitFor(() => {
      expect(downlink.frames).toEqual([{ type: 'item', streamId: 'round', value }])
    })

    await postUplink(entry, session, endFrame('round'))
    await vi.waitFor(() => { expect(downlink.frames.at(-1)).toEqual({ type: 'end', streamId: 'round' }) })

    downlink.close()
    await downlink.closed
  })
})

const mapFailure: RemoteStreamFailureMapper = (error) => {
  const remote = remoteErrorOf(error)
  if (remote !== undefined) return { code: remote.code, message: remote.message, details: remote.details }
  return {
    code: 'internal',
    message: error instanceof Error ? error.message : String(error),
    details: {},
  }
}

async function startMux(
  open: RemoteStreamOpener,
  heartbeatIntervalMs = 2_000,
  streamInboxBytes = 262_144,
  peer?: PeerScope,
): Promise<RunningMux> {
  const admitted = peer ?? await fixturePeer()
  const mux = new RemoteStreamMuxServer(open, mapFailure, { heartbeatIntervalMs, streamInboxBytes })
  const downlinks: ServerResponse[] = []
  let uplinkPeer: PeerScope | undefined
  const http = createServer((req, res) => {
    if (req.method === 'POST') {
      void mux.handleUplink(req, res, uplinkPeer ?? admitted).catch((error: unknown) => {
        res.destroy(error instanceof Error ? error : new Error(String(error)))
      })
      return
    }
    downlinks.push(res)
    mux.handleDownlink(req, res, admitted)
  })
  await new Promise<void>((resolve, reject) => {
    http.once('error', reject)
    http.listen(0, '127.0.0.1', () => {
      http.off('error', reject)
      resolve()
    })
  })
  const address = http.address()
  if (address === null || typeof address === 'string') throw new Error('fixture HTTP server has no TCP port')
  const entry: RunningMux = {
    http,
    mux,
    admitted,
    downlinks,
    url: `http://127.0.0.1:${String(address.port)}/api/remote.mux`,
    admitUplinkAs: (next) => { uplinkPeer = next },
  }
  running.add(entry)
  return entry
}

/** A Peer whose scope is a plain Cordis fiber, so `peer.ctx.effect` and `dispose()` behave as the registry's do. */
async function fixturePeer(): Promise<PeerScope> {
  const root = new Context()
  const fiber = root.plugin(() => {})
  await fiber
  return {
    id: 'fixture-peer' as PeerId,
    ctx: fiber.ctx,
    principal: { kind: 'owner' },
    interactions: 'answer',
    signal: new AbortController().signal,
    dispose: async () => { await fiber.dispose() },
  }
}

interface DownlinkOptions {
  /** `accept` header value; defaults to the mux media type. */
  readonly accept?: string
  /** Answer liveness pings; `false` simulates a Peer that stopped answering. */
  readonly answerPings?: boolean
}

interface MuxDownlink {
  readonly status: number
  readonly headers: IncomingHttpHeaders
  /** Session id the downlink was accepted with. */
  readonly session: string | undefined
  /** Logical frames received so far, in order. */
  readonly frames: Record<string, unknown>[]
  /** Liveness frames received so far. */
  readonly pings: () => number
  /** Body text received so far, for a refused downlink. */
  readonly text: () => string
  /** Whether the downlink body has ended. */
  readonly ended: () => boolean
  /** Resolves once the downlink body ends, however it ends. */
  readonly closed: Promise<void>
  /** Post one NDJSON uplink body for this session. */
  post(frames: string | readonly string[]): Promise<UplinkReply>
  /** Drop the downlink, as a Client that goes away does. */
  close(): void
}

/** Open one downlink and read its event stream the way a browser-side mux client does. */
function openMux(entry: RunningMux, options: DownlinkOptions = {}): Promise<MuxDownlink> {
  const answerPings = options.answerPings ?? true
  const request = httpRequest(entry.url, {
    method: 'GET',
    headers: { accept: options.accept ?? REMOTE_STREAM_MUX_MEDIA_TYPE },
  })
  return new Promise<MuxDownlink>((resolve, reject) => {
    request.once('error', reject)
    request.once('response', (response) => {
      request.off('error', reject)
      // A downlink the server ends is reported through `closed`, never as a
      // fixture fault.
      request.once('error', () => { /* the downlink already ended */ })
      const header = response.headers[REMOTE_STREAM_SESSION_HEADER]
      const session = typeof header === 'string' ? header : undefined
      const frames: Record<string, unknown>[] = []
      const ended = Promise.withResolvers<undefined>()
      const decoder = new TextDecoder()
      let pings = 0
      let text = ''
      let cursor = 0
      let closed = false
      const finish = (): void => {
        if (closed) return
        closed = true
        ended.resolve(undefined)
      }
      const deliver = (frame: Record<string, unknown>): void => {
        if (frame.type === 'ping') {
          pings += 1
          // A live Client answers every ping with one uplink pong.
          if (answerPings && session !== undefined) {
            void postUplink(entry, session, '{"type":"pong"}').catch(() => undefined)
          }
          return
        }
        frames.push(frame)
      }
      response.on('data', (chunk: Buffer) => {
        text += decoder.decode(chunk, { stream: true })
        for (;;) {
          const end = text.indexOf('\n\n', cursor)
          if (end < 0) break
          const block = text.slice(cursor, end)
          cursor = end + 2
          for (const line of block.split('\n')) {
            if (line.startsWith('data:')) {
              deliver(JSON.parse(line.slice('data:'.length).trim()) as Record<string, unknown>)
            }
          }
        }
      })
      response.once('end', finish)
      response.once('error', finish)
      response.once('close', finish)
      resolve({
        status: response.statusCode ?? 0,
        headers: response.headers,
        session,
        frames,
        pings: () => pings,
        text: () => text,
        ended: () => closed,
        closed: ended.promise,
        post: body => session === undefined
          ? Promise.reject(new Error('fixture downlink has no session id'))
          : postUplink(entry, session, body),
        close: () => { request.destroy() },
      })
    })
    request.end()
  })
}

/** Resolve `true` when the downlink attempt ends without a response, as a refused bind does. */
function refusedDownlink(entry: RunningMux): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    const request = httpRequest(entry.url, {
      method: 'GET',
      headers: { accept: REMOTE_STREAM_MUX_MEDIA_TYPE },
    })
    request.once('error', () => { resolve(true) })
    request.once('response', (response) => {
      response.resume()
      response.once('error', () => { resolve(false) })
      response.once('end', () => { resolve(false) })
    })
    request.end()
  })
}

interface UplinkReply {
  readonly status: number
  readonly text: string
}

/**
 * POST one uplink body. `chunked` omits the declared length, so the Host must
 * read the body to bound it instead of rejecting from the header.
 */
function postUplink(
  entry: RunningMux,
  session: string,
  frames: string | readonly string[],
  options: { readonly chunked?: boolean } = {},
): Promise<UplinkReply> {
  const body = typeof frames === 'string' ? frames : frames.join('\n')
  const url = new URL(entry.url)
  url.searchParams.set(REMOTE_STREAM_SESSION_PARAM, session)
  return new Promise<UplinkReply>((resolve, reject) => {
    const request = httpRequest(url, {
      method: 'POST',
      headers: {
        'content-type': 'application/x-ndjson',
        ...(options.chunked === true ? {} : { 'content-length': String(Buffer.byteLength(body)) }),
      },
    }, (response) => {
      const chunks: Buffer[] = []
      response.on('data', (chunk: Buffer) => { chunks.push(chunk) })
      response.once('error', reject)
      response.once('end', () => {
        resolve({ status: response.statusCode ?? 0, text: Buffer.concat(chunks).toString('utf8') })
      })
    })
    request.once('error', reject)
    request.end(body)
  })
}

/** The downlink response the fixture server last handed to the mux. */
function downlinkResponse(entry: RunningMux): ServerResponse {
  const response = entry.downlinks[0]
  if (response === undefined) throw new Error('fixture mux has no downlink response')
  return response
}

/** Session id of an accepted downlink; every client-side step needs it. */
function sessionOf(downlink: MuxDownlink): string {
  if (downlink.session === undefined) throw new Error('fixture downlink has no session id')
  return downlink.session
}

/** Intercept the next `setImmediate` so a spec can run one deferred check by hand. */
function captureNextImmediate(): { readonly next: Promise<() => void>; readonly restore: () => void } {
  const scheduled = Promise.withResolvers<() => void>()
  const immediate = globalThis.setImmediate
  const spy = vi.spyOn(globalThis, 'setImmediate').mockImplementation((callback: () => void) => {
    scheduled.resolve(callback)
    return immediate(() => {})
  })
  return { next: scheduled.promise, restore: () => { spy.mockRestore() } }
}

function openFrame(streamId: string, endpoint = 'fixture/follow'): string {
  return JSON.stringify({ type: 'open', streamId, endpoint, payload: {} })
}

function itemFrame(streamId: string, value?: unknown): string {
  return JSON.stringify(value === undefined ? { type: 'item', streamId } : { type: 'item', streamId, value })
}

function endFrame(streamId: string): string {
  return JSON.stringify({ type: 'end', streamId })
}

function cancelFrame(streamId: string): string {
  return JSON.stringify({ type: 'cancel', streamId })
}

async function *echoUplink(uplink: AsyncIterable<unknown>): AsyncIterable<unknown> {
  for await (const value of uplink) yield value
}

/** Yield `value` every few milliseconds until `stop` settles. */
async function *steadyItems(stop: Promise<void>, value = 'item'): AsyncIterable<string> {
  const stopped = stop.then(() => true)
  while (!await Promise.race([stopped, delay(5).then(() => false)])) yield value
}

async function *waitForAbort(signal: AbortSignal): AsyncIterable<never> {
  await new Promise<void>((resolve) => {
    if (signal.aborted) resolve()
    else signal.addEventListener('abort', () => { resolve() }, { once: true })
  })
}

async function *cleanlyCancelled(signal: AbortSignal, returned: () => void): AsyncIterable<never> {
  try {
    await new Promise<void>((resolve) => {
      if (signal.aborted) resolve()
      else signal.addEventListener('abort', () => { resolve() }, { once: true })
    })
  } finally {
    returned()
  }
}

async function *delayedItem(
  released: Promise<void>,
  opened: () => void,
  returned: () => void,
): AsyncIterable<string> {
  try {
    opened()
    await released
    yield 'item'
  } finally {
    returned()
  }
}

function delay(ms: number): Promise<void> {
  return new Promise<void>((resolve) => { setTimeout(resolve, ms) })
}

async function closeHttp(server: Server): Promise<void> {
  if (!server.listening) return
  await new Promise<void>((resolve, reject) => {
    server.close((error) => {
      if (error === undefined) resolve()
      else reject(error)
    })
  })
}
