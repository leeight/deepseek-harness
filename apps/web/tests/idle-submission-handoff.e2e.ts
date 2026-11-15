/** Keep idle submissions in Chat across independently delivered Inbox and transcript frames. */
import { readFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { chromium, type Page } from 'playwright'
import { expect, it, onTestFinished } from 'vitest'
import type { InboxState } from '@deepseek-ai/dsh-agent/types'
import type { SessionEvent } from '@deepseek-ai/dsh-session/types'
import {
  parseRemoteStreamClientMessage, parseRemoteStreamServerMessage,
} from '@deepseek-ai/dsh-api-gateway/stream-protocol'
import { installMuxProbe, type MuxProbe } from './mux.ts'
import { launchWebScaffold, seedSession, watchConsole, webSnapshotMode } from './scaffold.ts'
import { newEnglishPage } from './support.ts'

const FIXTURE = fileURLToPath(new URL('../../../snapshots/web/live-interactions/session.v3.jsonl', import.meta.url))
const SESSION_ID = 'idle-submission-handoff'
const TEXT = 'IDLE_SUBMISSION_HANDOFF Keep this message in the conversation.'
const HANDOFF_TIMEOUT_MS = 30_000

interface Delivery {
  readonly seq: number
  readonly stream: string
  readonly inbox?: InboxState
  readonly event?: SessionEvent
}

/** Delay real Host frames while preserving FIFO within each logical stream. */
class HistoryDeliveryGate {
  private readonly streams = new Map<string, string>()
  private buffered: Delivery[] = []
  private mux: MuxProbe | undefined
  holding = false

  async install(page: Page): Promise<void> {
    this.mux = await installMuxProbe(page, { gate: (message, _session, seq) => this.decide(message, seq) })
  }

  /**
   * Decide whether one downlink frame reaches the application now.
   *
   * The endpoint comes from the uplink `open` frame of the same stream, so an unknown stream refreshes the map.
   * @param message - Downlink frame text.
   * @param seq - Probe sequence number the gate's release call uses.
   * @returns Whether the frame passes or waits for {@link releaseThrough} or {@link releaseAll}.
   */
  private async decide(message: string, seq: number): Promise<'pass' | 'hold'> {
    const frame = parseRemoteStreamServerMessage(message)
    if (!this.streams.has(frame.streamId) && this.mux !== undefined) {
      for (const text of (await this.mux.snapshot()).uplink) {
        const open = parseRemoteStreamClientMessage(text)
        if (open.type === 'open') this.streams.set(open.streamId, open.endpoint)
      }
    }
    const endpoint = this.streams.get(frame.streamId)
    if (!this.holding || (endpoint !== 'session/control' && endpoint !== 'session/follow')) return 'pass'
    const control = endpoint === 'session/control' && frame.type === 'item'
      ? frame.value as { type: string; sessionId?: string; changes?: readonly { key: string; val: unknown }[] } : undefined
    const inbox = control?.type === 'projection' && control.sessionId === SESSION_ID
      ? control.changes?.find(change => change.key === 'inbox')?.val as InboxState | undefined : undefined
    const follow = endpoint === 'session/follow' && frame.type === 'item'
      ? frame.value as { type: string; event?: SessionEvent } : undefined
    this.buffered.push({ seq, stream: frame.streamId,
      ...follow?.event === undefined ? {} : { event: follow.event }, ...inbox === undefined ? {} : { inbox } })
    return 'hold'
  }

  inboxDelivery(queued: boolean, text = TEXT): Delivery | undefined {
    return this.buffered.find(item => item.inbox !== undefined && item.inbox['next-turn'].some(message =>
      message.content.some(block => block.type === 'text' && block.text === text)) === queued)
  }

  admissionDelivery(text = TEXT): Delivery | undefined {
    return this.buffered.find(item => item.event?.type === 'user/message'
      && item.event.data.content.some(block => block.type === 'text' && block.text === text))
  }

  turnStartDelivery(): Delivery | undefined { return this.buffered.find(item => item.event?.type === 'turn/start') }


  /**
   * Deliver every held frame of one stream up to and including the target.
   * @param target - Last held frame to deliver.
   * @returns Resolution once the probe handed the frames over.
   */
  async releaseThrough(target: Delivery): Promise<void> {
    const index = this.buffered.indexOf(target)
    if (index < 0) throw new Error('delivery is not buffered')
    const released = this.buffered.slice(0, index + 1).filter(item => item.stream === target.stream)
    this.buffered = this.buffered.filter(item => !released.includes(item))
    await this.mux?.release(released.map(item => item.seq))
  }

  /** Stop holding and deliver every remaining frame. */
  async releaseAll(): Promise<void> {
    this.holding = false
    this.buffered = []
    await this.mux?.release()
  }
}

/** Match the scaffold's turn-completion budget for Host frames and browser rendering. */
function poll<T>(read: () => T) {
  return expect.poll(read, { timeout: HANDOFF_TIMEOUT_MS })
}

/** The caller's await reports Turn failure; an earlier assertion may bypass that await. */
function whenTurnSettled(scaffold: Awaited<ReturnType<typeof launchWebScaffold>>) {
  const settled = scaffold.whenTurnSettled(HANDOFF_TIMEOUT_MS)
  void settled.catch(() => undefined)
  return settled
}

async function placement(page: Page, input = TEXT) {
  return page.evaluate((text) => {
    const matching = (selector: string) => [...document.querySelectorAll<HTMLElement>(selector)]
      .filter(element => element.textContent?.includes(text))
    const echo = matching('[data-submission-echo]')
    const dock = matching('[data-queue-dock] li')
    const durable = matching('[data-chat-flow-kind="user"]')
    return {
      echo: echo.length,
      dock: dock.length,
      durable: durable.length,
      messageTop: (echo[0] ?? dock[0] ?? durable[0])?.getBoundingClientRect().top ?? null,
      composerTop: document.querySelector('[data-composer-card]')?.getBoundingClientRect().top ?? null,
    }
  }, input)
}

it.skipIf(webSnapshotMode() === 'record').each(['inbox-first', 'transcript-first'] as const)(
  'keeps an idle submission in Chat under CPU throttling (%s)', async (order) => {
    const scaffold = await launchWebScaffold({ replayFixture: FIXTURE, compareReplaySession: false })
    onTestFinished(() => scaffold.close())
    const gate = new HistoryDeliveryGate()
    try {
      await seedSession(scaffold, await readFile(FIXTURE, 'utf8'), SESSION_ID)
      const browser = await chromium.launch()
      onTestFinished(() => browser.close())
      const page = await newEnglishPage(browser)
      const tripwire = watchConsole(page)
      await gate.install(page)
      await page.goto(scaffold.authenticatedUrl, { waitUntil: 'load' })
      await page.locator('[role="treeitem"]').first().click()
      await page.locator('[role="treeitem"]').nth(1).click()
      await page.locator('[data-chat-flow-kind="user"]').first().waitFor()
      await page.getByRole('button', { name: 'Send message', exact: true }).waitFor()
      const cdp = await page.context().newCDPSession(page)
      await cdp.send('Emulation.setCPUThrottlingRate', { rate: 6 })
      const trace: Array<{ phase: string } & Awaited<ReturnType<typeof placement>>> = []
      gate.holding = true
      const settled = whenTurnSettled(scaffold)
      const input = page.locator('[data-composer-input]').first()
      await input.fill(TEXT)
      await input.press('Enter')
      // The prompt receipt is not a stream frame: the accepted echo shows while the gate holds every Host frame.
      await poll(() => placement(page)).toMatchObject({ echo: 1, dock: 0, durable: 0 })
      trace.push({ phase: 'accepted', ...await placement(page) })

      if (order === 'transcript-first') {
        await poll(() => gate.turnStartDelivery()).toBeDefined()
        await gate.releaseThrough(gate.turnStartDelivery()!)
        await page.locator('[data-chat-flow-kind="turn-process"]').last().waitFor({ state: 'attached' })
        await poll(() => placement(page)).toMatchObject({ echo: 1, dock: 0, durable: 0 })
        trace.push({ phase: 'turn-start', ...await placement(page) })
        await poll(() => gate.admissionDelivery()).toBeDefined()
        await gate.releaseThrough(gate.admissionDelivery()!)
        await poll(() => placement(page)).toMatchObject({ echo: 0, dock: 0, durable: 1 })
        trace.push({ phase: 'durable-before-inbox', ...await placement(page) })
      }

      await poll(() => gate.inboxDelivery(true)).toBeDefined()
      await gate.releaseThrough(gate.inboxDelivery(true)!)
      await poll(() => placement(page)).toMatchObject({ echo: order === 'inbox-first' ? 1 : 0, dock: 0, durable: order === 'transcript-first' ? 1 : 0 })
      trace.push({ phase: 'inbox', ...await placement(page) })

      await poll(() => gate.inboxDelivery(false)).toBeDefined()
      await gate.releaseThrough(gate.inboxDelivery(false)!)
      await poll(() => placement(page)).toMatchObject({ echo: order === 'inbox-first' ? 1 : 0, dock: 0, durable: order === 'transcript-first' ? 1 : 0 })
      trace.push({ phase: 'claim-projection', ...await placement(page) })

      if (order === 'inbox-first') {
        await poll(() => gate.turnStartDelivery()).toBeDefined()
        await gate.releaseThrough(gate.turnStartDelivery()!)
        await page.locator('[data-chat-flow-kind="turn-process"]').last().waitFor({ state: 'attached' })
        await poll(() => placement(page)).toMatchObject({ echo: 1, dock: 0, durable: 0 })
        trace.push({ phase: 'turn-start', ...await placement(page) })
      }

      await gate.releaseAll()
      await poll(() => placement(page)).toMatchObject({ echo: 0, dock: 0, durable: 1 })
      trace.push({ phase: 'durable', ...await placement(page) })
      for (const entry of trace) {
        expect(entry.messageTop, entry.phase).toBe(trace[0]?.messageTop)
        expect(entry.composerTop, entry.phase).toBe(trace[0]?.composerTop)
      }
      await settled
      await cdp.send('Emulation.setCPUThrottlingRate', { rate: 1 })
      await cdp.detach()
      expect(tripwire.pageErrors).toEqual([])
    } finally {
      await gate.releaseAll()
    }
  },
)
