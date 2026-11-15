/** Real carrier loss without replacing Client plugins or navigating the page. */
import { fileURLToPath } from 'node:url'
import { readFile } from 'node:fs/promises'
import { chromium } from 'playwright'
import { expect, it, onTestFailed, onTestFinished } from 'vitest'
import { launchWebScaffold, seedSession, watchConsole } from './scaffold.ts'
import { countMuxStreams, installMuxProbe, type MuxStreamCounts } from './mux.ts'
import { newEnglishPage, saveFailureShot, writeComposerDraft } from './support.ts'

it.each([false, true])('retains the mounted application across mux recovery with an active Session: %s', async (activeSession) => {
  const scaffold = await launchWebScaffold({
    extraOverlayPath: fileURLToPath(new URL('./pin-browse-picker.overlay.yml', import.meta.url)),
  })
  onTestFinished(() => scaffold.close())
  const title = 'Connection recovery session'
  const draft = 'Unsent draft retained across connection recovery'
  if (activeSession) {
    const workspace = await scaffold.ctx.workspaceRegistry.create(scaffold.workspaceCwd)
    const seed = fileURLToPath(new URL('../../../snapshots/web/seeded-history/session.v3.jsonl', import.meta.url))
    const sessionId = await seedSession(scaffold, await readFile(seed, 'utf8'), 'connection-recovery-session')
    await workspace.attachSession(sessionId)
    await scaffold.ctx.sessionController.rename({ sessionId, title })
  }
  const browser = await chromium.launch()
  onTestFinished(() => browser.close())
  const page = await newEnglishPage(browser)
  const console = watchConsole(page)
  onTestFailed(() => saveFailureShot(page, 'web-e2e-connection-recovery'))
  const mux = await installMuxProbe(page)
  const streams = async (): Promise<MuxStreamCounts> => countMuxStreams(await mux.snapshot())
  await page.goto(scaffold.authenticatedUrl, { waitUntil: 'load' })
  const composer = page.locator('[data-composer-input][contenteditable="true"]')
  if (activeSession) {
    await page.getByRole('treeitem').filter({ has: page.getByText(title, { exact: true }) }).click({ timeout: 20_000 })
    await writeComposerDraft(page, composer, draft)
    await expect.poll(async () => (await streams()).sessionBaselines).toBeGreaterThan(0)
  } else {
    await page.getByText('Into the Unknown', { exact: true }).waitFor({ timeout: 20_000 })
  }
  await expect.poll(async () => (await streams()).ready).toBeGreaterThan(0)
  const root = await page.locator('[data-slot="root"]').elementHandle()
  expect(root).not.toBeNull()
  let navigations = 0
  page.on('framenavigated', () => { navigations++ })

  for (let attempt = 0; attempt < 3; attempt++) {
    const previous = await streams()
    await mux.kill()
    await expect.poll(async () => (await streams()).ready, { timeout: 20_000 }).toBeGreaterThan(previous.ready)
    if (activeSession) {
      await expect.poll(async () => (await streams()).sessionBaselines, { timeout: 20_000 })
        .toBeGreaterThan(previous.sessionBaselines)
    }
    await page.evaluate(() => new Promise<void>((resolve) => {
      requestAnimationFrame(() => { requestAnimationFrame(() => { resolve() }) })
    }))
    expect(await root!.evaluate(element => element.isConnected)).toBe(true)
    expect(console.pageErrors).toEqual([])
    if (activeSession) expect(await composer.textContent()).toBe(draft)
  }

  expect(navigations).toBe(0)
  if (!activeSession) expect(await page.getByText('Into the Unknown', { exact: true }).isVisible()).toBe(true)
})
