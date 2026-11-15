/** Default token authentication, HTTP exchanges, and entry URL contribution. */
import type { ServerResponse } from 'node:http'
import { Context } from '@deepseek-ai/cordis'
import z from '@deepseek-ai/schemastery'
import type {} from '@deepseek-ai/dsh-credentials'
import { CONNECT_PATH } from '@deepseek-ai/dsh-client-connection'
import type { ConnectionTrustRequest, PeerScope } from '@deepseek-ai/dsh-client-connection'
import type {} from '@deepseek-ai/dsh-host-webserver'
import { BrowserAuth } from './browser-auth.ts'

/** Browser API mux path, matching the API Gateway's stream mux route. */
const MUX_PATH = '/api/remote.mux'

/** Stable plugin name. */
export const name = 'host-authentication-token'
/** Services owning request admission and the persistent signing key. */
export const inject = ['connection', 'connection.authentication', 'credentials']

/** Default browser and API cookie lifetime. */
export interface Config {
  /** Absolute cookie lifetime in days; default 30. */
  cookieMaxAgeDays: number
}

/** One admitted browser API mux session and the release that revokes its Peer. */
interface MuxSession {
  readonly response: ServerResponse
  readonly release: () => Promise<void>
}

/** Validate cookie lifetime before preparing the token implementation. */
export const Config: z<Config> = z.object({
  cookieMaxAgeDays: z.natural().min(1).default(30),
})

/**
 * Register the token provider and its optional Web transport integration.
 * @param ctx - plugin scope owning the shared Peer and registrations.
 * @param config - validated browser and API cookie lifetime.
 */
export async function apply(ctx: Context, config: Config): Promise<void> {
  const auth = await BrowserAuth.create(ctx[Context.root], ctx.credentials, config.cookieMaxAgeDays)
  const peer = ctx.connection.createPeer({ principal: { kind: 'owner' } })
  const apiPeers = new Map<PeerScope['id'], PeerScope>()
  ctx.connection.authentication.register({
    authenticate(request) {
      const secure = ctx.get('webServer')?.protocol === 'https:'
      if (auth.isAuthenticated(request, secure)) return peer
      const id = auth.authenticateApi(request, secure)
      return id === undefined ? undefined : apiPeers.get(id)
    },
  })
  ctx.inject(['webServer'], (webCtx) => {
    let active = true
    const sessions = new Map<PeerScope['id'], MuxSession>()
    webCtx.effect(() => async () => {
      active = false
      apiPeers.clear()
      for (const session of [...sessions.values()]) session.response.destroy()
      await Promise.all([...sessions.values()].map(session => session.release()))
    }, 'host-authentication-token: API mux sessions')
    webCtx.on('webserver/request', async (request, response, next) => {
      const facts: ConnectionTrustRequest = request
      if (facts.socket?.destroyed || response.destroyed || facts.signal?.aborted) return
      if (!active) { await next(); return }
      const url = new URL(request.url ?? '/', 'http://dsh.invalid')
      if (url.pathname !== '/' || !url.searchParams.has('token')) {
        await next()
        return
      }
      auth.authorizeIndex(request, response, webCtx.webServer.protocol === 'https:')
    })
    webCtx.effect(() => webCtx.webServer.register({
      kind: 'exact', path: CONNECT_PATH, auth: 'anonymous',
      handler(request, response) {
        const facts: ConnectionTrustRequest = request
        if (facts.socket?.destroyed || response.destroyed || facts.signal?.aborted) return
        if (!active) {
          response.writeHead(401)
          response.end('unauthorized')
          return
        }
        auth.authorizeConnect(request, response, webCtx.webServer.protocol === 'https:', (interactions) => {
          const apiPeer = webCtx.connection.createPeer({ principal: { kind: 'owner' }, interactions })
          apiPeers.set(apiPeer.id, apiPeer)
          apiPeer.ctx.effect(() => () => { apiPeers.delete(apiPeer.id) }, 'authentication-token: API credential')
          return apiPeer.id
        })
      },
    }), 'host-authentication-token: API connect')
    // Mirrors the API Gateway's REMOTE_STREAM_MUX_PATH: this plugin owns the
    // one-session-per-Peer policy for browser API credentials on that path.
    webCtx.on('webserver/request', async (request, response, next) => {
      const facts: ConnectionTrustRequest = request
      if (facts.socket?.destroyed || response.destroyed || facts.signal?.aborted) return
      if (!active) { await next(); return }
      const url = new URL(request.url ?? '/', 'http://dsh.invalid')
      if (url.pathname !== MUX_PATH || request.method !== 'GET') { await next(); return }
      const secure = webCtx.webServer.protocol === 'https:'
      const credential = auth.authenticateApi(request, secure)
      // Owner cookies and anonymous requests rely on the Gateway's own admission.
      if (credential === undefined) { await next(); return }
      const apiPeer = apiPeers.get(credential)
      if (apiPeer === undefined) {
        // The credential outlived its Peer, so the Client must reconnect.
        response.writeHead(401, { 'cache-control': 'no-store' })
        response.end('unauthorized')
        return
      }
      if (sessions.has(credential)) {
        response.writeHead(409, { 'cache-control': 'no-store', connection: 'close' })
        response.end('conflict')
        return
      }
      let released: Promise<void> | undefined
      const release = (): Promise<void> => {
        if (released !== undefined) return released
        sessions.delete(credential)
        // A request the Gateway refused never owned this Peer; an unwritten status
        // still reads 200, so admission requires the admitted downlink's own headers.
        if (!response.headersSent || response.statusCode !== 200) {
          released = Promise.resolve()
          return released
        }
        // Revocation precedes asynchronous Peer and session cleanup.
        apiPeers.delete(credential)
        released = apiPeer.dispose().catch((error: unknown) => {
          console.error('[host-authentication-token] Peer release failed:', error)
        })
        return released
      }
      sessions.set(credential, { response, release })
      response.once('close', () => { void release() })
      try {
        await next()
      } catch (error) {
        await release()
        throw error
      }
    })
  })
  ctx.on('webserver/entry-url', (baseUrl, entries) => {
    entries.push({ label: 'dsh web', url: auth.authenticatedUrl(baseUrl), open: true })
  })
}
