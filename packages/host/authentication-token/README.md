---
description: "Sign in through the default launch token and retain browser access through authority-bound Cookies."
kind: "package-reference"
---

# @deepseek-ai/dsh-host-authentication-token

English | [中文](README.zh.md)

## Summary

Open the tokenized launch URL to sign a browser Cookie, then continue accessing the application with that Cookie. API clients exchange the same process token through `/connect` for their independent connection credentials. The plugin preserves the default Cookie format and signing-key record while registering through the common ConnectionAuthentication service.

## Table of Contents

- [Use this package](#use-this-package)
- [Understand the implementation](#understand-the-implementation)
- [Further Exploration](#further-exploration)
- [Model Experience](#model-experience)
- [Known Limitations and Deferred Work](#known-limitations-and-deferred-work)
- [Dev Note](#dev-note)

-----

<a id="use-this-package"></a>
## Use this package

Mount this function plugin beside Connection and Credentials. It also injects `connection.authentication`, which the Connection plugin provides. A WebServer adds HTTP exchange routes; the authentication provider also works in compositions without a Web listener. The plugin contributes a signed `dsh web` entry through `webserver/entry-url`; WebApp owns printing and browser opening. The package's runtime entry exports `name`, `inject`, `Config`, and `apply`.

| Field | Default | Meaning |
|---|---|---|
| `cookieMaxAgeDays` | `30` | Absolute browser and API Cookie lifetime in days |

Configure `printUrl` and `openBrowser` on the WebApp runtime, not this plugin. Authentication remains available without startup publication.

A root request carrying the launch-token query is handled by the original token exchange: successful `GET /` returns 303 with a Cookie and redirects to `./`; invalid credentials return 401. Requests on other paths continue to their route owner. When WebApp collects entries after readiness, this plugin appends its process token to the supplied base URL while preserving its authority, path, and other query fields. The contributed entry is eligible for automatic opening. Token neither reads WebApp services nor owns printing, readiness, or the native browser opener.

Browser v1 Cookies authenticate to one shared Peer owned by this plugin. `/connect` declares anonymous route admission: Connection checks the source, then the handler verifies the launch token, creates an API Peer, and signs its ID in a v2 Cookie. A valid v2 Cookie authenticates pages and API requests through the same Provider. The Peer's principal and RemoteAccess determine permissions; there is no page/API purpose level. Token permits one `/api/remote.mux` mux session per browser API credential: a second concurrent `GET` returns 409 without revoking the first session. Closing that response revokes the credential synchronously, then disposes the Peer; later requests using that Cookie are refused. Other request listeners, including those of other upgrade routes such as Inspector CDP, neither claim the Peer nor revoke it on closure.

The signing secret remains the `client-connection/browser-session` grant record in Credentials. Cookie names, signed audiences, expiry validation, and the listener-controlled `Secure` attribute follow the existing protocol. Reloading retains this root application's launch token and still-valid Cookies while creating a new plugin-owned shared Peer. Token activation failure may leave the application running without this verifier; it does not permit unauthenticated access to protected routes. Desktop can mount this Provider alongside its private-credential Provider.

-----

<a id="understand-the-implementation"></a>
## Understand the implementation

<details>
<summary>Implementation internals — click to expand</summary>

The plugin prepares its signer, creates a shared Peer through Connection, and registers through `ctx.connection.authentication` a verifier returning that Peer for v1 Cookies or resolving a verified v2 ID through its own API Peer map. It uses the same verification for pages and API calls. Its WebServer request listener owns root token exchange, while an anonymous exact route owns `/connect`.

Beside that route, a `webserver/request` listener delegates every request except `GET /api/remote.mux` and recognizes its own API credentials through the `apiPeers` and `sessions` maps: a credential whose Peer is gone returns 401, and a second concurrent session returns 409 with `connection: close`. Shared browser Peers, owner cookies, and other Providers delegate with `next()`. This listener runs after route admission and before the Gateway handler. Response close and handler failure release the session claim; a session the Gateway admitted with 200 also revokes the API credential before asynchronous Peer disposal, while a request the Gateway refused never owned the Peer. Peer disposal also closes its session response. Cleanup is idempotent; the Web integration scope removes its listeners, revokes its API credentials, destroys only its owned API sessions, and awaits session closure and Peer cleanup. Late middleware cannot restore a revoked Peer. A synchronous `webserver/entry-url` listener contributes the signed URL; its event and entry type are declared by WebServer, without a dependency on the WebApp bundle.

The original launch-token lifetime and Cookie algorithms stay in the internal [BrowserAuth implementation](src/browser-auth.ts). The [plugin entry](src/index.ts) supplies the Connection, ConnectionAuthentication, and WebServer integration, and [internal types](src/types.ts) describe the HTTP operations used by the signer.

</details>

-----

<a id="further-exploration"></a>
## Further Exploration

- [Connection](../../client/connection/README.md) — ConnectionAuthentication registration and existing Peer lifecycle.
- [WebServer](../webserver/README.md) — HTTP routes and request events.
- [Credentials](../../credentials/credentials/README.md) — persistent signing-key records.

<a id="model-experience"></a>
## Model Experience

None, as the plugin authenticates HTTP callers and contributes no model input.

#### KV Cache effect

None; authentication does not modify model requests.

## Known Limitations and Deferred Work

<a id="known-limitations-and-deferred-work"></a>

- Browser Cookies share one plugin-owned Peer; ending the plugin scope ends its live connections together.
- Plain HTTP transports bearer Cookies without encryption. Listener TLS determines the Cookie's `Secure` attribute.
- The token middleware owns only the root token exchange. Other HTML or product paths keep their route owner's handling, including any token query on those paths.

<a id="dev-note"></a>
### Dev Note

<details>
<summary>Working context for maintainers — click to expand</summary>

None.

</details>
