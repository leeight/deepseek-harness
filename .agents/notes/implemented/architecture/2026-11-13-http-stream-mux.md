# Agent Note: Remote stream mux over HTTP

Status: implemented

English | [中文](2026-11-13-http-stream-mux.zh.md)

## Problem

`dsh-api-gateway` carries every live Remote stream of a browser connection over one mux carrier at `/api/remote.mux`: model token deltas, session events, tool output, terminal output, and the `$events` fan-out. A WebSocket carrier needs an `Upgrade` handshake, and deployment targets exist that admit only HTTP and HTTPS: a hosted development container, a TLS-terminating proxy that forwards `GET` and `POST`, an ingress that strips `Upgrade`. Through those, a browser opens no WebSocket at all, so the Web GUI there has no live path to the Host. The logical [Remote duplex stream](2026-09-19-remote-duplex-stream.md) does not depend on its carrier: it needs ordered downlink push, ordered uplink, one admitted Peer per browser API credential, liveness detection, and one failure report per lost carrier. Only the physical carrier had to change.

## Decision

A mux session is an authenticated `GET /api/remote.mux` with `Accept: text/event-stream` for the downlink, plus `POST /api/remote.mux?session=<uuid>` with `content-type: application/x-ndjson` for the uplink. The Host answers the downlink with `content-type: text/event-stream`, `cache-control: no-store`, `x-accel-buffering: no`, and `x-dsh-mux-session: <uuid>`, then writes one `data: <json>` event per Host frame; the Client posts each frame, or a newline-joined batch, to the session URL. The session value is an unguessable Host-generated id returned in the response header of the request that opened the downlink, which binds the two directions with one route and one credential rather than a second route and a second credential.

SSE carries the downlink because it is the only standardized server push over plain HTTP that survives an unmodified proxy path: it is a response body a proxy already forwards, so nothing between the browser and the Host needs reconfiguration. The uplink stays on the same path because a per-frame `POST` inherits that route's cookie and Peer admission untouched and because one path keeps one admission site for the session. Ordering holds because the Client serializes its posts: one pump awaits each request before it splices the next batch. Logical frames, error codes, and the session log are unchanged.

### Downlink

The Gateway registers `REMOTE_STREAM_MUX_PATH` as one exact route with `auth: 'required'`, dispatching `GET` to `handleDownlink`, `POST` to `handleUplink`, and any other method to 405 with `allow: 'GET, POST'`. `handleDownlink` refuses a request whose `Accept` does not include `text/event-stream` with 406 before writing headers, binds the response lifetime to the admitted Peer with `peer.ctx.effect(() => () => res.destroy())`, destroys the response at once when that scope is already disposed, and writes the session header before the first frame so the Client can address its uplink as soon as the downlink opens.

`encodeRemoteStreamFrame` emits `data: <json>` followed by a blank line. A JSON text never contains a raw newline, so one frame is exactly one `data` line and a proxy that splits or coalesces chunks cannot merge two frames into one event.

Downlink writes chain through one promise, and a write the response buffer does not accept waits for `drain` before the next frame goes out. One slow reader therefore delays every stream on that session, the same way one socket did.

### Uplink

An uplink body is newline-delimited frames; the Host splits it on `\n`, drops empty lines, and feeds each line either to the transport frame parser or to `connection.receive`. A Client with queued frames joins them into one request, which amortizes the request line and headers across the batch.

The Host answers 404 when the session value is missing, unknown, or owned by a different Peer. The id alone authorizes nothing: the Gateway compares the resolved Peer with the session's, so a leaked id is useless without the credential that admits the downlink.

A body above `REMOTE_STREAM_UPLINK_MAX_BYTES` (16 MiB) answers 413 and keeps the session, because the Client can retry a smaller batch. A line the logical decoders reject answers 400 and terminates the session: after that violation the Host's readers and the Client's queue disagree about the stream, and a session that continued would feed frames the peer never meant to send.

The request carries no sequence number. The Client's single uplink pump is the ordering mechanism, and it awaits each `POST` before splicing the next batch, so a slow or refused post cannot be overtaken.

### Session admission

`@deepseek-ai/dsh-host-authentication-token` keeps the one-session-per-browser-API-credential policy on a `webserver/request` listener keyed by `/api/remote.mux`. The first `GET` for a credential records the session; a second concurrent `GET` for the same credential answers 409 with `connection: close`; a credential whose Peer is gone answers 401; the response `close` releases the session, deletes the Peer, and awaits its disposal. Owner-principal browser cookies and anonymous requests pass to the Gateway's own admission, and a response the Gateway refused before writing headers does not claim the credential.

Every stream opened on the session carries the Peer admitted for its downlink, and the uplink never authenticates items again.

### Liveness

Liveness lives inside the carrier as the transport frames `{"type":"ping"}` and `{"type":"pong"}`, which the logical decoders never see: `parseRemoteStreamTransportFrame` recognizes exactly an object with one `type` key and returns every other text to `receive()`.

One unref'd interval writes `{"type":"ping"}` to every session at `streamHeartbeatIntervalMs` (default 2000). A session that reaches `MAX_MISSED_HEARTBEATS` (2) unanswered pings is destroyed on the next tick, and any accepted uplink line, `{"type":"pong"}` included, resets the count. A downlink write that had to wait for `drain` resets it too, because a reader that keeps draining is a live reader; a write the response accepted at once does not, because the local buffer took it without the peer reading.

### Client carrier

`packages/client/connection/src/client/mux.ts` replaces the deleted `socket.ts`. It exports `openHttpStreamMux`, the `ConnectionMuxCarrier` interface, and `ConnectionMuxRequest`/`ConnectionMuxSession`; `ClientConnection.openStreamMux` delegates to it, `streamBaseUrl` and an optional `fetch` supply the address, and `ConnectionHandle.openWebSocket` with the `ClientTransportHooks.createWebSocket` hook is gone.

The carrier keeps the socket-shaped surface it replaced — `readyState` as `MUX_CONNECTING`/`MUX_OPEN`/`MUX_CLOSED`, `send`, `close`, `addEventListener` — so `RemoteStreamMuxClient` above it stays carrier-agnostic; that client now takes an explicit carrier opener, and the in-process carrier supplies the same interface without HTTP. `close(code, reason)` accepts both arguments and transmits neither, because HTTP has no place for them.

`send` before the downlink opens throws. A downlink that answers non-2xx, or omits `x-dsh-mux-session`, fails the carrier with the status in the message. A rejected uplink `POST` fails the carrier and closes both directions, because a dropped frame would leave the Host's readers waiting for a frame the Client believes it sent.

`EventStreamDecoder` splits the body on `\n`, tolerates a trailing `\r`, ignores a comment line, joins the `data:` lines of one event, and reassembles a frame split across chunks.

### Compression and buffering

The Host compresses neither direction. The web server's gzip middleware skips `text/event-stream` (`packages/host/webserver/src/index.ts:314-325`), because a compression layer over a response that never ends must flush once per event to keep frames moving, so whole-response compression saves nothing on small frames and holds large ones back.

The downlink sets `cache-control: no-store` and `x-accel-buffering: no`, the header the nginx family honors to disable its own buffering. A proxy, CDN, or compression layer that buffers the response regardless holds frames and pings together, and the session then dies of missed heartbeats while the connection still reads as open; [the upgrade guide](../../../../docs/upgrade-guide/v0.2.2-rc.3/remote-mux-http/guide.md) makes that the operator's check.

## Alternatives considered

**Long-polling with one `GET` per poll.** Each poll would return the frames accumulated so far and close, and the Client would immediately ask again. Rejected because the poll itself becomes the latency floor — a 2000 ms ping cadence costs one request per Client per interval even when idle, and polling faster than frames arrive wastes the difference — and because the Host would then hold frames across polls for every session under its own retention rule.

**A `fetch` request body as the uplink half of one duplex request.** Rejected because streaming a request body while reading the response needs an HTTP/2 exchange in every shipping browser, and the targets that motivate a plain-HTTP carrier are exactly the ones that pin HTTP/1.1 through a proxy. A proxy that re-frames or buffers a request body leaves the uplink unread while the downlink reads as healthy, which is worse than a refused exchange.

**HTTP/2 server push for the downlink.** Rejected because push is removed from browsers that shipped it, was never implemented in others, sends a response the client did not request, and therefore cannot carry the per-session downlink the Client relates its posts to.

**Keep the WebSocket and require operators to enable upgrades.** Rejected because the constraint is the deployment, not a setting: a target that strips `Upgrade` cannot be configured into proxying it, and every such deployment would need this carrier anyway, so the repository would maintain two transports for one protocol.

**A second route for the uplink, such as `POST /api/remote.uplink`.** Rejected because a second path needs its own admission wiring, its own cookie and Peer resolution, and its own route documentation, and the two paths could then be admitted independently. The session query parameter on the same path keeps one route, one admission, and one site where the Peer comparison happens.

**Compress the carrier frame by frame.** This was the WebSocket answer: `permessage-deflate` at zlib level 1 with context takeover, applied only to frames above 1,024 characters of JSON, because `ws` ignored its `threshold` option while context takeover was enabled ([upstream 41ae5631](https://github.com/websockets/ws/commit/41ae5631)). It reclaimed the bandwidth of session replay, long tool output, and terminal scrollback across a tunnel, a reverse proxy, or an admitted public URL. An event stream gives it no ground: the response body is compressed as a whole or not at all, and the web server excludes `text/event-stream` because a gzip layer over a never-ending response must flush per event, which costs more than small frames save. The price it carried goes with it — one deflate and one inflate context per connection, zlib work on the libuv thread pool shared with file I/O, and CPU spent for no bandwidth gain over loopback.

**Let the Client decide whether frames are compressed.** Rejected while the carrier was a WebSocket because the browser `WebSocket` API exposed no extension parameters and no way to disable negotiation, so the choice could only live on the server. On this carrier the equivalent choice is the web server's response compression, which is a deployment setting applying to every response rather than to one stream.

**Give the stream its own `compressionLevel` and `compressionThresholdBytes`.** Deferred rather than rejected while `permessage-deflate` existed, on the ground that no measurement justified the knobs and the repository forbids both a real tunable hidden in a constant and speculative configuration. A stream on this carrier has no compression of its own to tune: `compression`, `compressionLevel`, and `compressionThresholdBytes` belong to the web server, and `text/event-stream` is excluded from them.

**Compress every connection, or only the downlink.** Rejected then because a loopback deployment paid CPU for no gain and the extension applied to both directions, so a one-way accept was not expressible. Neither question survives: the exclusion of `text/event-stream` is per response type rather than per direction, and a deployment turns response compression on or off with the web server's `compression` key.

## Consequences

A deployment that admits only HTTP and HTTPS now gets the live Web GUI: no `Upgrade`, no upgrade timeout, and no proxy rule for a protocol switch.

Each uplink batch costs one request, with its request line and headers, amortized by joining queued frames but never free. The Client's pump is serial, so a busy uplink — terminal keystrokes, `$events` posts — accumulates one round trip per batch, and a `POST` refused for size or a malformed frame costs the whole session.

The session id is a routing token, not a credential. It is `randomUUID()`, it appears in a response header of an authenticated request, and the uplink still requires the same admitted Peer, so the two directions cannot be joined by an attacker who observes only one of them.

Liveness sits inside the carrier, so `streamHeartbeatIntervalMs` both paces the pings and sets the activity deadline. A deployment that stalls — a long GC pause, a slow disk, a suspended tab — must raise it; two missed answers end the session, and the replaced carrier's write-liveness caveat is gone with its compression.

One downlink write chain and one uplink pump per session remain the only backpressure sites: a slow reader holds the session's streams, and the per-stream inbox still fails a stream with `gateway/uplink-overflow` when a method does not read.

The compression capability is given up, and with it the frame threshold, zlib level, and context takeover that shaped its decision. Frames travel as JSON text, so a tunnel or a public URL carries session replay, long tool output, and terminal scrollback in full. No byte-and-latency comparison was ever run for the WebSocket carrier — its `'deflate'` default rested on the HTTP gzip precedent — and none is inherited here; a deployment reporting bandwidth pressure should measure the event stream as a whole rather than per frame.

Session logs, error codes, and snapshot replay are untouched: the logical frames are identical, so no recorded session needed re-recording and a reloaded transcript is the same.

The removed and renamed surfaces are `websocketCompression`, `ConnectionWebSocket`/`openConnectionWebSocket`, the `ClientTransportHooks.createWebSocket` hook, `websocketHeartbeatIntervalMs` (now `streamHeartbeatIntervalMs`), and `ConnectionHandle.openWebSocket` (now `openStreamMux`); [the upgrade guide](../../../../docs/upgrade-guide/v0.2.2-rc.3/remote-mux-http/guide.md) owns the migration steps.

## Testing

`packages/api/gateway/tests/stream-server.host.spec.ts` pins the downlink headers and session id, the 406 refusal for a non-SSE `Accept`, ping cadence on an idle stream, termination after two missed heartbeats, `drain` as activity against an immediate write as none, the 404s for an unknown and a foreign session, 413 above the limit with the session kept, and the 400 that ends a session. `packages/client/connection/tests/mux.client.spec.ts` pins base resolution, the event-stream subscription, chunk splitting with comment skipping, arrival-order posting, `send` before open, a refused downlink status, a refused uplink closing the carrier, and a single close. `packages/api/gateway/tests/operator-connect.host.spec.ts` pins the 409 for a second concurrent session and the Peer release when a downlink closes. `apps/web/tests/sidebar-terminal.e2e.ts` drives the shipped sidebar terminal over the real mux in Chromium, including a killed session that the Client re-establishes.
