---
kind: upgrade-guide
description: Remote streams use an HTTP/SSE mux session at /api/remote.mux instead of a WebSocket.
---

# Remote stream mux moves to HTTP

English | [中文](guide.zh.md)

## Change

The Gateway multiplexes Remote streams over plain HTTP at `/api/remote.mux`. One mux session is an authenticated `GET` with `Accept: text/event-stream`; the Host answers `content-type: text/event-stream`, `cache-control: no-store`, `x-accel-buffering: no`, and `x-dsh-mux-session: <uuid>`, and writes every Host frame as one `data: <json>` event. The Client posts each frame, or a newline-joined batch, to `POST /api/remote.mux?session=<uuid>` with `content-type: application/x-ndjson`; the Host answers 204, 404 for an unknown session, 400 plus session termination for a protocol violation, and 413 above 16 MiB. Logical stream frames are unchanged.

`websocketHeartbeatIntervalMs` becomes `streamHeartbeatIntervalMs`, heartbeat pings travel as `{"type":"ping"}` events answered by `{"type":"pong"}` posts, and `websocketCompression` is removed because an event stream is never gzipped. `ConnectionHandle.openWebSocket()` becomes `openStreamMux()`, the `ConnectionWebSocket`/`createWebSocket` transport hook becomes `ConnectionMuxCarrier` with `openHttpStreamMux()`, `packages/client/connection/src/client/socket.ts` is deleted, and `@deepseek-ai/dsh-host-authentication-token` enforces one session per browser API credential in a `webserver/request` hook.

## Migration

1. Rename `websocketHeartbeatIntervalMs` to `streamHeartbeatIntervalMs` in every cordis.yml, overlay, and TestHost option; the old key is not read.
2. Delete `websocketCompression`. Nothing replaces it.
3. Replace `connection.openWebSocket(path)` with `connection.openStreamMux({ path })`; it returns a `ConnectionMuxCarrier` whose `send()` posts uplink frames and whose `close()` ends the downlink. In-process carriers keep using `connection.rpc.open`.
4. Replace `ClientTransportHooks.createWebSocket` with `streamBaseUrl` and an optional `fetch`; the default carrier is `openHttpStreamMux`. Delete `ConnectionWebSocket` imports and the removed `socket.ts` module.
5. Serve `text/event-stream` unbuffered. The Host sends `x-accel-buffering: no`, which the nginx family honors; a proxy, CDN, or compression layer that buffers the response holds frames and pings together, so streams stall while the connection still reads as open. A proxy rule that forwarded `Upgrade: websocket` for `/api/remote.mux` now serves a plain `GET`.
6. Keep `/api/remote.mux` authenticated like every other `/api` route: a non-SSE `Accept` returns 406, a missing or expired credential returns 401, and a second concurrent session for one browser API credential returns 409, so retry after the owning tab closes.
7. Verify a stream, an uplink, and an idle session across two heartbeat intervals; the Host terminates a session that misses two consecutive answers.
