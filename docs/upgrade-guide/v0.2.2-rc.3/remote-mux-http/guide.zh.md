---
kind: upgrade-guide
description: Remote 流在 /api/remote.mux 上使用 HTTP/SSE mux 会话，而不再使用 WebSocket。
---

# Remote 流 mux 迁至 HTTP

[English](guide.md) | 中文

## 变更

Gateway 在 `/api/remote.mux` 上以普通 HTTP 多路复用 Remote 流。一个 mux 会话就是一次带认证的 `GET`，并携带 `Accept: text/event-stream`；Host 应答 `content-type: text/event-stream`、`cache-control: no-store`、`x-accel-buffering: no` 与 `x-dsh-mux-session: <uuid>`，并把每个 Host 帧写成一个 `data: <json>` 事件。Client 把每个帧，或以换行连接的批，以 `content-type: application/x-ndjson` 发送到 `POST /api/remote.mux?session=<uuid>`；Host 应答 204，未知会话应答 404，协议违规应答 400 并终止会话，超过 16 MiB 应答 413。逻辑流帧不变。

`websocketHeartbeatIntervalMs` 改为 `streamHeartbeatIntervalMs`，存活性 ping 以 `{"type":"ping"}` 事件发出、由 `{"type":"pong"}` POST 应答，`websocketCompression` 被移除，因为事件流从不经 gzip 压缩。`ConnectionHandle.openWebSocket()` 改为 `openStreamMux()`，`ConnectionWebSocket`/`createWebSocket` 传输 hook 改为 `ConnectionMuxCarrier` 与 `openHttpStreamMux()`，`packages/client/connection/src/client/socket.ts` 被删除，`@deepseek-ai/dsh-host-authentication-token` 改在 `webserver/request` hook 中执行「每个浏览器 API 凭据一个会话」的策略。

## 迁移

1. 在每个 cordis.yml、overlay 与 TestHost 选项中把 `websocketHeartbeatIntervalMs` 改名为 `streamHeartbeatIntervalMs`；旧键不再被读取。
2. 删除 `websocketCompression`，没有替代项。
3. 用 `connection.openStreamMux({ path })` 取代 `connection.openWebSocket(path)`；它返回 `ConnectionMuxCarrier`，其 `send()` 发送上行帧、`close()` 结束下行。进程内载体继续使用 `connection.rpc.open`。
4. 用 `streamBaseUrl` 与可选的 `fetch` 取代 `ClientTransportHooks.createWebSocket`；默认载体是 `openHttpStreamMux`。删除 `ConnectionWebSocket` 导入与被移除的 `socket.ts` 模块。
5. 不要缓冲 `text/event-stream`。Host 发送 nginx 系会遵循的 `x-accel-buffering: no`；会缓冲该响应的代理、CDN 或压缩层会把帧与 ping 一起压住，于是连接看上去仍然正常，流却停滞。为 `/api/remote.mux` 转发 `Upgrade: websocket` 的代理规则现在服务普通 `GET`。
6. 让 `/api/remote.mux` 像其他 `/api` 路由一样保持认证：非 SSE 的 `Accept` 返回 406，缺失或过期的凭据返回 401，同一浏览器 API 凭据的第二个并发会话返回 409，因此应等占用会话的标签页关闭后重试。
7. 跨两个心跳间隔验证一条流、一次上行与一个空闲会话；连续两次未应答的会话会被 Host 终止。
