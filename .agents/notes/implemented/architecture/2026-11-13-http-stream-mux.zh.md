# Agent Note: 通过 HTTP 承载 Remote 流复用

Status: implemented

[English](2026-11-13-http-stream-mux.md) | 中文

## 问题

`dsh-api-gateway` 通过 `/api/remote.mux` 上的一条复用载体承载浏览器连接的全部在线 Remote 流：模型 token 增量、会话事件、工具输出、终端输出以及 `$events` 扇出。WebSocket 载体需要 `Upgrade` 握手，而现实中存在只放行 HTTP 与 HTTPS 的部署目标：托管的开发容器、转发 `GET` 与 `POST` 的 TLS 终止代理、剥掉 `Upgrade` 的 ingress。经过这些目标，浏览器根本开不出 WebSocket，Web GUI 在那里也就没有任何通往 Host 的在线通道。逻辑上的 [Remote 双工流](2026-09-19-remote-duplex-stream.zh.md)不依赖其载体：它需要有序的下行推送、有序的上行、每个浏览器 API 凭据一个已准入的 Peer、存活检测，以及每次载体丢失时的一条失败报告。需要更换的只有物理载体。

## 决定

一个 mux 会话是一条带认证的 `GET /api/remote.mux`（`Accept: text/event-stream`）作为下行，加上 `POST /api/remote.mux?session=<uuid>`（`content-type: application/x-ndjson`）作为上行。Host 以下行响应回以 `content-type: text/event-stream`、`cache-control: no-store`、`x-accel-buffering: no` 与 `x-dsh-mux-session: <uuid>`，之后每帧 Host 数据写成一个 `data: <json>` 事件；Client 把每一帧，或换行拼接的一批帧，POST 到会话 URL。会话值是一个不可猜测、由 Host 生成、出现在打开下行那次请求的响应头里的 id，它用一条路由和一份凭据把两个方向绑定在一起，而不是用第二条路由加第二份凭据。

下行由 SSE 承载，因为它是纯 HTTP 上唯一能在未经改动的代理路径中存活的标准化服务端推送：它就是代理本就会转发的响应体，浏览器与 Host 之间无需为此改动任何配置。上行留在同一条路径上，因为逐帧 `POST` 原样继承了该路由的 cookie 与 Peer 准入，也因为同一条路径让整个会话只有一处准入点。顺序成立的原因是 Client 串行发送：单一 pump 等每次请求完成后才拼接下一批。逻辑帧、错误码与会话日志都不变。

### 下行

Gateway 把 `REMOTE_STREAM_MUX_PATH` 注册为一条 `auth: 'required'` 的精确路由，`GET` 分派给 `handleDownlink`，`POST` 分派给 `handleUplink`，其他方法一律 405 并附 `allow: 'GET, POST'`。`handleDownlink` 对 `Accept` 未包含 `text/event-stream` 的请求在写出响应头之前就以 406 拒绝；用 `peer.ctx.effect(() => () => res.destroy())` 把响应生命期绑到已准入的 Peer 上，并在该作用域已 dispose 时立即销毁响应；在第一帧之前写出会话头，让 Client 在下行打开时就能寻址自己的上行。

`encodeRemoteStreamFrame` 写出 `data: <json>` 再加一个空行。JSON 文本中不会出现裸换行，所以一帧恰好是一行 `data`，拆分或合并 chunk 的代理无法把两帧并成一个事件。

下行写入串在同一个 promise 上，响应缓冲区不接受的那次写入会等到 `drain` 之后才发出下一帧。因此一个慢读者会拖住该会话上的所有流，与过去一条 socket 的情形相同。

### 上行

上行请求体是换行分隔的帧；Host 按 `\n` 拆分、丢弃空行，把每一行交给传输帧解析器或 `connection.receive`。Client 有排队帧时会把它们拼进一次请求，把请求行与请求头开销摊薄到整批。

会话值缺失、未知，或属于另一个 Peer 时，Host 回 404。id 本身不构成任何授权：Gateway 把解析出的 Peer 与会话所属 Peer 比对，所以只泄露 id 而不持有可准入下行的凭据毫无用处。

请求体超过 `REMOTE_STREAM_UPLINK_MAX_BYTES`（16 MiB）时回 413 并保留会话，因为 Client 可以重发更小的一批。被逻辑解码器拒绝的一行回 400 并终止会话：出现该违规之后，Host 的读取方与 Client 的队列对这条流的认识已经不一致，若会话继续，就会喂入对端从未打算发送的帧。

请求不携带序列号。Client 的单一上行 pump 就是排序机制，它等每次 `POST` 完成后才拼接下一批，因此缓慢或被拒绝的一次发送不会被后来的批次越过。

### 会话准入

`@deepseek-ai/dsh-host-authentication-token` 在按 `/api/remote.mux` 匹配的 `webserver/request` 监听器上维持「每个浏览器 API 凭据一个会话」的策略。某个凭据的第一次 `GET` 会登记会话；同一凭据的第二个并发 `GET` 回 409 并附 `connection: close`；Peer 已消失的凭据回 401；响应的 `close` 释放会话、删除 Peer 并等待其 dispose。属主 principal 的浏览器 cookie 与匿名请求交给 Gateway 自己的准入，Gateway 在写出响应头之前就拒绝的响应不会占用凭据。

会话上打开的每条流都携带其下行准入的那个 Peer，上行不再对条目做第二次认证。

### 存活检测

存活检测位于载体内部，形式是传输帧 `{"type":"ping"}` 与 `{"type":"pong"}`，逻辑解码器永远看不到它们：`parseRemoteStreamTransportFrame` 只识别恰好含一个 `type` 键的对象，把其他文本全部交回 `receive()`。

一个 unref 的定时器按 `streamHeartbeatIntervalMs`（默认 2000）向每个会话写 `{"type":"ping"}`。达到 `MAX_MISSED_HEARTBEATS`（2）次未应答 ping 的会话在下一个 tick 被销毁；任何被接受的上行行（包括 `{"type":"pong"}`）都会把计数归零。必须等待 `drain` 的下行写入同样归零，因为持续排空响应的读者就是存活的读者；响应立刻接受的写入则不然，因为本地缓冲区收下了它而 peer 并未读取。

### 客户端载体

`packages/client/connection/src/client/mux.ts` 取代了已删除的 `socket.ts`。它导出 `openHttpStreamMux`、`ConnectionMuxCarrier` 接口以及 `ConnectionMuxRequest`/`ConnectionMuxSession`；`ClientConnection.openStreamMux` 委托给它，地址来自 `streamBaseUrl` 与可选的 `fetch`，`ConnectionHandle.openWebSocket` 与 `ClientTransportHooks.createWebSocket` 钩子已不存在。

载体保留了它所取代的 socket 形态表面——`readyState` 取 `MUX_CONNECTING`/`MUX_OPEN`/`MUX_CLOSED`，加上 `send`、`close`、`addEventListener`——因此其上层的 `RemoteStreamMuxClient` 与载体无关；该 client 现在显式接收一个载体开启函数，进程内载体供应同样的接口而不经 HTTP。`close(code, reason)` 接受这两个参数但都不传输，因为 HTTP 没有它们的位置。

下行打开之前调用 `send` 会抛错。下行回非 2xx 或缺少 `x-dsh-mux-session` 时，载体以带状态码的消息失败。上行 `POST` 被拒绝时载体失败并关闭两个方向，因为丢帧会让 Host 的读取方一直等待一帧 Client 认为已经发出的数据。

`EventStreamDecoder` 按 `\n` 拆分响应体、容忍结尾的 `\r`、忽略注释行、拼接同一事件的多个 `data:` 行，并重组被 chunk 边界切开的帧。

### 压缩与缓冲

Host 对两个方向都不做压缩。web server 的 gzip 中间件跳过 `text/event-stream`（`packages/host/webserver/src/index.ts:314-325`），因为对一条永不结束的响应做压缩层必须逐事件 flush 才能让帧流动，整响应压缩在小帧上毫无收益，在大帧上只是把它们扣住。

下行设置 `cache-control: no-store` 与 `x-accel-buffering: no`，后者是 nginx 系列用来关闭自身缓冲的响应头。无视该头仍然缓冲响应的代理、CDN 或压缩层会把帧与 ping 一起扣住，会话随后因错过心跳而死亡，而连接看上去依旧打开；[升级指南](../../../../docs/upgrade-guide/v0.2.2-rc.3/remote-mux-http/guide.zh.md)把这一项列为运维检查。

## 考虑过的替代方案

**每次轮询发一条 `GET` 的长轮询。** 每次轮询返回截至当时的帧并关闭，Client 立刻再问一次。不予采纳，因为轮询本身成为延迟下限——按 2000 ms 的 ping 节拍，即使空闲也要为每个 Client 每个间隔付出一次请求，而比帧到达更快地轮询只是白费——并且 Host 之后要为每个会话跨轮询保留帧，自带一套保留规则。

**用 `fetch` 请求体承载单次双工请求的上行半边。** 不予采纳，因为边流式发送请求体边读取响应，在所有已发布的浏览器里都需要 HTTP/2 交换，而促成纯 HTTP 载体的部署目标恰恰是把 HTTP/1.1 固定在代理上的那些。重新分帧或缓冲请求体的代理会让上行读不到数据，而下行看起来仍然健康，这比一次被拒绝的交换更糟。

**用 HTTP/2 server push 承载下行。** 不予采纳，因为 push 已从实现过它的浏览器中移除、在其他浏览器中从未实现，它推送的是客户端没有请求的响应，因此无法承载 Client 用来关联自己那些 POST 的按会话下行。

**保留 WebSocket，要求运维开启 upgrade。** 不予采纳，因为约束在部署而不在配置：剥掉 `Upgrade` 的目标无法通过配置让它代理，而每个这样的部署最终仍然需要本载体，于是仓库要为一个协议维护两套传输。

**为上行开第二条路由，例如 `POST /api/remote.uplink`。** 不予采纳，因为第二条路径需要自己的准入接线、自己的 cookie 与 Peer 解析、自己的路由文档，而且两条路径此后可能被独立准入。同一路径上的会话 query 参数保持了一条路由、一处准入，以及唯一一处 Peer 比对。

**逐帧压缩载体。** 这是 WebSocket 时代的答案：`permessage-deflate`，zlib 级别 1，启用 context takeover，只作用于 JSON 文本超过 1,024 字符的帧，因为 `ws` 在 context takeover 开启时忽略自己的 `threshold` 选项（[上游 41ae5631](https://github.com/websockets/ws/commit/41ae5631)）。它为隧道、反向代理或已准入公开 URL 上的会话回放、长工具输出与终端回滚收回了带宽。事件流不给它立足之地：响应体要么整体压缩要么不压缩，而 web server 排除 `text/event-stream`，因为对永不结束的响应加 gzip 层必须逐事件 flush，代价高于小帧省下的字节。它带来的代价一并消失——每条连接一份 deflate 与一份 inflate 上下文、与文件 I/O 共享 libuv 线程池的 zlib 工作，以及本地回环上白付的 CPU。

**由 Client 决定是否压缩帧。** 载体还是 WebSocket 时不予采纳，因为浏览器 `WebSocket` API 不暴露扩展参数，也无法关闭协商，决定只能落在服务端。在本载体上，等价的选择是 web server 的响应压缩，它是作用于每条响应的部署设置，而不是针对某条流。

**给流自己的 `compressionLevel` 与 `compressionThresholdBytes`。** 在 `permessage-deflate` 存在时属于推迟而非否决，理由是没有测量能证明这些旋钮必要，而仓库既禁止把真实可调项藏进常量，也禁止投机性配置。本载体上的流没有自己的压缩可供调节：`compression`、`compressionLevel` 与 `compressionThresholdBytes` 属于 web server，而 `text/event-stream` 被排除在外。

**对所有连接压缩，或只压缩下行。** 当时不予采纳，因为本地回环部署白付 CPU，且扩展作用于两个方向，单向接受无法表达。这两个问题都不再成立：对 `text/event-stream` 的排除按响应类型而不按方向，部署用 web server 的 `compression` 键开启或关闭响应压缩。

## 后果

只放行 HTTP 与 HTTPS 的部署现在能拿到在线的 Web GUI：没有 `Upgrade`，没有 upgrade 超时，也没有为协议切换而写的代理规则。

每批上行都要付出一次请求的代价，含其请求行与请求头；把排队帧拼接起来可以摊薄，但永远不会免费。Client 的 pump 是串行的，所以繁忙的上行——终端按键、`$events` 发送——每批累积一次往返，而因大小或帧格式错误被拒的一次 `POST` 会让整个会话付出代价。

会话 id 是路由令牌而非凭据。它是 `randomUUID()`，出现在一次已认证请求的响应头里，上行仍然要求同一个已准入的 Peer，因此只观察到其中一个方向的攻击者无法把两个方向接起来。

存活检测位于载体内部，因此 `streamHeartbeatIntervalMs` 既决定 ping 节拍也决定活动期限。会停顿的部署——长时间 GC 暂停、慢磁盘、被挂起的标签页——必须调高它；两次未应答即终止会话，而被取代载体的写入存活附加说明随其压缩一并消失。

每条会话仍然只有一个下行写入链与一个上行 pump 作为背压位置：慢读者会拖住该会话的流，而按流的 inbox 仍会在方法不读取时以 `gateway/uplink-overflow` 让该流失败。

压缩能力被放弃，随之放弃的是塑造该决定的帧阈值、zlib 级别与 context takeover。帧以 JSON 文本传输，因此隧道或公开 URL 会完整承载会话回放、长工具输出与终端回滚。WebSocket 载体从未跑过字节与延迟对比——它的 `'deflate'` 默认值依赖 HTTP gzip 的先例——这里也不继承任何结论；报告带宽压力的部署应把事件流作为整体测量，而不是逐帧测量。

会话日志、错误码与快照重放都不受影响：逻辑帧完全相同，所以没有任何录制会话需要重录，重新加载的 transcript 也一致。

被移除与改名的表面是 `websocketCompression`、`ConnectionWebSocket`/`openConnectionWebSocket`、`ClientTransportHooks.createWebSocket` 钩子、`websocketHeartbeatIntervalMs`（现为 `streamHeartbeatIntervalMs`）以及 `ConnectionHandle.openWebSocket`（现为 `openStreamMux`）；[升级指南](../../../../docs/upgrade-guide/v0.2.2-rc.3/remote-mux-http/guide.zh.md)负责迁移步骤。

## 测试

`packages/api/gateway/tests/stream-server.host.spec.ts` 固定下行响应头与会话 id、对非 SSE `Accept` 的 406 拒绝、空闲流上的 ping 节拍、两次错过心跳后的终止、`drain` 计为活动而立即写入不计、未知会话与外来会话的 404、超限的 413 且保留会话，以及终止会话的 400。`packages/client/connection/tests/mux.client.spec.ts` 固定基址解析、事件流订阅、chunk 拆分与注释跳过、按到达顺序发送、打开前的 `send`、下行被拒时的状态码、上行被拒时关闭载体，以及只关闭一次。`packages/api/gateway/tests/operator-connect.host.spec.ts` 固定第二个并发会话的 409 与下行关闭时 Peer 的释放。`apps/web/tests/sidebar-terminal.e2e.ts` 在 Chromium 中通过真实 mux 驱动已发布的侧栏终端，包括一次被杀死后由 Client 重建的会话。
