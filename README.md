# leanwire

[![CI](https://github.com/segdumping/leanwire/actions/workflows/ci.yml/badge.svg)](https://github.com/segdumping/leanwire/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/segdumping/leanwire.svg)](https://pkg.go.dev/github.com/segdumping/leanwire)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

通用网络通信框架（Go）。

内核只有两个包：`codec` 定义线上消息与编解码，`server` 负责连接管理、每帧透传根回调（执行契约：读协程内联、同连接保序、panic 隔离）与干净停止——内核不持有路由表，不关心消息怎么处理。传输接口就是标准库的 `net.Conn` / `net.Listener`——TCP/TLS 零适配；WebSocket、QUIC、gRPC 通过类型适配接入。`ext/router` 把 msgID 路由表挂成根回调（原"Handle 注册"的默认形态），服务层 `app` 把内核与 `ext/` 插件组合成单进程服务骨架。

## 特性

- **极简内核**：2 个核心包、1 个自定义接口（`codec.Codec`），入站只有一个根回调签名
- **传输接口 = 标准库**：换协议只需换 `Serve` / `Connect` 的参数，业务代码不变
- **业务回调串行化**：入站回调提交到 app worker，同一 key 严格保序，可通过 `WithWorkers` 增加并行度
- **投递语义**：`Send(nil, m)` 用连接 flush 超时兜底；`Send(ctx, m)` 按调用方时长等待；`TrySend(m)` 非阻塞、队列满即丢
- **外部驱动的生命周期**：`Start` 非阻塞，只标记启动；`Shutdown(ctx)` 由外部触发，关闭 listener/连接、限时排空并等待读写协程退出
- **进程内唯一连接 ID**：原子自增计数器，用于 map key 与日志，不保证跨重启唯一
- **可删除的插件**：内核对 `ext/` 零依赖，由自动化测试机检（`codec` 的 `TestKernelPurity`、`server` 的 `TestKernelContract`）

## 安装

要求 Go ≥ 1.26：

```
go get github.com/segdumping/leanwire
```

## 快速开始

服务端（内核直接用；路由表来自 `ext/router`）：

```go
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"time"

	"github.com/segdumping/leanwire/codec"
	"github.com/segdumping/leanwire/ext/router"
	"github.com/segdumping/leanwire/server"
)

func main() {
	routeTable := router.New()
	appServer := server.New(
		server.WithRootHandler(routeTable.Handler()),
		server.WithIdleTimeout(5*time.Minute),
	)
	routeTable.Attach(appServer)

	// msgID 1：回显（Send 阻塞至入队，ctx 到期返回）
	routeTable.Handle(1, func(s *server.Conn, m codec.Msg) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.Send(ctx, codec.Msg{ID: m.ID, Body: m.Body})
	})

	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal(err)
	}
	if err := appServer.Serve(listener); err != nil {
		log.Fatal(err)
	}
	if err := appServer.Start(); err != nil { // 非阻塞：只标记启动
		log.Fatal(err)
	}

	// 停止由外部触发；Shutdown 完成时返回
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := appServer.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
```

内核侧的等价接口：`server.New(server.WithRootHandler(fn), ...)`——不想用 msgID 表时，路由、鉴权
过滤等全部由这一个回调自己做。根回调只能在 `New` 时设置一次；连接创建时会
快照根回调，运行中不能替换。

客户端（同一套 `server.Server` 语义，用 `Connect` 接入已拨号的连接）：

```go
conn, _ := net.Dial("tcp", "127.0.0.1:9000")

routeTable := router.New()
client := server.New(server.WithRootHandler(routeTable.Handler()))
routeTable.Attach(client)
routeTable.Handle(1, func(s *server.Conn, m codec.Msg) {
	log.Printf("echo: %s", m.Body)
})
session, err := client.Connect(conn)
if err != nil {
	log.Fatal(err)
}
// 阻塞至入队（nil ctx 用 flush 超时兜底）；非阻塞用 TrySend，自定义时长传 ctx
_ = session.Send(nil, codec.Msg{ID: 1, Body: []byte("hello")})
```

切换到 TLS 只需把 `net.Listen` 换成 `tls.NewListener`，把 `net.Dial` 换成 `tls.Dial`，业务零改动。

说明：

- `server.Start` 非阻塞，只标记启动；`Shutdown` 才等待生命周期结束，须由外部调用
- `server.WithIdleTimeout(0)` 关闭空闲超时；未配置时默认 10 分钟
- `Serve` 传入的 listener、`Connect` 传入的连接由 Server 接管，`Shutdown` 时统一关闭
- 帧格式为 `| len:u32 | id:u32 | body[len] |`（大端），头固定 8 字节；两端 `MaxBody` 须一致
- `msgID` 为 `uint32`，其中 `0xF000–0xFFFF` 是框架保留段（可用 `codec.IsReserved` 判断），业务消息请避开
- handler 重复注册默认只记 Warn 并覆盖；要求唯一性可用 `ext/rpc.Channel` / `app` 的 `MustHandle`，或 `router.MustHandle`（重复即 panic）。装配契约：`app.Handle`/`Serve`/生命周期钩子须在 `Run` 前调用（之后 panic）；`rpc.Channel.Handle`（含 `app.RPC().Handle`）支持运行期更新（注册有锁，同号覆盖并记 Warn）
- 未注册 msgID 由 `router` 缺省丢弃并记 Debug；完全未设置根回调时内核记 Warn 并丢弃。两者都不断开连接；接入外部服务端可用 `router.WithUnknown` 自定义处置（例如 `s.Close()`）
- 发送超过 `MaxBody` 由写协程检测为 `codec.ErrTooLarge`，按写错误关闭连接（OnClose 收到该原因）

## 服务层（`app/`）

把内核、目录、出站连接、请求/响应和生命周期组合成服务骨架：

```go
reg, _ := etcd.New([]string{"10.0.0.1:2379"}) // 生产用 etcd；开发用 registry.NewMemory()
a := app.New("game1",
	app.WithRegistry(reg),
	app.WithServiceName("game"),       // 目录注册名：同服务多实例共用
	app.WithAdvertise("10.0.0.1:9001"),
)

a.Handle(1, func(s app.Session, body []byte) { ... })  // 单向消息：消息号 + 二进制 body
a.RPC().Handle(2, func(_ *server.Conn, _ uint32, body []byte, reply rpc.Reply) {
	_ = reply(resp, nil)                             // 成功回包
	// _ = reply(nil, &rpc.RemoteError{Code: code, Msg: "msg"}) // 错误回包
})

a.AppendStarting(func(a *app.App) { ... })  // 任何入站消息之前执行
a.AppendStopping(func(a *app.App) { ... })  // 停止开始：worker 已排空，连接仍开放，可收尾 Send
a.AppendStopped(func(a *app.App) { ... })  // 内核关闭之后，逆序执行

service := a.Depend("rank")                                          // 出站依赖：一条持久连接
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()
resp, err := service.Call(ctx, 2, body)                              // 同步请求，必须带超时
err = service.CallAsync(ctx, 3, body, func(resp []byte, err error) { // 异步请求，回调处理
})                                                               // 发送失败走返回值
err = service.Send(ctx, 1, body)                                     // 单向，队列满等至 ctx

a.Listen(":9001")  // 装配顺序自由；起听在 AppendStarting 之后
a.Run()            // 阻塞至 SIGINT/SIGTERM 或 Exit()
```

### Session 层（`app.Session`）

Session 直接放在 `app` 包。注册与发送都只使用“消息号 + 二进制 body”，框架不解释 body 格式，业务自行编解码：

```go
func OnNetMsgReqTableReady(sender app.Session, body []byte) {
	u := userMgr.FindUser(sender.ID())
	if u == nil {
		return
	}
	_ = sender.SendBytes(context.Background(), 1030, respBody)
}

a.Handle(1030, OnNetMsgReqTableReady)
```

- `Session.ID()` 缺省为 `server.Conn.ID()`；需要 conn→uid 等映射时用 `app.WithSessionID`。
- `a.Handle(msgID, func(s app.Session, body []byte))`：注册单向消息，handler 不接触 `codec.Msg` / `server.HandlerFunc`。
- `SendBytes(ctx, msgID, body)` 阻塞入队（nil ctx 用 flush 兜底）；非阻塞用 `TrySendBytes(msgID, body)`。
- `Session.Conn()` 返回底层 `*server.Conn`，可直接用 `Done`/`Close`/`RemoteAddr` 等当前框架能力。
- handler 提交到 app worker 执行，同一 key 串行；body 不做框架层解码，非法格式由业务忽略或回错。
- 发送只作用于当前 inbound 连接；跨节点定向仍用 `app.Client`/`Depend`。

### Division 节点与持久连接

节点身份使用 `app.server.id` 形式的 division，例如 `example.gate.1`。每个节点
以 division 作为目录注册名，其他节点按 division 建一条持久连接：

```go
// 本节点：目录名就是 division
a := app.New("gate1",
    app.WithServiceName("example.gate.1"),
    app.WithRegistry(reg),
)

// 到各个对端节点的持久连接（不是"选一个 game 实例"）
game2 := a.Depend("example.game.2")
lobby3 := a.Depend("example.lobby.3")

// 静态地址
game1 := a.Depend("example.game.1", app.WithAddress("10.0.0.11:9001"))
```

规则：

- `Depend(name)` 用 `registry.Resolver` 订阅 name，每次重拨读取最新 endpoint；持久连接由 `ext/peer` 维持。
- division 通常只有一个 endpoint，不需要 `lb.Picker`。
- `ext/lb` 只提供 Picker（RoundRobin/Random/Weighted），不 Watch、不拨号、不维护连接。
- `app.WithPicker`（作为 `Depend` 的 `ClientOption`）只影响**重拨时**选择 endpoint；不是每次调用轮转。调用级 LB 由业务维护多个 Client 并用 `lb.Picker` 选择。
- `Depend("example.game")` 是到一个服务名的单条连接，可能选任意实例；全互联必须按 division 建立连接。
- 节点下线/上线如果要动态增删连接，需要业务 Watch 目录并维护 division→`*Client` 表；静态拓扑直接声明即可。

客户端 uid 关联也已收进 `app`（原 `ext/gate`）。`Gate` 同时持有 App、客户端连接与路由表，业务侧只存一个 `*app.Gate`：

```go
gateRouter := router.New(router.WithLogger(a.Log()))
clientSrv := server.New(
	server.WithCodec(clientCodec),
	server.WithLogger(a.Log()),
	server.WithRootHandler(gateRouter.Handler()),
)
g := app.NewGate(a, clientSrv, gateRouter)
_ = g.Bind(s, uid) // s 为客户端消息里的 app.Session；uid → Session，同 uid 自动顶号
_ = g.SendTo(ctx, uid, msgID, payload)
// 广播由业务自理：连接集合（AOI 域/公会/场景）是业务概念，业务在自己的
// worker（按连接 ID 分片）上遍历成员，用 Session.TrySendBytes 逐个投递
// （队列满即丢——广播尽力而为，必达用 SendTo/SendBytes）；全量遍历
// 可用 g.Server().Range。

// 客户端连接消息（消息号 + 二进制；Bind 前 Session.ID 为 conn.ID，Bind 后为 uid）
g.HandleClient(1, func(s app.Session, body []byte) { ... })

// 内部服务连接消息（委托 App.Handle）
g.HandleServer(200, func(s app.Session, body []byte) { ... })
```

客户端 Server 的停机也交给 App：`NewGate` 把它登记进停机清单，`Run` 退出时与内部 Server 共享预算并发 `Shutdown`，业务无需再自行关闭。

执行模型：

- `Handle` 与 `RPC` 请求回调统一提交到 app worker：同一 key 串行，不同 key 可并行；key 取连接 ID（`server.Conn.ID()`，`Bind` 改写会话 ID 也不影响单连接保序），worker 数由 `WithWorkers` 配置。
- 回调里同步 `Call` 会占用当前 worker 直到响应或 ctx 到期（响应由读协程内联交付，不会自锁）；慢调用应异步派发或改用 `CallAsync`。
- `CallAsync` 的回调同样提交到 app worker，不得阻塞；发送失败只通过返回值报错，不触发回调。
- 框架不设默认请求超时，调用方必须传带超时的 ctx。
- 退出期（`IsExit()` 为真后）拒收新的单向消息与请求，只继续处理响应。

生命周期（`Run`）：启动出站连接 → `AppendStarting`（入站派发由 ready 门控，任何 handler 都在钩子完成后才执行）→ 内核 `Start`（非阻塞标记）→ 起听并注册目录（失败转后台退避重试）→ 等待退出 → 摘除目录 → `AppendStopping`（连接仍开放，可收尾 `Send`；钩子限时执行）→ 停出站连接 → 内核 `Shutdown`（关连接、限时排空、等待读写协程；Gate 持有的客户端 Server 与内核同预算并发关闭）→ `AppendStopped`（逆序）。

目录与选址：`Depend` 声明的依赖是服务名/division 下的**一条持久连接**，地址由 `ext/registry.Resolver` 订阅并更新，连接由 `ext/peer` 退避重连；可选 `lb.Picker` 只在重拨时选 endpoint，不做调用级轮转。需要把消息定向到某个节点时（例如 uid 绑定到固定节点），为该节点单独声明依赖：

```go
dep := a.Depend("game@"+addr, app.WithAddress(addr))
```

`Depend` 在 `Run` 之后调用同样有效，依赖立即开始建立连接。

## 投递语义

| 路径 | 行为 |
|---|---|
| `server.Conn.Send(ctx, m)` | 阻塞至入队、连接关闭（`ErrClosed`）或 ctx 到期；nil ctx 用 flush 兜底（`ErrTimeout`） |
| `server.Conn.TrySend(m)` | 非阻塞；队列满返回 `ErrQueueFull`，连接保持打开 |
| `app.Client.Send` / `rpc.Reply` / `gate.SendTo` | 经 `Send`，事件与回包不丢 |
| `ext/heartbeat` | 经 `TrySend` 非阻塞，允许丢弃（心跳语义） |
| `ext/logfile` | 异步入队，队列满丢弃并计数（日志不是事件流） |

## 插件一览（`ext/`）

| 包 | 说明 |
|---|---|
| `ext/router` | 内核根回调之上的 msgID 路由表：查重（`MustHandle`）、未知帧策略（`WithUnknown`）；可选 `Attach` 后冻结注册 |
| `ext/heartbeat` | 全连接定时心跳：单调度协程逐次快照发送，无常驻会话资源；允许丢弃（心跳语义） |
| `ext/peer` | 出站拨号 + 指数退避自动重连（经 `Conn.Done` 感知断开） |
| `ext/registry` | 服务注册与订阅（目录接口），内置 `etcd` 实现与进程内 `Memory`、`Static` |
| `ext/lb` | 调用级选择策略：`Picker`（轮转/随机/带权）；不 Watch、不拨号 |
| `ext/rpc` | 对称双向复用请求/响应通道：同步 `Call`、异步 `CallAsync`、业务错误码（`RemoteError`）、限时回包 |
| `ext/proto` | protobuf 消息体的泛型 `Register` / `Send`（内核级，无 app 依赖） |
| `ext/trace` | 跨服务 traceID（8 字节 body 前缀约定，Pack/Unpack） |
| `ext/metrics` | 会话数、关闭原因分布、错误计数；支持 Prometheus 文本导出 |
| `ext/ratelimit` | 会话粒度令牌桶入站限流 |
| `ext/breaker` | 出站调用熔断：连续失败断开，冷却期满半开 |
| `ext/shard` | 按 key 分片串行执行的工作池（业务按需使用） |
| `ext/logfile` | 日志文件：按大小轮转、异步写入，实现 `io.Writer`，配合 `slog` |
| `ext/ws` | WebSocket → `net.Conn` / `net.Listener` 适配 |
| `ext/quic` | QUIC → 传输接口适配 |
| `ext/grpc` | gRPC 双向流 → 传输接口桥接 |
| `ext/codec-aes` | 加密连接编解码器（key 必须显式配置）：`NewCodec` 为 legacy 旧协议（全局 IV，CTR 流按连接重启，跨连接明文差泄漏，仅限内网兼容场景）；`NewSecureCodec` 为新协议（每连接每方向随机 nonce 作 CTR 起始计数器，随首帧前缀明文交付，密钥流不复用）。两者均无消息认证，防劫持请上 TLS |
| `ext/codec-legacy` | 旧版三形态帧格式兼容编解码器 |

## 文档

- [framework-v4-review.md](docs/framework-v4-review.md) —— v4 工作区代码评审记录（含运行时复现与复核修正）
- [framework-v4-design.md](docs/framework-v4-design.md) —— 框架 v4 精简内核设计与评审记录（r6 修订见文首）
- [appframe-v4-adapter-design.md](docs/appframe-v4-adapter-design.md) —— 应用框架 v4 适配层设计记录（历史文档）

## 许可

[MIT](LICENSE)
