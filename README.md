# leanwire

[![CI](https://github.com/segdumping/leanwire/actions/workflows/ci.yml/badge.svg)](https://github.com/segdumping/leanwire/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/segdumping/leanwire.svg)](https://pkg.go.dev/github.com/segdumping/leanwire)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Go实现的通信框架。内核只有两个包：`codec` 定义消息与格式（`| len:u32 | id:u32 | body |`，大端），`server` 负责连接管理、每帧回调和启停。传输层使用标准库的 `net.Conn` / `net.Listener`，TCP、TLS无需适配。

路由、RPC、心跳、服务目录等能力以插件形式放在 `ext/`，按需引入；`app` 包把它们与内核组合成单进程服务骨架，目录注册、出站连接、依赖重连和生命周期都已内置。

## 特性

- 内核仅 `codec` 和 `server` 两个包，入站只有一个根回调
- 传输接口即标准库的 `net.Conn` / `net.Listener`，WebSocket、QUIC、gRPC通过适配器接入
- 回调在读协程内联执行，同一连接严格保序，panic被隔离为连接关闭
- 三种发送语义：`Send` 阻塞入队、`SendWait` 按ctx等待、`TrySend` 队列满即返回
- 内核对 `ext/` 零依赖，删除全部插件仍可编译
- `app` 提供目录注册、服务依赖、请求/响应、Session与Gate，可单进程起一套服务

## 安装

Go 1.26及以上：

```
go get github.com/segdumping/leanwire
```

## 快速开始

### 服务端

路由表来自 `ext/router`：

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
	srv := server.New(server.WithIdleTimeout(5 * time.Minute))
	routeTable := router.New()
	srv.SetRootHandler(routeTable.Handler())

	routeTable.Handle(1, func(s *server.Conn, m codec.Msg) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = s.SendWait(ctx, codec.Msg{ID: m.ID, Body: m.Body})
	})

	listener, err := net.Listen("tcp", ":9000")
	if err != nil {
		log.Fatal(err)
	}
	if err := srv.Serve(listener); err != nil {
		log.Fatal(err)
	}
	if err := srv.Start(); err != nil {
		log.Fatal(err)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
```

`Start` 只标记启动，`Shutdown(ctx)` 等待读写协程退出，两步都由调用方驱动。

### 客户端

客户端复用同一套 `server.Server`，用 `Connect` 接管已拨号的连接：

```go
conn, _ := net.Dial("tcp", "127.0.0.1:9000")

client := server.New()
routeTable := router.New()
client.SetRootHandler(routeTable.Handler())
routeTable.Handle(1, func(s *server.Conn, m codec.Msg) {
	log.Printf("echo: %s", m.Body)
})
c, err := client.Connect(conn)
if err != nil {
	log.Fatal(err)
}
_ = c.Send(codec.Msg{ID: 1, Body: []byte("hello")})
```

根回调建议在Serve/Connect之前设置：连接创建时会快照，运行中修改只影响后续新连接。换TLS只需改用 `tls.NewListener` 和 `tls.Dial`，业务代码不变。

## 服务层（app）

`app` 把内核、目录、出站连接和生命周期组合成服务骨架：

```go
reg, _ := etcd.New([]string{"10.0.0.1:2379"})
a := app.New("game1",
	app.WithRegistry(reg),
	app.WithServiceName("game"),
	app.WithAdvertise("10.0.0.1:9001"),
)

// 单向消息：消息号 + 二进制body
a.Handle(1, func(s app.Session, body []byte) {
	_ = s.SendBytes(2, respBody)
})

// 双向RPC
a.RPC().Handle(102, func(_ *server.Conn, _ uint32, body []byte, reply rpc.Reply) {
	_ = reply(respBody, nil)
	// _ = reply(nil, &rpc.RemoteError{Code: 500, Msg: "boom"})
})

a.OnStart(func(a *app.App) { ... })
a.OnStop(func(a *app.App) { ... })
a.OnFini(func(a *app.App) { ... })

// 服务依赖：一条经目录选址、自动重连的持久连接
service := a.Depend("rank")
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
defer cancel()
resp, err := service.Call(ctx, 2, body)
err = service.CallAsync(ctx, 3, body, func(resp []byte, err error) {})
err = service.Send(ctx, 1, body)

a.Listen(":9001")
a.Run() // 阻塞至SIGINT/SIGTERM或Exit()
```

`Run` 的执行顺序：启动出站连接 → `OnStart` → 内核 `Start` → 监听并注册目录 → 等待退出 → 注销目录 → `OnStop` → 停止出站连接 → 内核 `Shutdown` → `OnFini`。

### Session

入站handler拿到的是 `app.Session`，只包含消息号和body：

```go
a.Handle(1001, func(s app.Session, body []byte) {
	_ = s.SendBytes(1002, respBody) // 限时投递用SendWaitBytes(ctx, …)
})
```

- `Session.ID()` 默认取 `server.Conn.ID()`，需要conn到uid的映射时用 `app.WithSessionID`
- handler由app worker执行，同一key串行；不要在回调里同步等待同一条连接的 `Call`，回包会经过同一个接收协程

### Gate

客户端接入和uid关联使用 `app.Gate`：

```go
clientSrv := server.New(server.WithCodec(clientCodec), server.WithLogger(a.Log()))
g := app.NewGate(a, clientSrv)

g.HandleClient(1, func(s app.Session, body []byte) { ... })   // 客户端连接的消息
g.HandleServer(200, func(s app.Session, body []byte) { ... }) // 内部服务连接的消息

_ = g.Bind(s, uid) // uid → Session，同uid顶号
_ = g.SendTo(ctx, uid, msgID, payload)
n, err := g.Broadcast(ctx, msgID, payload)
```

节点之间按division建持久连接，例如 `a.Depend("example.game.2")`；静态地址用 `app.WithAddress("10.0.0.11:9001")`。`app.WithPicker` 只在重拨时选择endpoint，调用级负载均衡由业务维护 `ext/lb.Picker`。

## 使用说明

发送有三个入口：

- `Send(m)` 阻塞至入队或flush超时，连接已关闭时返回 `ErrClosed`
- `SendWait(ctx, m)` 按ctx等待，入队、连接关闭或ctx到期时返回
- `TrySend(m)` 非阻塞，队列满返回 `ErrQueueFull`，连接保持打开

`app.Client.Send`、`rpc.Reply`、`Gate.SendTo`、`Broadcast` 内部走 `SendWait`，事件与回包不丢；`ext/heartbeat` 走 `TrySend`，允许丢帧；`ext/logfile` 异步写入，队列满时丢弃并计数。

默认参数：帧上限64 KiB（两端须一致）、发送队列1024、空闲10分钟、写超时与停机排空3秒，日志默认 `slog.Default()`。分别用 `WithCodec`、`WithQueueSize`、`WithIdleTimeout`、`WithFlushTimeout` 调整。

其他约定：

- 消息号 `0xF000–0xFFFF` 为保留段（`codec.IsReserved`），其中 `0xF001` 心跳、`0xF002` RPC、`0xF005` TUN，业务消息请避开
- 未注册的msgID缺省丢弃并记Debug，不断开连接；对外服务可用 `router.WithUnknown` 改为断开
- `Handle` 重复注册会覆盖并记Warn；要求严格唯一时用 `router.MustHandle` 或 `app.MustHandle`，重复即panic。惯例在首次Serve/Connect前完成装配
- 发送超过 `MaxBody` 时写协程返回 `codec.ErrTooLarge`，按写错误关闭连接

## 扩展包

| 包 | 说明 |
|---|---|
| `ext/router` | msgID路由表：冻结注册、`MustHandle` 查重、`WithUnknown` 处理未知帧 |
| `ext/rpc` | 双向复用的请求/响应通道：`Call`、`CallAsync`、`RemoteError`、限时回包 |
| `ext/heartbeat` | 定时心跳，单调度协程逐次快照发送，允许丢帧 |
| `ext/peer` | 出站拨号与指数退避重连 |
| `ext/registry` | 服务目录，内置 `etcd` 实现和进程内的 `Memory`、`Static` |
| `ext/lb` | 调用级选点：`Picker`（轮转/随机/带权），不Watch不拨号 |
| `ext/proto` | protobuf消息体的泛型 `Register` / `Send` |
| `ext/trace` | 跨服务traceID，8字节body前缀约定 |
| `ext/metrics` | 会话数、关闭原因、错误计数，支持Prometheus文本导出 |
| `ext/ratelimit` | 会话粒度令牌桶限流 |
| `ext/breaker` | 出站熔断：连续失败断开，冷却期满半开 |
| `ext/shard` | 按key分片串行执行的工作池 |
| `ext/logfile` | 日志文件，按大小轮转、异步写入，实现 `io.Writer` |
| `ext/ws` | WebSocket → `net.Conn` / `net.Listener` 适配 |
| `ext/quic` | QUIC → 传输接口适配 |
| `ext/grpc` | gRPC双向流 → 传输接口桥接 |
| `ext/codec-aes` | 加密连接编解码器（key须显式配置，`NewSecureCodec` 每连接每方向随机nonce起始计数器）；无消息认证，防劫持请上TLS |
| `ext/codec-legacy` | 扩展帧编解码器：`\| len \| id \| extLen \| ext \| body \|`，附 `WrapExt`/`SplitExt` 前缀组装助手 |

## 文档

API文档见 [pkg.go.dev/github.com/segdumping/leanwire](https://pkg.go.dev/github.com/segdumping/leanwire)。

## 贡献

欢迎Issue和PR。提交前请确认 `go vet ./...` 与 `gofmt -l .` 无输出，新增能力附带测试。

## 许可

[MIT](LICENSE)
