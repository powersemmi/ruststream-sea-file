---
title: 快速开始
sidebar_position: 1
description: "安装 ruststream-sea-file，在流文件上运行第一个服务，查看文件内部，测试服务，并在管道中运行同一个处理器。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 快速开始

本指南安装 `ruststream-sea-file`，在流文件上运行第一个服务，再把同一个处理器放进 shell 管道。
两种传输都不需要服务器。

## 安装 {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-sea-file = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-sea-file = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`ruststream` 是框架本身：`macros` 提供属性宏，`json` 提供默认编解码器，`logging` 把服务的日志打印到终端。
`ruststream-sea-file` 包含两种传输。
它的 `testing` feature 加入测试所用的[进程内传输](../components/testing/index.md)，`asyncapi` 加入本 crate 为 [AsyncAPI 文档](../components/asyncapi/index.md)提供的内容。
这两个 feature 默认都不启用。
流文件客户端只能在 Unix 系统上构建，因此本 crate 无法在 Windows 上构建。

## 第一个服务 {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` 读取文件 `/tmp/orders.ss` 中流键 `orders` 下的 JSON 订单。
带金额的订单被 ack，没有金额的订单被丢弃。
[`FileBroker::new`](../components/broker/file-broker.md) 只记录路径。
文件在服务启动时才打开，文件不存在时，启动过程会创建它。
[`FileStream::new`](../components/subscriptions/file-stream.md) 指定流键，[`start_at(FilePosition::beginning())`](../components/positions/start-at.md) 让订阅从文件开头而不是末尾读取该键。
`#[ruststream::app]` 把 `app` 变成 `main`；在手写形式中，[`run_main`](api:ruststream::runtime::cli::run_main) 做同样的事。

运行服务：

```bash
cargo run -- run
```

服务读取文件中已有的每个订单，然后等待下一个。
另一个服务在同一路径上以 `orders` 发布订单，就会把它追加到文件中，[磁盘上的事件日志](event-log.md)展示了这一点。
<kbd>Ctrl</kbd>+<kbd>C</kbd> 停止服务。

## 文件内部 {/* #inside-the-file */}

文件客户端自带一个解码器，它把流文件按每行一条消息打印出来：

```bash
cargo install sea-streamer-file --features=executables --bin ss-decode
ss-decode --file /tmp/orders.ss
```

每一行的形式是 `[timestamp | stream key | sequence | shard] payload`，例如 `[2026-10-18T09:12:03.120 | orders | 1 | 0] {"id":1,"amount_cents":1250}`。
一个文件容纳所有流键，每个键的序号从一开始。
新文件中还有一条 `ruststream-internal` 键下的消息，由本 crate 在创建文件时写入。

## 第一个测试 {/* #the-first-test */}

示例文件里还有两个测试。
每个测试都用 [`TestApp::start`](../components/testing/index.md) 启动同一个 `app`，发布一个订单，然后检查 `take_order` 如何结算它。
在测试中，`FileBroker` 在进程内运行，所以测试不接触任何文件：

```bash
cargo test
```

[测试文件或管道服务](testing.md)逐一讲解测试套件，以及进程内传输模拟了什么。

## 管道中的同一个处理器 {/* #the-same-handler-in-a-pipeline */}

另一种传输读取标准输入，写入标准输出。
处理器保持不变，改变的是 prelude、Broker 和订阅。

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/getting_started_pipe.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started_pipe.rs#service"
```

</TabItem>
</Tabs>

[`StdioBroker::new`](../components/broker/stdio-broker.md) 不接收任何设置，订阅只指定流键。
管道的一级用 [`out_retry(Publish).to(..)`](../components/settlement/out-retry.md) 指定延迟副本发往的键，没有它就不会启动。
两个 prelude 中的发布策略都叫 [`Publish`](../components/publishing/index.md)，所以负责发布的代码在两种传输上写法相同。

给这一级输入一行：

```bash
echo '[orders] {"id":1,"amount_cents":1250}' | cargo run -- run
```

方括号指定流键。
日志写到标准错误，因为标准输出把这一级的消息送往下一个进程。
[shell 管道](pipelines.md)讲解行格式和多级串联。

## 从 sea-streamer 迁移 {/* #coming-from-sea-streamer */}

基于 `sea-streamer-file` 的服务要自己编写消费者循环和生产者。
下表列出每一部分在 RustStream 中的位置：

| sea-streamer | RustStream |
|---|---|
| `FileStreamer::connect` 加上 `FileConnectOptions` | [`FileBroker::new`](../components/broker/file-broker.md) 及其设置，在启动时生效 |
| `create_consumer` 加一个 `next()` 循环 | [`FileStream`](../components/subscriptions/file-stream.md) 上的 `#[subscriber]` 处理器 |
| 解码消息载荷 | 处理器的参数类型，由编解码器解码 |
| `create_producer` 和 `send_to` | 回复或 `Out` 参数，经由 [`Publish`](../components/publishing/file-publish.md) 发送 |
| 在消费者选项中调用 `set_auto_stream_reset`，再调用 `seek_to` | 挂载点上的 [`start_at`](../components/positions/start-at.md)，或处理器中的[定位句柄](../components/positions/seek.md)（[恢复与定位](resume-and-seek.md)） |
| `StdioStreamer` | [`StdioBroker`](../components/broker/stdio-broker.md) |
