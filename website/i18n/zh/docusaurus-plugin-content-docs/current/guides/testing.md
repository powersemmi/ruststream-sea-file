---
title: 测试文件或管道服务
sidebar_position: 6
description: "用 TestApp 在进程内测试流文件或管道服务：生产环境的应用、回复、延迟，以及在真实流文件上运行的同一个测试。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 测试文件或管道服务

测试用 [`TestApp`](api:ruststream::testing::TestApp) 运行服务自己的应用，运行在[进程内传输](../components/testing/index.md)或者真实流文件上。

被测服务负责结账。
订单到达 `orders`，处理器在 `receipts` 下以一张收据作为回复。
没有金额的订单被丢弃。
超出反欺诈检查限额的订单会被暂缓：处理器用 [`retry_after`](../components/settlement/retry-after.md) 要求一分钟后再投递它。
经过三次投递后，订单进入[死信键](../components/settlement/dead-letters.md) `orders.review`。
应用由 `service(file)` 构建，所以测试可以选择文件，而 `main` 使用生产环境的文件。
被测服务就是示例文件自己的服务，[`examples/guide/testing.rs`](https://github.com/powersemmi/ruststream-sea-file/blob/main/crates/ruststream-sea-file/examples/guide/testing.rs)。
测试需要在 `[dev-dependencies]` 中启用 `ruststream-sea-file` 的 `testing` feature。

## 订单及其收据 {/* #an-order-and-its-receipt */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) 把 [`FileBroker`](../components/broker/file-broker.md) 连接到内存中的一份日志。
路径从不被打开，所以多个测试可以在一个进程中并行运行。
测试按生产类型访问 Broker：`tb.broker::<FileBroker>()`。
`publish` 在处理器及其回复都完成结算后才返回。
所以接下来的几行断言的是已经完成的状态：订阅者如何结算，以及什么到达了 `receipts`。
测试主体把已启动的应用作为参数接收，所以下面的实时测试也运行它。

## 没有金额的订单 {/* #an-order-without-an-amount */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` 检查处理器返回的结果。
`assert_not_called` 检查没有追加任何收据。

## 不必等待的延迟 {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

测试在暂停的时钟上启动，[`advance`](api:ruststream::testing::TestApp::advance) 让时钟前进。
一分钟的 `HOLD` 不花费任何时间。
延迟结束时，`retry_after` 把订单的副本追加到 `orders` 下。
副本在消息头中带着重试次数，`published` 把它和测试自己的订单一起显示在 `orders` 下。
第三次投递是挂载点允许的最后一次，所以这个订单按到达时的样子进入 `orders.review`。
每次被暂缓的投递都记录为 `Outcome::Nack`。

## 进程内传输模拟了什么 {/* #what-the-in-process-transports-model */}

进程内流文件是内存中的一份日志，每个 Broker 值各有一份新的日志，由它的克隆共享。
它保留文件所保留的行为：

- 每个流键的序号从一开始，实时订阅从末端开始。
- [重放](../components/subscriptions/replay.md)读取日志中已有的内容，读到末尾即完成。
- [位置](../components/positions/index.md)和[定位](../components/positions/seek.md)按日志解析，拒绝的情形与文件相同。
- [`end_with_eos`](../components/broker/end-with-eos.md) 在关闭时结束每个订阅。

服务在启动时读取的日志，在 `TestApp::start` 之前记录在其 Broker 的一个克隆上，[恢复与定位](resume-and-seek.md)的测试就是这样做的。

进程内管道由测试输入的标准输入和服务写出的标准输出组成。
`tb.broker::<StdioBroker>().message(..)` 是以其键到达的一行，`published::<T>(key)` 读取这一级写出的一行。
只有在 [`loopback`](../components/broker/loopback.md) 下，一级才会读到自己的输出，与真实管道上一样。
两种传输都拒绝真实传输所拒绝的内容：客户端不接受的流键、管道上的空行，以及不是 1024 倍数的信标间隔。

`.ss` 文件的字节、文件在多次运行之间保留的内容，以及真实管道上的行格式和输入结束，只在真实传输上才能看到。

## 在流文件上运行同一个测试 {/* #the-same-test-on-a-stream-file */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) 在临时目录中的真实流文件上运行同一个应用。
传输就是本地文件系统，所以实时测试不需要服务器，与其他测试一起在 `cargo test` 中运行。
文件以进程命名，所以之前运行追加的内容不会被读到，测试结束时会删除该文件。
断言与进程内的完全相同。
管道级的实时测试把它作为子进程启动，标准输入和标准输出都接到管道上，[shell 管道](pipelines.md#testing-the-stage)展示了这一点。
