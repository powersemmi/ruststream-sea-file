---
title: shell 管道
sidebar_position: 4
description: "把 RustStream 服务作为 shell 管道中的一级运行：行格式、向一级输入、读取它的输出、输入结束时退出，以及多级串联。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# shell 管道

本指南把服务作为 shell 管道 `producer | stage | consumer` 中的一级运行。

[`StdioBroker`](../components/broker/stdio-broker.md) 把进程的标准输入作为流读取，并写入标准输出。
订阅读取某一个流键的行，发布则为下一个进程写出一行。

示例是一个定价级：它读取订单，并用一张收据回复每个订单。

## 管道级 {/* #the-stage */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/pipelines.rs#stage"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/pipelines.rs#stage"
```

</TabItem>
</Tabs>

`price` 读取 `orders` 下的行，并以一张 `Receipt` 回复，回复以 `receipts` 键写到标准输出。
即使 `price` 从不重试，这一级也需要 [`out_retry(Publish).to(..)`](../components/settlement/out-retry.md)：在管道上，延迟副本只能发往后面的某一级，所以每个注册都要指定这个键，没有它这一级就不会启动。

## 行 {/* #the-line */}

一行由方括号中的消息头和其后的载荷组成（[行](../components/subscriptions/lines.md)）：

```text
[timestamp | stream_key | sequence | shard_id] payload
```

消息头的每个字段都是可选的，所以生产者通常只写键：

```bash
echo '[orders] {"id":7,"quantity":3,"unit_cents":250}' | ./pricing
```

其他键下的行会被跳过，所以多个级可以从同一个流中各取自己的键。
没有消息头的行不指定键，客户端把它交给这一级的每个订阅。
因此，输出普通 JSON 行的工具会同时为每个处理器提供输入：

```bash
jq -c '.[]' orders.json | ./pricing
```

这一级在服务看到一行之前先解析它的消息头。
读不了的行会被跳过，日志里留下一条警告，这一级继续往下读。
被跳过的行有两种：不是 UTF-8 的输入，以及开头的 `[...]` 不是消息头，例如一个 JSON 数组。
产生这类载荷的生产者要在每个载荷前面写上消息头。

## 输出 {/* #the-output */}

这一级每次发布写出一行，消息头由客户端填写：

```text
[2026-10-18T15:08:01 | receipts | 0] {"order":7,"total_cents":750}
```

时间戳是写入时间，精确到秒；序号为每个键的行从零开始计数。
读取普通 JSON 的工具取消息头之后的载荷：

```bash
./pricing < orders.txt | sed 's/^\[[^]]*\] //' | jq .total_cents
```

文本行会改变的载荷放进[信封](../components/headers/payloads.md)发出：二进制载荷、带换行符或首尾空白的载荷，以及每条带消息头的消息。
信封是以 `rs1:` 或 `rs2:` 开头的一行，供下一个 RustStream 级使用；它打开信封，把发布时的字节和消息头交给自己的处理器。

日志通过 `tracing` 写到标准错误，所以从不与消息混在一起。
用 `println!` 打印到标准输出的一行，会作为没有键的行到达下一级，那里的每个订阅都会读到它。

## 输入结束时 {/* #when-the-input-ends */}

{/* single-form: `main` is the same with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/pipelines.rs#exit"
```

标准输入结束时，这一级的每个订阅在交付完已经读到的行之后结束。
`main` [启动](api:ruststream::runtime::RustStream::start)服务并观察 [`HealthProbe`](api:ruststream::runtime::HealthProbe)，它统计仍在读取的订阅。
一个都不剩时，[`shutdown`](api:ruststream::runtime::RunningApp::shutdown) 停止服务，这一级以状态码 0 退出。
fail-fast 失败也会结束等待，`shutdown` 把它作为错误返回，于是这一级以失败状态码退出。
所以 `./pricing < orders.txt` 为文件里的每个订单定价，然后退出。

`#[ruststream::app]` 则一直运行服务，直到收到 `SIGINT` 或 `SIGTERM`。
这适合在整个运行期间都等待输入的一级，例如接在 `tail -f` 后面的一级。

## 多级串联 {/* #chaining-stages */}

```bash
./intake < orders.txt | ./pricing | ./billing
```

每一级读取它订阅的键，并为下一级写出回复。
消息头和二进制载荷完整地穿过每一个管道，因为每一级都会打开上一级写出的信封。
稍后重试的一级把副本以挂载点指定的键沿管道向下发送，由后面的某一级读取，[重试与死信](retries.md)展示了这一点。

## 测试这一级 {/* #testing-the-stage */}

示例文件在进程内测试这一级：[`TestApp::start`](../components/testing/index.md) 运行它，`tb.broker::<StdioBroker>().message(..)` 注入一行订单，`published::<Receipt>("receipts")` 读取它写出的行。

实时测试把构建好的这一级作为子进程启动，标准输入和标准输出都接到管道上。
它们检查这一级回复 `orders` 行和没有键的行，跳过其他键、JSON 数组行和非 UTF-8 输入，并在输入结束时退出。
[`loopback`](../components/broker/loopback.md) 把一级自己的输出交还给它，这样两级可以在一个进程中运行，由一个测试同时驱动。
