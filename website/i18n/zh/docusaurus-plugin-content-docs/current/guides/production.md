---
title: 在生产环境中运行
sidebar_position: 7
description: "在生产环境中运行流文件或管道服务：文件的路径和设置、一次发布写下了什么、大小上限、关闭和日志。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 在生产环境中运行

流文件是服务自己的存储。网络 Broker 在其服务器上掌管的事项，在这里由服务负责：文件放在哪里，字节何时到达设备，以及文件会增长到多大。

## 文件 {/* #the-file */}

{/* single-form: the broker's configuration is the same code with and without the macros */}
```rust title="src/main.rs" reference="examples/guide/production.rs#broker"
```

部署通过环境变量指定路径。
文件不存在时，启动过程会创建它。
只读取另一个进程所写文件的服务，用 [`existing_only`](../components/broker/existing-only.md) 打开它，这样错误的路径会让服务在启动时停止。
[`beacon_interval`](../components/broker/beacon-interval.md) 设置文件中索引项的间距。
定位从最近的信标向前读取，所以信标越密，定位的步长越细，占用的空间也越多；客户端的默认值是每 1 MiB 一个。
间隔必须是 1024 的正整数倍，其他任何值都会让服务在启动时停止。
文件客户端只能在 Unix 系统上构建。

## 服务 {/* #the-service */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/production.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/production.rs#service"
```

</TabItem>
</Tabs>

`app` 在生产环境的 `broker` 上构建服务，测试启动的也是同一个 `app`。

## 一次发布写下了什么 {/* #what-a-publish-writes */}

发布把消息追加到它的流键下并刷新到文件，因此调用返回之前，字节已经交给操作系统。
从那时起，本进程或另一个进程中的实时读取者都能看到这条消息。
Broker [关闭](../components/broker/shutdown.md)时，文件同步到设备。
在这两个时间点之间，由操作系统决定字节何时到达设备。

文件客户端把流文件的大小上限定为 16 GiB。
达到上限的发布返回 [`SeaFileError::Publish`](../components/broker/errors.md)，该连接之后的每次发布也都返回这个错误。
因此，长期使用的日志要在达到上限之前换到新文件：服务在新路径上启动，旧文件的读取者用[重放](../components/subscriptions/replay.md)把它读完。

## 关闭 {/* #shutdown */}

Broker 关闭时，等待进行中的发布完成；如果 Broker 要求，就写入[流结束标记](../components/broker/end-with-eos.md)；然后同步文件。
关闭返回时文件已经关闭，所以 `after_shutdown` 钩子可以移动或复制它。
`end_with_eos` 适合停止时文件即完整的写入者，例如按天记录的日志：重放读到该标记即完成，实时读取者则在该标记处结束。
关闭之前保留下来的发布者返回 [`SeaFileError::NotConnected`](../components/broker/errors.md)，从不向已关闭的文件写入。

## 管道级 {/* #a-pipeline-stage */}

一级的标准输出承载它的消息，所以它的日志通过 `tracing` 写到标准错误。
进程中其他任何地方写到标准输出的一行，都会作为消息到达下一级。
stdio 传输属于整个进程：关闭一个 [`StdioBroker`](../components/broker/stdio-broker.md)，会结束其中的每个 stdio 订阅和发布者。

## AsyncAPI 文档 {/* #the-asyncapi-document */}

启用 `asyncapi` feature 时，文件 Broker 是生成文档中的一个[服务器](../components/asyncapi/server.md)，协议为 `file`，描述是流文件的路径。
文档会分享给阅读它的人，路径也随之分享出去。

## 测试配置 {/* #testing-the-configuration */}

测试在 `TestApp::start` 下启动生产环境的 `app`，所以设置按启动时的方式接受检查，而不需要文件。
不是 1024 倍数的信标间隔，在测试中与在真实文件上一样会让启动返回错误。
