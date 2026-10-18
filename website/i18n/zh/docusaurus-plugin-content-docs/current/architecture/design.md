---
title: 设计
sidebar_position: 2
description: "ruststream-sea-file 的位置：在 RustStream 运行时与两个 sea-streamer 客户端之间，流文件一列，管道一列。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "自上而下依次是服务、RustStream 运行时、本 crate、sea-streamer 客户端及其连接的对象",
  "text": "以文字阅读本图",
  "service": "你的服务",
  "handlers": ["#[subscriber]", "订阅者处理器"],
  "app": ["RustStream::new(..)", ".with_broker(..)"],
  "core": "RustStream 核心",
  "runtime": "运行时：分发、编解码器、结算、重试",
  "crate": "ruststream-sea-file",
  "fileSubscriber": ["FileSubscriber", "驱动任务"],
  "stdioSubscriber": ["StdioSubscriber", "自己读取 stdin"],
  "filePublisher": "FilePublisher",
  "stdioPublisher": "StdioPublisher",
  "fileBroker": ["FileBroker", "ConnectedFileBroker"],
  "stdioBroker": ["StdioBroker", "ConnectedStdioBroker"],
  "envelope": ["消息头信封", "消息头放在载荷里"],
  "client": "sea-streamer 客户端",
  "fileClient": "sea-streamer-file",
  "stdioClient": "sea-streamer-stdio",
  "disk": "磁盘上的 .ss 文件",
  "pipes": "stdin 和 stdout",
  "inProcess": ["在 testing 下，Broker 的连接可以是内存中的文件或管道", "而不是客户端"],
  "mounted": "挂载到",
  "polls": "轮询流"
};

# 设计

`ruststream-sea-file` 的位置：在 RustStream 运行时与两个 sea-streamer 客户端之间，流文件一列，管道一列。

<Diagram name="design" words={words} />

本 crate 在 [sea-streamer](https://github.com/SeaQL/sea-streamer) 的两个客户端之上实现核心的 Broker trait。`sea-streamer-file` 负责流文件，`sea-streamer-stdio` 负责进程的标准输出和行格式；标准输入由本 crate 按同一格式自己读取。它上面的运行时对每个 Broker 都相同。文件或管道特有的一切都在本 crate 里：[描述符](../components/subscriptions/file-stream.md)、[位置与定位](../components/positions/index.md)、[ack 的含义](../components/settlement/index.md)、[重试副本发往何处](../components/settlement/retry-after.md)，以及消息头如何传递。

已连接的 Broker 持有一条通往其传输的连接。对文件来说，这条连接包括客户端的 streamer、一个生产者和执行 `connect` 的运行时，每次发布都通过这个生产者追加。对 stdio 来说，它包括客户端的 streamer，以及 Broker 是否回环。Broker 的每个克隆共用这条连接，因此连接两次只会连接一次。连接之前交出的发布者在第一次发布时找到它。

文件连接的每个任务都运行在执行 `connect` 的运行时上，标准输入则在它自己的线程上读取。因此，从另一个运行时打开的订阅，在那个运行时停止后仍会继续投递。

文件订阅是一个驱动任务，它独占客户端的消费者。原因是客户端的定位需要独占消费者，而且不能中途打断。它通过容量为 64 的通道交出消息。stdio 订阅读取进程的读取线程填充的队列。两种传输都一次读取一条消息，因此[批](../components/subscriptions/batches.md)在本 crate 中组装。不满的批在它的第一条消息到达 10 毫秒后发出。

两种传输都不接受逐条消息的设置，所以发布没有选项。客户端只接受载荷，不接受消息头，所以消息头放在载荷里，在[信封](envelope.md)中传递。

各部分如何启动和停止，见[生命周期](lifecycle.md)；一条消息的路径，见[路径](path.md)。
