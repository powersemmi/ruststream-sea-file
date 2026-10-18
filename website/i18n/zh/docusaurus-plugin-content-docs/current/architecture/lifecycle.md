---
title: 生命周期
sidebar_position: 3
description: "FileBroker::new 记录路径，connect 打开文件，shutdown 结束它的写入者；StdioBroker 在第一次使用时接上进程的管道。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const file = {
  "title": "文件 Broker 的三个状态作为类型，以及每次转换对流文件做什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "transport": "文件传输",
  "unconnected": ["FileBroker::new", "(path)"],
  "connected": ["ConnectedFileBroker"],
  "closed": ["Closed = ()", "没有要报告的"],
  "config": ["记录路径和设置，", "无 I/O"],
  "open": ["打开或创建", "文件；为空文件", "写入预置消息"],
  "subscriptions": ["每个订阅：", "一个消费者，或", "一个重放读取者"],
  "publishes": ["每次发布：", "追加并刷写"],
  "shutdown": ["等待发布完成，", "设置时写入结束标记，", "刷写，", "同步到设备"],
  "afterShutdown": ["之前保留的发布者返回 NotConnected，", "通过旧定位器的定位被拒绝"],
  "connect": "connect(self)",
  "close": "shutdown(self)"
};

export const stdio = {
  "title": "stdio Broker 的三个状态作为类型，以及每次转换对进程的管道做什么",
  "text": "以文字阅读本图",
  "states": "Broker",
  "transport": "管道传输",
  "unconnected": ["StdioBroker::new()"],
  "connected": ["ConnectedStdioBroker"],
  "closed": ["Closed = ()", "没有要报告的"],
  "config": ["记录 loopback，", "不碰任何管道"],
  "reading": ["第一个订阅：", "stdin 线程"],
  "writing": ["第一次发布：", "stdout 线程"],
  "shutdown": ["结束进程中的", "每个 stdio 流"],
  "afterShutdown": ["之前保留的发布者返回 NotConnected"],
  "connect": "connect(self)",
  "close": "shutdown(self)"
};

# 生命周期

`FileBroker::new` 记录路径，connect 打开文件，每个订阅打开一个读取者，shutdown 结束写入者；`StdioBroker` 在第一次使用时接上进程的管道。

<Diagram name="lifecycle" words={file} />

`new` 不做任何 I/O，所以服务的构建函数保持同步。运行时在应用启动时连接 Broker。文件不存在时，connect 会创建它；在 [`existing_only`](../components/broker/existing-only.md) 下则会失败。connect 检查信标间隔，打开客户端的 streamer 和一个生产者，并向还没有任何内容的文件写入预置消息。

每个订阅在执行 `connect` 的运行时上打开它的读取者。实时订阅打开一个位于末端的消费者，重放则打开一个读取整个文件的读取者。发布追加并刷写，[写入者](transports/writer.md)一页用图说明了这一点。

关闭先把连接标为已关闭，因此在此之后开始的发布会失败。然后它等待在途的发布，并取出生产者。客户端结束该文件的写入者：设置了[流结束标记](../components/broker/end-with-eos.md)时写入它，然后刷写，并把文件同步到设备。本 crate 在这之后才丢弃自己的生产者，并再次请求客户端。否则，一个晚丢弃的句柄会结束下一次连接到同一文件时的写入者。

关闭之后，之前保留的[发布者](../components/broker/shutdown.md)返回 `NotConnected`，通过之前保留的定位器进行的定位会被拒绝。二者都不会对已关闭的文件报告成功。

## stdio Broker {/* #the-stdio-broker */}

<Diagram name="stdioLifecycle" words={stdio} />

connect 记录 Broker 是否回环，不碰任何管道。进程中的第一个订阅启动客户端的 stdin 线程，第一次发布启动它的 stdout 线程，[管道](transports/pipe.md)一页用图说明了这一点。两个线程服务于整个进程，因此关闭会结束其中的每个 stdio 消费者和生产者，包括其他 Broker 的。
