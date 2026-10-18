---
title: 管道
sidebar_position: 3
description: "stdio 客户端在一个线程上读取标准输入，在另一个线程上写入标准输出，每条消息一行，整个进程共用。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "stdio 客户端如何从上一级接收行，又如何把行送往下一级",
  "text": "以文字阅读本图",
  "upstream": "上一级",
  "stdin": ["stdin 线程", "读取并解析一行"],
  "dispatch": ["按流键分发：", "没有键的行", "发给每个消费者"],
  "consumers": ["StdioConsumer", "每个订阅一个"],
  "publisher": ["StdioPublisher", "需要时套上信封"],
  "stdout": ["stdout 线程", "编号，打时间戳"],
  "line": ["[time | key | seq]", "payload"],
  "downstream": "下一级",
  "process": ["每个进程一个 stdin 线程和一个 stdout 线程：", "关闭会结束其中的每个 stdio 流"],
  "lines": "行"
};

# 管道

stdio 客户端在一个线程上读取标准输入，在另一个线程上写入标准输出，每条消息一行，整个进程共用。

<Diagram name="pipe" words={words} />

一行的形式是 `[timestamp | stream_key | sequence | shard_id] payload`，方括号中的每个字段都是可选的。时间戳的格式为 `YYYY-MM-DDTHH:MM:SS`，允许带秒的小数部分。以 `[WORD]` 开头的行，读作流键为 `WORD` 的消息。

第一个订阅启动 stdin 线程。该线程读取一行，解析它的字段，然后交给该键的消费者。没有键的行发给每个消费者。因此，上一级标准输出上的一行杂散输出，会到达本级的每个订阅。本级启动时，本 crate 会扣住标准输入，直到它的第一个消费者出现，所以已经在等待的行不会丢失。

第一次发布启动 stdout 线程。它为每个键从零开始编号，以秒为精度打上写入时间，然后输出这一行。它会跳过空载荷，因此[发布者](../../components/publishing/stdio-publish.md)拒绝空消息，而不是报告一行从未发出的内容。一行会改变的载荷放进[信封](../envelope.md)。

虚线箭头是 `loopback`：stdout 线程还把每一行交给本进程自己的消费者。这样，一个 stdio 服务可以在一个进程里运行，测试正是这样做的。

两个线程都属于进程，而不属于某个 Broker。[关闭](../../components/broker/shutdown.md)会结束进程持有的每个 stdio 消费者和生产者。标准输入结束时，stdin 线程停止，订阅仍保持打开。一行不是 UTF-8，或者方括号部分无法解析，都会使进程中止。

这一模型是 `sea-streamer-stdio` 自己的模型（[客户端](https://docs.rs/sea-streamer-stdio/1.0.0-rc.2/sea_streamer_stdio/)）。订阅如何读取它的键，见[行](../../components/subscriptions/lines.md)。
