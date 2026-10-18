---
title: 管道
sidebar_position: 3
description: "本 crate 在一个线程上读取标准输入，stdio 客户端在另一个线程上写入标准输出，每条消息一行，整个进程共用。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一级如何从上一级接收行，又如何把行送往下一级",
  "text": "以文字阅读本图",
  "upstream": "上一级",
  "stdin": ["读取线程", "读取并解析一行"],
  "dispatch": ["按流键分发：", "没有键的行", "发给每个订阅"],
  "subscriptions": ["StdioSubscriber", "每个一个队列"],
  "publisher": ["StdioPublisher", "需要时套上信封"],
  "stdout": ["stdout 线程", "编号，打时间戳"],
  "line": ["[time | key | seq]", "payload"],
  "downstream": "下一级",
  "process": ["每个进程一个读取线程和一个 stdout 线程：", "关闭会结束其中的每个 stdio 流"],
  "lines": "行"
};

# 管道

本 crate 在一个线程上读取标准输入，stdio 客户端在另一个线程上写入标准输出，每条消息一行，整个进程共用。

<Diagram name="pipe" words={words} />

一行的形式是 `[timestamp | stream_key | sequence | shard_id] payload`，方括号中的每个字段都是可选的。时间戳的格式为 `YYYY-MM-DDTHH:MM:SS`，允许带秒的小数部分。以 `[WORD]` 开头的行，读作流键为 `WORD` 的消息。

第一个订阅启动读取线程。该线程读取一行，按客户端格式的定义解析它的字段，然后放进该键各个订阅的队列。没有键的行发给每个订阅。因此，上一级标准输出上的一行杂散输出，会到达本级的每个订阅。线程在第一个订阅出现之后才启动，所以本级启动时已经在等待的行不会丢失。不是 UTF-8 的行，或者方括号部分不是消息头的行，会被跳过并留下一条警告，线程继续往下读。

标准输入结束时，线程在每个订阅收完已经排入队列的行之后结束它，之后才打开的订阅会立即结束。随后这一级按 [shell 管道](../../guides/pipelines.md#when-the-input-ends)展示的方式退出。

第一次发布启动 stdout 线程。它为每个键从零开始编号，以秒为精度打上写入时间，然后输出这一行。它会跳过空载荷，因此[发布者](../../components/publishing/stdio-publish.md)拒绝空消息，而不是报告一行从未发出的内容。一行会改变的载荷放进[信封](../envelope.md)。

虚线箭头是 [`loopback`](../../components/broker/loopback.md)：进程写出的每一行也会放进它自己订阅的队列，带着 stdout 线程给它的消息头。这样，一个 stdio 服务可以在一个进程里运行，测试正是这样做的。`loopback` 下的订阅读取进程自己的输出，所以标准输入结束时它保持打开。

两个线程都属于进程，而不属于某个 Broker。[关闭](../../components/broker/shutdown.md)会在每个订阅交付完已读取的行之后，结束进程持有的每个 stdio 订阅和生产者。

行格式和 stdout 线程属于 `sea-streamer-stdio`（[客户端](https://docs.rs/sea-streamer-stdio/1.0.0-rc.2/sea_streamer_stdio/)）。订阅如何读取它的键，见[行](../../components/subscriptions/lines.md)。
