---
title: 路径
sidebar_position: 4
description: "一条消息在 RustStream 服务中的路径，分别在流文件和管道上：读取者、通道、处理器、应答和写入。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一条需要应答的消息的路径，分别在流文件和管道上",
  "text": "以文字阅读本图",
  "fileLane": "流文件",
  "pipeLane": "管道",
  "fRead": ["客户端消费者", "读取文件"],
  "fDriver": ["驱动任务", "打上代号"],
  "fChannel": "容量 64 的通道",
  "fStream": ["FileSubscriber", "丢弃旧代号"],
  "fBatch": ["组批", "在 batch(n) 下"],
  "fDecode": ["FileContext，", "然后解码"],
  "fHandler": ["订阅者的处理器", "#[subscriber] fn"],
  "fPublish": ["FilePublisher", "有消息头时套信封"],
  "fAppend": ["追加、刷写", "写入文件"],
  "pRead": ["读取线程", "解析这一行"],
  "pTask": ["分发", "按流键"],
  "pChannel": "每个订阅一个队列",
  "pStream": ["StdioSubscriber", "每次轮询打开一行"],
  "pBatch": ["组批", "在 batch(n) 下"],
  "pDecode": ["解码"],
  "pHandler": ["订阅者的处理器", "#[subscriber] fn"],
  "pPublish": ["StdioPublisher", "需要时套信封"],
  "pWrite": ["stdout 线程", "写出这一行"]
};

# 路径

一条消息在 RustStream 服务中的路径，分别在流文件和管道上：读取者、通道、处理器、应答和写入。

<Diagram name="path" words={words} />

在文件上，订阅的任务走在处理器前面读取。它从客户端取出每条消息，保留读到的字节，由[信封](envelope.md)在其中找到载荷，然后把这次投递排入容量最多为 64 的通道。处理器从这个通道读取，因此在处理当前消息的同时，下一条消息已经从文件中读出。

在管道上，走在前面读取的是进程的读取线程。它把每一行放进该键各个订阅的队列，队列不设上限；处理器要下一条消息时，信封才找出载荷。

在两种传输上，消息头都在第一次被读取时才解析，消息的编号也在这时写进 [`stream-sequence`](../components/headers/sequence-header.md) 消息头。只读取载荷的处理器根本不会解析消息头。

在文件上，每次投递都带有读取者读到它时所处的代号。[定位](seeking.md)开启一个新代号，订阅丢弃在旧代号下排队的内容。投递还持有对订阅定位器的一个共享引用，因此为每条消息构建上下文只是增加引用计数。

处理器的应答编码进分发循环复用的缓冲区。信封不改动这些字节时，发布者把它们原样交给客户端，不做复制。默认信封只在消息带有消息头时，或者在管道上一行会改变载荷时，才把它们包起来。文件发布在追加内容刷写到文件之后返回。写入订阅所读文件的应答，会被它的读取者跳过，因为它属于另一个流键。

投递在应答之后结算。在两种传输上，ack 和 nack 都报告自己不受支持，[结算](settlement.md)一页用图说明了这一点。
