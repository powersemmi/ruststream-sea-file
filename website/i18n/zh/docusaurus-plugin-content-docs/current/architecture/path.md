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
  "pRead": ["stdin 线程", "解析这一行"],
  "pTask": ["读取任务", "属于该订阅"],
  "pChannel": "容量 64 的通道",
  "pStream": ["StdioSubscriber", "每次轮询一条消息"],
  "pBatch": ["组批", "在 batch(n) 下"],
  "pDecode": ["解码"],
  "pHandler": ["订阅者的处理器", "#[subscriber] fn"],
  "pPublish": ["StdioPublisher", "需要时套信封"],
  "pWrite": ["stdout 线程", "写出这一行"]
};

# 路径

一条消息在 RustStream 服务中的路径，分别在流文件和管道上：读取者、通道、处理器、应答和写入。

<Diagram name="path" words={words} />

订阅的任务走在处理器前面读取。它从客户端取出每条消息，打开[信封](envelope.md)，把载荷复制到自己的缓冲区里。它把消息的编号写进 `stream-sequence` 消息头，然后把这次投递排入容量最多为 64 的通道。处理器从这个通道读取，因此在处理当前消息的同时，下一条消息已经从文件中读出。

在文件上，每次投递都带有读取者读到它时所处的代号。[定位](seeking.md)开启一个新代号，订阅丢弃在旧代号下排队的内容。投递还持有对订阅定位器的一个共享引用，因此为每条消息构建上下文只是增加引用计数。

处理器的应答编码进分发循环复用的缓冲区。发布者把这些字节复制进交给客户端的消息。只有在消息带有消息头时，或者在管道上一行会改变载荷时，发布者才用信封把它们包起来。文件发布在追加内容刷写到文件之后返回。写入订阅所读文件的应答，会被它的读取者跳过，因为它属于另一个流键。

投递在应答之后结算。在两种传输上，ack 和 nack 都报告自己不受支持，[结算](settlement.md)一页用图说明了这一点。
