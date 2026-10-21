---
title: 流文件
sidebar_position: 0
description: ".ss 流文件是磁盘上供所有流键共用的一份日志：一个文件头、逐帧写入的消息、每个间隔处的一个信标，以及可选的流结束标记。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "磁盘上的流文件，逐个区域排列，其中一条消息拆成各个字段",
  "text": "以文字阅读本图",
  "file": "orders.ss",
  "header": ["文件头，128 字节", "信标间隔"],
  "orders1": ["消息", "orders，1 号"],
  "receipts1": ["消息", "receipts，1 号"],
  "orders2": ["消息", "orders，2 号"],
  "beacon": ["信标", "每个间隔一个"],
  "continued": ["orders 2，", "后半部分"],
  "eos": ["流结束标记", "设置时写入"],
  "message": "一条消息",
  "key": ["流键", "长度和名称"],
  "meta": ["分片、序号、", "毫秒时间戳"],
  "size": "载荷大小",
  "payload": "载荷",
  "checksum": "校验和",
  "end": "0x0D",
  "keys": ["所有流键共用这个文件，各自从一开始编号，", "全部位于分片 0"]
};

# 流文件

`.ss` 流文件是磁盘上供所有流键共用的一份日志：一个文件头、逐帧写入的消息、每个间隔处的一个信标，以及可选的流结束标记。

<Diagram name="streamFile" words={words} />

文件以 128 字节的文件头开始。文件头包含字节 `Ss`、格式版本、文件名、创建时间和信标间隔。

消息按写入顺序依次排列，与流键无关。每条消息依次带有它的键、一个分片、一个序号和以毫秒计的时间戳，然后是载荷和校验和。数字采用大端序，每一帧以一个 `0x0D` 字节结束。因此，载荷为文本的文件在编辑器里仍然可读。

流键从一开始编号。之后连接到同一文件并追加的进程，会接着原来的编号继续。这个客户端把每条消息都写到分片零。

[信标间隔](../../components/broker/beacon-interval.md)的每个整数倍处都有一个信标，默认间隔为 1 MiB。信标为每个流键记录最后一条消息的消息头和累计校验和，因此它起到文件内索引的作用。读取者落在文件中间时，会对齐到下一个信标；定位也借助信标缩小查找范围。跨越间隔的消息会被拆开，信标位于它的两部分之间。

[`end_with_eos`](../../components/broker/end-with-eos.md) 在关闭时写入一条流结束消息，使用客户端的内部键，载荷为 `EOS`。实时订阅在这里结束，重放在这里完成。

文件还没有任何内容时，Broker 在连接时用键 `ruststream-internal` 写入一条消息。客户端无法在没有内容的文件上打开实时消费者，而且没有订阅读取这个键。

流键长 1 到 249 个字符：ASCII 字母和数字、`.`、`_` 和 `-`。文件的大小没有上限，除非用 [`size_limit`](../../components/broker/size-limit.md) 设定一个。

这一格式是 `sea-streamer-file` 自己的格式（[文件格式](https://github.com/SeaQL/sea-streamer/blob/main/sea-streamer-file/src/format.rs)、[客户端](https://docs.rs/sea-streamer-file/1.0.0-rc.2/sea_streamer_file/)）。客户端如何读取文件，见[读取者](reader.md)；如何写入文件，见[写入者](writer.md)。
