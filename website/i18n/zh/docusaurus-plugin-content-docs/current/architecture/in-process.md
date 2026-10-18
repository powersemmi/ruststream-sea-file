---
title: 进程内
sidebar_position: 8
description: "在 testing 特性下，已连接的 Broker 可以承载保存在内存中的流文件或管道，测试套件用未经改动的生产应用驱动它。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const words = {
  "title": "一个已连接的 Broker，两种传输：经 connect 的客户端，经 connect_in_process 的内存文件或管道",
  "text": "以文字阅读本图",
  "connected": ["ConnectedFileBroker", "ConnectedStdioBroker"],
  "live": ["客户端：一个 .ss", "文件，或", "进程的管道"],
  "memory": ["保存在内存中的", "文件或管道"],
  "coordinator": ["测试套件为 settle()", "统计在途的", "投递"],
  "liveOnly": ["只在真实传输上：", ".ss 文件的字节、信标、", "重启、同一文件上", "的两个 Broker、没有键", "的行、整个进程", "的关闭"],
  "emulated": ["在内存中：每个键", "一份从一编号的日志、", "末端、重放、定位", "及其拒绝、结束标记、", "loopback、信封"],
  "production": ["不带 testing 特性时，连接只有客户端：", "大小相同，没有第二个分支"],
  "connect": "connect",
  "inProcess": "connect_in_process"
};

# 进程内

在 `testing` 特性下，已连接的 Broker 可以承载保存在内存中的流文件或管道，[测试套件](../components/testing/index.md)用未经改动的生产应用驱动它。

<Diagram name="inProcess" words={words} />

[`TestApp::start`](../components/testing/index.md) 通过 `connect_in_process` 而不是 `connect` 连接每个 Broker。已连接的 Broker、它的订阅、发布者和投递都是同样的类型，各自把内存传输作为自己的一个变体。因此，服务的描述符、发布策略和执行定位的处理器都无需改动即可运行。

内存传输读取生产 Broker 的设置，没有任何自己的设置。消息经过它时，走的步骤与真实消息相同：流键检查、信封，以及管道上对空行的拒绝。所以它拒绝流文件或管道所拒绝的内容。

内存文件为每个流键保存一份日志，从一开始编号。实时订阅从末端开始，重放读取日志并在其末尾完成，定位按照文件的拒绝规则在日志上解析。Broker 的每个克隆共用一个文件，新的 Broker 从一个空文件开始。内存管道把测试注入的内容当作标准输入上的行，并记录服务写出的内容。只有在 [`loopback`](../components/broker/loopback.md) 下，一行才会回到服务自己的订阅。

测试套件统计每种传输上的在途投递，因此每个订阅处理完到达它的内容之后，`tb.settle()` 才返回。属于真实传输的部分在[真实环境](../components/testing/live.md)中测试：在临时目录中的流文件上，或者以子进程方式启动一级管道。

不带 `testing` 特性时，连接只有一个变体。编译器检查它的大小恰好等于它所包装的客户端句柄，因此生产构建既不携带第二种传输，也没有通向它的分支。
