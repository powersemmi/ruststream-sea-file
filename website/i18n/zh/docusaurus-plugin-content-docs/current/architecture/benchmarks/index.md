---
title: 基准测试
description: "ruststream-sea-file 在每条消息上的开销：与裸 sea-streamer-file 客户端对照的吞吐量，以及它自身代码的指令数和内存分配次数。"
---

import {Environment, MovedSections, Results} from '@site/src/components/Benchmarks';
import {seaFile} from '@site/src/components/Benchmarks/seaFile';

export const labels = {"loading": "正在读取结果...", "scenario": "场景", "raw": "裸客户端", "adapter": "Broker crate", "framework": "RustStream", "overhead": "额外开销", "against": "（比客户端 {percent}）", "indistinguishable": "无法区分", "brokerBound": "受存储限制", "measured": "测量于", "details": "这些数字是怎样测得的", "pending": "尚未公布结果：{brokers}。", "instructions": "指令数", "allocations": "内存分配", "cold": "冷启动", "table": "以表格显示数字", "axis": "各循环与裸客户端最佳一轮对照：零点右侧更慢，左侧更快", "rounds": "各轮，从最佳到最慢", "best": "最佳一轮", "median": "中位一轮", "noise": "轮次的波动范围", "comparison": "与裸客户端对照", "code": "代码的开销", "broker": "传输", "plugin": "{plugin} 插件", "pluginRaw": "不用插件", "pluginAdapter": "手写的插件", "pluginFramework": "crate 自带的插件", "pluginAxis": "各循环比不用插件的应用最佳一轮低多少", "pluginAgainst": "（比不用插件 {percent}）", "pluginOverhead": "比手写插件", "pluginOverHand": "（比手写插件 {percent}）"};

export const machine = {"cpu": "处理器", "architecture": "架构", "cpu_frequency": "频率", "cores": "核心数", "memory": "内存", "memory_speed": "内存速度", "os": "操作系统", "broker": "Broker", "filesystem": "文件系统", "file_size": "流文件大小", "page_cache": "页缓存", "round_trip": "一次追加", "rustc": "编译器", "profile": "构建配置", "features": "特性", "rustflags": "RUSTFLAGS", "valgrind": "Valgrind"};

# 基准测试

`ruststream-sea-file` 在每条消息上的开销：与裸 `sea-streamer-file` 客户端对照的吞吐量，以及它自身代码的指令数和内存分配次数。

## 与裸客户端对照 {/* #against-a-raw-client */}

第一张图在本机磁盘上的一个流文件上，把两个场景各用三种方式运行。`裸客户端` 直接驱动 `sea-streamer-file` 的 streamer、消费者和生产者。`Broker crate` 用一个自己的循环驱动本 crate 的订阅和发布者，上面没有运行时。`RustStream` 是用户写的服务：应用里的一个 `#[subscriber]` 处理器。第一项差值是本 crate 比它所封装的客户端多付出的开销，第二项是运行时在其上增加的开销。

每个循环画成一条横条，从最佳一轮延伸到最慢的一轮，圆点标出最佳一轮，缺口标出中位一轮。坐标轴的零点是裸客户端的最佳一轮：圆点在零点右侧表示更慢，在左侧表示更快。空心圆点表示该循环与裸客户端的差异小于各轮的波动范围。图表下方的表格列出最佳一轮，括号里是中位数。

两个场景对应读取[流文件](../transports/index.md)的两种方式。重放读取一个在订阅打开之前就已完整写好的文件，没有任何东西限制它的节奏。实时跟随读取一个仍在追加的文件，每次投递都要等待一次追加。

在重放这一行，两层都比裸客户端快。本 crate 的[订阅在单独的任务上预读](../path.md)，因此在处理当前消息的同时，下一条消息已经从文件中读出。手写的裸循环在同一个任务里读取和处理。

## 代码的开销 {/* #cost-of-the-code */}

第二张图统计一条消息在服务线程上的开销：稳态下 callgrind 测得的指令数和 DHAT 测得的内存分配次数。每个场景都是一个基于 [`FileBroker`](../../components/broker/file-broker.md)、使用自己的流文件的服务。计数包括框架的分发、本 crate 的代码，以及在该线程上运行的那部分客户端代码。客户端的写入者，以及在 tokio 阻塞线程上进行的文件读写，不计入。`冷启动` 是启动服务并取得第一条消息一次性付出的开销。

## 结果 {/* #results */}

<Results entry={seaFile} labels={labels} details="/architecture/benchmarks/methodology/" />

## 机器 {/* #the-machine */}

<Environment entry={seaFile} labels={machine} />

传输是一个文件，因此文件系统取代了 Broker 的服务器，这些数字既属于本 crate，也同样属于文件系统。`一次追加` 是单独测得的一次追加并刷写到文件的时间。

数字来自本版本的 `docs/benchmarks/results.json`，由基准测试命令写入。[方法论](methodology.md)说明它们是怎样测得的，[数字](numbers.md)说明可以从中得出什么结论。

<MovedSections />
