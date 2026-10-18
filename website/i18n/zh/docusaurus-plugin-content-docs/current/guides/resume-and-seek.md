---
title: 恢复与定位
sidebar_position: 3
description: "用保存的序号让流文件服务从上次运行停下的地方继续，从某个时刻开始读取，以及移动正在运行的订阅。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 恢复与定位

本指南让服务从上次运行停下的地方继续，并把正在运行的订阅移到文件中的另一个位置。

流文件不记录读取者读到了哪里。
在这种传输上，`ack` 和 `nack` 报告 `Unsupported`（[确认](../components/settlement/index.md)），所以重启后的订阅从挂载点指定的位置打开。
不能把一个订单处理两次的服务，要自己保存检查点。

示例是一个开票服务，它读取商店的订单，并在多次重启之间为每个订单只开一次发票。

## 检查点 {/* #the-checkpoint */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/checkpoint.rs" reference="examples/guide/resume.rs#checkpoint"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/checkpoint.rs" reference="examples/manual/guide/resume.rs#checkpoint"
```

</TabItem>
</Tabs>

检查点是服务最后开票的订单的序号，保存在一个单独的文件中。
`Invoicing` 是应用的状态，处理器从中取得检查点。
服务已有的任何存储都可以同样使用：数据库中的一行、缓存中的一个键。

## 从检查点继续 {/* #resuming-at-the-checkpoint */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/resume.rs#service"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/resume.rs#service"
```

</TabItem>
</Tabs>

`invoicing` 在服务启动之前读取检查点，并把它传给挂载点上的 [`start_at`](../components/positions/start-at.md)。
第一次运行没有检查点，从头读取文件。
[`FilePosition::sequence(n)`](../components/positions/index.md) 投递第 `n` 条消息本身，所以从保存的序号打开的运行，会再次得到上次运行最后处理的订单。
`invoice` 用 [`Ctx<Position>`](../components/positions/position.md) 读取投递的序号，丢弃序号不大于检查点的订单。
其余每个订单都开出发票，它的序号成为新的检查点。

检查点在开票之后写入。
如果一次运行在这两步之间停止，下次启动会为该订单再开一次发票，所以开票本身必须能承受重复。
先写检查点则是相反的取舍：订单永远不会开两次发票，但可能丢失一个。

[`FilePosition::timestamp(ms)`](api:ruststream_sea_file::FilePosition::timestamp) 从某个时刻之后写入的第一条消息打开，时刻以自 Unix 纪元起的毫秒数表示。
它适合从某个时间点而不是从保存的序号重建视图的服务。

## 移动正在运行的订阅 {/* #moving-a-running-subscription */}

处理器用 [`Ctx<SeekHandle>`](../components/positions/seek.md) 移动它自己的订阅。
`seek(FilePosition::end())` 跳过积压，从末端继续；`seek(FilePosition::sequence(n))` 回到第 `n` 条消息，或者向前越过一段已知会失败的消息。
从旧位置已排入队列的投递被丢弃，下一次投递来自新位置。
定位到文件下一个写入序号之后的序号会被拒绝并返回错误：实时订阅随之结束，[重放](../components/subscriptions/replay.md)则留在原处。
批处理器通过 [`FileBatchContext`](../components/positions/batch-context.md) 拿到同一个句柄。

## 测试恢复 {/* #testing-the-resume */}

进程内测试在服务启动之前记录五个订单，并保存检查点三。
服务读取三号到五号订单，丢弃第三个，为另外两个开票。
重启测试用 [`TestApp::start_live`](../components/testing/live.md) 在一个真实文件上运行服务两次：第二次运行在第一次结束时的订单处打开，并丢弃它。
[测试文件或管道服务](testing.md)讲解这两种模式。
