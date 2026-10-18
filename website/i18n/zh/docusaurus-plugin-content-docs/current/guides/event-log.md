---
title: 磁盘上的事件日志
sidebar_position: 2
description: "把服务的事件记录到流文件中，从另一个订阅实时跟随它们，并一次读完已结束的文件做汇总。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 磁盘上的事件日志

本指南把一家商店的事件记录到流文件中，在写入的同时跟随读取，并一次读完已结束的文件做汇总。

流文件是一份日志：发布把消息追加到某个流键下，该键的每个读取者都按顺序看到它。
示例是一家商店，它为每个订单记录一张收据；还有一个审计作业，它汇总已结束文件中的收据。

## 记录与跟随 {/* #recording-and-following */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/event_log.rs#record"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/event_log.rs#record"
```

</TabItem>
</Tabs>

`checkout` 用一张 `Receipt` 回复每个订单，回复追加到同一个文件中，位于该类型声明的键 `receipts` 下。
一个文件容纳所有流键，所以路径只在 `FileBroker::new` 中指定一次。
发布追加消息并刷新到文件，因此调用返回之前，字节已经交给操作系统。
Broker [关闭](../components/broker/shutdown.md)时，文件同步到设备。

`notify` 从文件末尾跟随 `receipts`，在 `checkout` 追加每张收据时读取它。
打开同一路径的另一个进程也以同样的方式跟随该键。
没有指定[位置](../components/positions/start-at.md)的订阅从末尾开始，所以重启后的 `notify` 只读取它启动之后写入的收据。

每个流键的序号从一开始。
收据保存订单的序号，处理器通过 [`Ctx<Position>`](../components/positions/position.md) 得到它，读取者之后可以凭它[回到](resume-and-seek.md)那个订单。
之后向同一文件追加内容的运行会延续编号：重启后，下一个订单是三号，而不是一号。

文件客户端把流文件的大小上限定为 16 GiB。
达到上限的发布会返回错误，该连接之后的每次发布也都会返回错误。
[在生产环境中运行](production.md)说明这对长期使用的文件意味着什么。

## 一次读完的作业 {/* #a-one-pass-job */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/bin/audit.rs" reference="examples/guide/event_log.rs#audit"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/bin/audit.rs" reference="examples/manual/guide/event_log.rs#audit"
```

</TabItem>
</Tabs>

[`replay()`](../components/subscriptions/replay.md) 从第一条消息开始读取文件中 `receipts` 下的内容，读到文件末尾即完成。
在那之后追加的收据不会被读取，所以作业读完一遍就无事可等。
[`existing_only`](../components/broker/existing-only.md) 让启动在文件不存在时返回错误，这样，拼错的路径会让作业停止，而不是创建一个空文件。

`total` 接收一个切片，挂载点上的 [`batch(nonzero!(64))`](../components/subscriptions/batches.md) 把一批的上限定为 64 张收据。
一批取走文件中已有的内容，不会等待凑满；一个结果结算整批。

写入者用 [`end_with_eos`](../components/broker/end-with-eos.md) 把文件标记为已结束，它在关闭时追加一个流结束标记。
重放读到该标记即完成，`notify` 这样的实时订阅则在该标记处结束。

## 测试日志 {/* #testing-the-log */}

示例文件中的测试在进程内运行商店和审计：订单得到收据，`notify` 读到它；审计在一批中读到三张已记录的收据，并忽略第四张。
审计启动之前，它的收据记录在其 Broker 的一个克隆上，就像商店留下的那样。
重启测试用 `TestApp::start_live` 在一个真实文件上运行商店两次，并检查第二次运行把它的订单编为三号。
[测试文件或管道服务](testing.md)讲解这两种模式。
