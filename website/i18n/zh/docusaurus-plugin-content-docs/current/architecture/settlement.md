---
title: 结算
sidebar_position: 6
description: "ack、drop、retry 和 retry_after 在流文件和管道上变成什么；二者既不保存消费者的位置，也不确认任何东西。"
---

import {Diagram} from '@site/src/components/Diagrams';

export const outcomes = {
  "title": "处理器在流文件或管道上的结果：只有运行时发布的副本能把消息带回来",
  "text": "以文字阅读本图",
  "outcome": ["处理器的结果"],
  "ack": ["ack、drop：", "报告", "不受支持，", "记录日志"],
  "retry": ["retry()：", "重新入队", "不受支持，", "记录日志"],
  "later": ["retry_after(d)：", "运行时", "发布", "一份副本"],
  "resume": ["没有任何东西记录服务读到了哪里：", "重启后从它保存的位置继续"]
};

export const retries = {
  "title": "流文件或管道上的一次重试：尝试上限决定发副本、发到死信键，还是什么也不写",
  "text": "以文字阅读本图",
  "outcome": ["retry_after(d)，或在声明了上限时的 retry()", "重试次数消息头说明这是第几次尝试"],
  "cap": ["max_attempts：这次尝试是否低于上限？"],
  "copy": ["低于上限：", "一份副本，", "经过可能的", "延迟"],
  "deadLetter": ["到达上限：", "副本发往", "死信", "键"],
  "dropped": ["到达上限，", "没有死信：", "什么也", "不写"],
  "file": ["文件：同一个", "流键，由该", "订阅读取"],
  "pipe": ["管道：out_retry", "指定的键，", "供下一级读取"],
  "crash": ["延迟的副本保存在进程中：", "计时器触发前崩溃就会丢失它"],
  "retries": "重试",
  "below": "低于上限",
  "atCap": "到达上限",
  "onFile": "文件",
  "onPipe": "管道"
};

# 结算

`ack`、`drop`、`retry` 和 [`retry_after`](../components/settlement/retry-after.md) 在流文件和管道上变成什么。二者既不保存消费者的位置，也不确认任何东西。

<Diagram name="outcomes" words={outcomes} />

两个客户端都不记录读取者处理过什么。因此，投递的 [`ack` 和 `nack`](../components/settlement/index.md) 会立即报告 `AckError::Unsupported`，运行时记录日志。被确认的消息不会在任何地方留下标记，重新入队的消息也不会回来。重启的服务从[它自己保存的位置](../components/positions/start-at.md)继续。

重试是唯一能把消息带回来的结果，而且全部由运行时完成。

## 重试 {/* #retries */}

<Diagram name="retries" words={retries} />

`retry_after` 结果，以及声明了 [`max_attempts`](../components/settlement/retries.md) 或[死信键](../components/settlement/dead-letters.md)的注册上的 `retry()`，走这条路径。两种传输都不统计投递次数，所以尝试次数随副本的重试次数消息头传递，第一次投递算作一次。先检查上限。到达上限时，声明的死信会得到一份副本；没有声明时，这次投递被拒绝，什么也不写。

低于上限时，运行时丢弃这次投递并发布一份副本，`retry_after` 则在延迟之后发布。在流文件上，流键既是来源也是去处：订阅报告它读取的键，副本就追加到这个键下，因此挂载点无需指定任何东西。重放只有在尚未读到文件末尾时才会读到这份副本。

在管道上，副本从标准输出发往下一级，因此挂载点要用 [`out_retry`](../components/settlement/out-retry.md) 指定它的键。管道上没有指定键的注册拒绝启动。
