---
title: 重试与死信
sidebar_position: 5
description: "在流文件或管道上稍后重试一条消息，限定投递次数，并把一直失败的消息转入死信流键。"
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# 重试与死信

本指南稍后重试一条消息，限定它的投递次数，并把一直失败的消息转入[死信键](../components/settlement/dead-letters.md)。

两种传输都既不扣留消息，也不统计投递次数。
这两件事都由框架来做：之后的投递是运行时在延迟过去后发布的一份副本，次数记在这份副本的消息头里。

示例是一个发货服务，它在订单付款到账后发货。

## 订阅者 {/* #the-subscriber */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#ship"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#ship"
```

</TabItem>
</Tabs>

`Payments` 是服务访问支付服务的客户端，保存在应用的状态中。
订单的付款尚未到账时，`ship` 以 [`retry_after`](../components/settlement/retry-after.md) 作答，服务在两秒后再次询问。

`retry_after` 结算这次投递，等待延迟过去，然后把订单的副本追加到订阅读取的键下。
订阅把副本当作一条新消息读取，排在文件中已有的全部内容之后。
因此，排在等待中的订单后面的订单不会被耽搁。
副本带着重试次数消息头（[`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)），其值加一。
延迟结束之前，副本保存在服务的内存中，所以在此之前停止的进程会丢失它。
[重放](../components/subscriptions/replay.md)只有在尚未读到文件末尾时才会读到副本。

## 上限与死信键 {/* #the-cap-and-the-dead-letter-key */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](../components/settlement/retries.md) 统计一个订单的每次投递，第一次也算在内。
第三次投递不再重试：[`dead_letter`](api:ruststream::runtime::Mounting::dead_letter) 把它追加到 `orders.dead` 下，载荷和消息头保持到达时的样子。
在流文件上，死信键是同一文件中的又一个流键。
只设上限而没有死信键时，用完尝试次数的订单不会写到任何地方。

## 读取死信 {/* #reading-the-dead-letters */}

<Tabs groupId="api">
<TabItem value="macros" label="宏">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="手写">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`review` 像读取其他键一样读取 `orders.dead`。
死信的重试次数消息头记录该订单得到的投递次数，这里是 3。
之后才检查死信的运维人员改用[重放](../components/subscriptions/replay.md)读取它们。

## 管道上的重试 {/* #retries-on-a-pipe */}

一级的标准输出到达下一个进程，而从不回到它自己的输入，所以副本无法回到延迟它的那一级。
在 [`StdioBroker`](../components/broker/stdio-broker.md) 上，挂载点用 `b.include(ship).out_retry(Publish).to("orders.retry")` 指定副本发往的键，由后面的某一级读取（[out_retry](../components/settlement/out-retry.md)）。
[快速开始](getting-started.md#the-same-handler-in-a-pipeline)中的管道级就是这样挂载的。

没有指定键的一级不会启动，因为任何处理器都可能以 `retry_after` 作答。
上限和死信键在管道上与在文件上的作用相同；死信是其键下的一行，供下游的一级读取。

## 测试延迟 {/* #testing-the-delay */}

示例文件中的测试在暂停的时钟上于进程内运行应用，所以 `tb.advance(RECHECK)` 让两秒立刻过去。
一个测试在两次投递之间让付款到账，订单在第二次投递时发货。
死信测试既在进程内运行，也用 `TestApp::start_live` 在真实流文件上运行，后者的延迟按真实时钟过去。
[测试文件或管道服务](testing.md)讲解测试套件。
