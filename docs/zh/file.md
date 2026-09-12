# 文件与 stdio

`ruststream-sea-file` 让 RustStream 服务完全不需要服务器：跑在磁盘上一个持久、可重放的流文件上，
或者跑在进程自己的标准输入和标准输出上。订阅者、路由、编解码器和中间件都来自框架，
[RustStream 文档](https://powersemmi.github.io/ruststream/)已经讲过；本页讲的是这两种传输。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sea-file = "0.7"
serde = { version = "1", features = ["derive"] }
```

文件传输在 Windows 上无法编译：它底层的文件客户端只支持 Unix。

## 能力 { #capabilities }

这个 crate 实现了框架的哪些可选能力 trait，以及每一项能给你什么：

| 能力 | 是否实现 | 说明 |
| --- | --- | --- |
| `Subscribe` | 是 | 两种传输上，流键都可以直接写成字符串字面量，`#[subscriber("key")]` 不需要描述符。参见[订阅](#subscriptions)。 |
| `Seekable` + `Positioned` | 两种传输都实现 `Positioned`，文件上还实现 `Seekable` | 每次投递都会报出自己所在的序号。流文件上的处理器还能通过 `Position` 和 `SeekHandle` 两个上下文键给自己的订阅重新定位，这个 crate 就是框架里该能力的参考实现。stdio 上的订阅不能定位：标准输入没有可供移动的日志。参见[定位](#seeking)。 |
| `Partitioned` | 否 | 流文件是一份有序日志，没有分片。 |
| `BatchSubscriber` | 是，在客户端一侧 | 在挂载点写上 `batch(n)`，两种传输都会给出最多 `n` 条的批。参见[批](#batches)。 |
| `RequestReply` | 否 | 两种传输都没有响应地址。 |
| `TransactionalPublisher` | 否 | 流文件没有原子的多次写入单元：每次发布各自追加并刷盘。 |
| `OwnedTransactions` | 否 | 同样的原因：没有事务可供拥有。 |
| `DescribeServer` | 是 | 生成的 AsyncAPI 文档写的是一个进程内服务器：带路径的 `file`，或者 `stdio`。 |

两种传输上，`ack` 和 `nack` 都返回 `AckError::Unsupported`：客户端不记录消费者位置，它的可恢复模式
在上游还没有实现。参见[确认](#acknowledgement)。

## 两个 Broker { #the-two-brokers }

每种传输都有自己的模块，里面是该传输的类型、它的 `Publish` 策略和它的 prelude。流文件上的服务以
`use ruststream_sea_file::file::prelude::*;` 开头，管道上的服务以
`use ruststream_sea_file::stdio::prelude::*;` 开头，此外不再从这个 crate 导入任何东西。

同时用到两者的服务，glob 导入 `ruststream_sea_file::prelude`，并写带前缀的 `FilePublish` 和
`StdioPublish`，因为两种形态会争用同一个策略名字。

`FileBroker::new(path)` 记下一个 `.ss` 流文件的路径。`StdioBroker::new()` 什么都不记。打开和关闭
发生在随后的状态转移里：

```text
FileBroker::new(path)      只有配置，同步，没有 I/O
  .connect()   ->  ConnectedFileBroker      打开的文件；订阅和发布者
  .shutdown()  ->  ()                       已刷盘并关闭
```

关闭之前发出的发布者共用该 Broker 的连接：Broker 一旦关闭，它就返回 `SeaFileError::NotConnected`
错误，而不是悄悄报成功。

`FileBroker` 接受三项可选设置，它们都在连接时生效：

- `existing_only()` 要求文件必须已经存在，而不是去创建它。
- `end_with_eos()` 在关闭时写入流结束标记，因此重放这个已完成文件的消费者会结束，而不是继续等
  新数据。
- `beacon_interval(bytes)` 设定文件里的信标相隔多少字节；这个值必须是 1024 的正整数倍。信标汇总
  它之前写入的各个流，文件正是靠它才能定位，因此信标越密，定位越精细，文件也越大。

关闭 stdio Broker 会结束进程内的每一个 stdio 消费者和生产者，不只是这个 Broker 打开的那些。

## 订阅 { #subscriptions }

`FileStream::new(key)` 是文件里某一个流键的订阅描述符。它直接写在 `#[subscriber(..)]` 属性里，
不加任何设置的描述符跟随实时的末尾：

```rust
--8<-- "crates/ruststream-sea-file/examples/file_service.rs:handler"
```

把它挂载到 Broker 上：

```rust
--8<-- "crates/ruststream-sea-file/examples/file_service.rs:app"
```

手写路径接受同一个描述符：`subscriber(FileStream::new("orders"), body)`。

在 stdio Broker 上，订阅就是流键本身：`#[subscriber("jobs")]` 从标准输入消费 `jobs` 这个键。

### 重放模式 { #replay-mode }

`FileStream::new(key).replay()` 从文件开头读起，并在文件末尾结束订阅，而不是跟随实时写入。要一次
性处理一份已录制的日志，就用这个。用 `end_with_eos()` 写出的文件，末尾带着重放会停下来的那个标记。

重放是唯一一种位置表达不了的读取模式。

### 批 { #batches }

接受 `&[T]` 的处理器处理的是一个批：

```rust
--8<-- "crates/ruststream-sea-file/examples/file_batches.rs:batch"
```

批的上限由挂载点给出：

```rust
--8<-- "crates/ruststream-sea-file/examples/file_batches.rs:mount"
```

两个客户端都是一次读一条记录，因此批在客户端一侧组装。一个批装的消息不会超过挂载点给出的大小；
传输当时只有那么多时，就装得更少。

未满的批在第一次投递之后 10 毫秒发出。这个期限在这里是固定的，挂载点改不了。它限定的是一个批在
空闲的末尾要等多久。

## 定位 { #seeking }

流文件上的订阅可以移到保留日志里的任何位置。位置就是 `FilePosition`：

| 位置 | 含义 |
| --- | --- |
| `FilePosition::beginning()` | 保留文件的开头。 |
| `FilePosition::end()` | 流的末端。 |
| `FilePosition::sequence(n)` | 第 `n` 号消息，重新投递时包含它本身。 |
| `FilePosition::timestamp(millis)` | 严格晚于该时刻的第一条消息；`millis` 是从 Unix 纪元起算的毫秒数。 |

从投递上取得的位置是钉住的：定位回到它，会重新投递正是那一条消息，然后按顺序投递日志的其余部分。

属性上的 `start_at(..)` 子句说明订阅从哪里开始，也就是第一次投递之前的位置。运行中的处理器通过
两个上下文键给自己的订阅重新定位：

| 键 | 读到 | 可用于 |
| --- | --- | --- |
| `Position` | 本次投递的 `FilePosition` | `FileContext` |
| `SeekHandle` | 给订阅重新定位的句柄 | `FileContext`、`FileBatchContext` |

处理器用 `Ctx` 提取器把这两个键作为参数取出，完全不必写出上下文类型。

```rust
--8<-- "crates/ruststream-sea-file/examples/file_replay.rs:seek"
```

[批](#batches)的函数体声明 `ctx: &mut Context<'_, FileBatchContext>`，并用
`ctx.context(SeekHandle)` 读出句柄。一个批横跨多次投递，因此这个上下文里没有位置；每个元素自己的
序号在它的 `stream-sequence` 消息头里。

读取这两个键中任何一个的处理器，在 `StdioBroker` 上编译不过：标准输入没有可供移动的日志。

定位会丢弃排在它之前的投递，因此处理器看到的下一条消息来自新位置。能力本身参见框架文档里的
[定位](https://powersemmi.github.io/ruststream/latest/guides/subscribers/#seeking)。

## 发布 { #publishing }

`FilePublish` 是构造发布者 `FilePublisher` 的策略，后者向流文件追加写入；`StdioPublish` 构造
`StdioPublisher`，它向标准输出写出一行行文本。策略由你在注册处理器时指定，启动时它在已连接的
Broker 上实例化出发布者。

每个策略都是自己 Broker 的默认值，因此挂载点没有指定别的策略时，响应就走它。要在挂载点指定另一个
策略，用 `.out`，先写标记再写策略：`b.include(handle).out(Reply, Publish);` 为响应指定策略，注入的
`Out` 槽位也用同样的方式、在自己的标记下接受一个策略。

这个传输上的目的地，是 Broker 内部的一个流键。文件本身只在 `FileBroker::new(path)` 里写一次，
stdio 那种形态则写向标准输出。因此你可以用 `#[outgoing(name = "results")]` 把键声明在响应类型上，
并在订阅者上写不带名字的 `publish` 子句。类型没有声明键的响应，发布到订阅者说的地方：
`publish("results")`。

每种传输的 prelude 都把自己的策略以 `Publish` 这个名字导出，因此挂载点写的是概念本身：在两种形态
之间迁移的服务，改的是 glob，不是策略名字。

文件发布者每次发布都会刷盘，因此实时订阅者，或者同一个文件的外部读取方，在调用返回时就能看到这条
消息。

从启动钩子发布，写的是同一个策略：

```rust
--8<-- "crates/ruststream-sea-file/examples/file_replay.rs:publish"
```

对既没有载荷也没有消息头的消息，stdio 发布者返回 `SeaFileError::Invalid` 错误，因为客户端的按行
格式会悄悄丢掉空行。

### 每条消息的参数 { #per-message-arguments }

处理器通过框架的构建器发布：`publisher.message(&value).publish()`；值自己的
`#[derive(Outgoing)]` 把流键留给调用方时，再加上 `.to(key)`。这里的一条消息只有流键和载荷，而这
两样正是构建器自己的参数。

不透明的字节走同样的路径：作为一个类型已声明自己是序列化完毕的值
（`#[derive(Outgoing, Serialized)] struct Frame(Vec<u8>)`），因此没有编解码器在它们上面运行。

每条消息带一个协议字段的 Broker，也就是优先级、QoS 或者排序键，会为它在那个构建器上加一步。这两
种传输都没有这种字段：向流文件追加的一条记录、标准输出上的一行，装的就是一个键和一份载荷，别的
什么也没有。因此这个 crate 不加任何步骤，发布的处理器函数体只需要框架的 prelude，以及朴素的
`Out<impl Publisher, Marker>` 约束。

## 消息头信封 { #the-header-envelope }

客户端的载荷就是纯字节，没有放消息头的地方，因此消息头写进载荷本身。只有消息带了消息头时，信封
才会出现：

- 不带消息头发布的消息，原样写入。这样录下的文件，任何 `sea-streamer` 消费者都能当作普通的载荷流
  读取；别的工具写出的文件，在这里也照样能读。
- 带消息头的消息，写成 `rs1:` 加上一段 base64，其内容是带长度前缀的消息头块和载荷。这种形式对文本
  安全，因为 stdio 传输是按行的 UTF-8。

不带消息头的非 UTF-8 载荷，stdio 发布者也会给它套上信封，因此二进制数据能完整穿过 shell 管道。

每次投递的序号都放在 `stream-sequence` 消息头（`SEQUENCE_HEADER`）里。

## 确认 { #acknowledgement }

在这个传输上，续读由你显式完成：把从投递上取得的 `FilePosition` 存下来，下一次运行用
`start_at(..)` 打开；或者从头重放这个文件。`ack` 和 `nack` 返回 `AckError::Unsupported`，因此没有
别的东西记录你读到了哪里。

### 延迟之后重试 { #retrying-after-a-delay }

`HandlerOutcome::retry_after(delay)` 在这里没有传输可以依靠，因此框架自己的兜底就是全部机制：它
丢弃这次投递，等延迟走完，再发布一份消息副本，重试计数消息头加一。它用哪个发布者，由你在组合根里
接线：

```rust
--8<-- "crates/ruststream-sea-file/tests/redelivery.rs:retry_via"
```

副本发往该订阅读取的那个流键，因此流文件上的实时订阅能把消息拿回来。重放拿不回来：它读的是文件
当时已有的那一段，读完就结束，之后写入的副本永远到不了它那里。因此在 `FileStream::replay()` 订阅
之上接了 `retry_via` 的作用域起不来，并报出是哪条订阅，而不是在运行期把每条延迟消息都丢掉。stdio
因为同样的原因也起不来：你发布出去的东西流向管道里的下一个进程，而不是回到你自己的标准输入。

没有 `retry_via` 时，`retry_after` 退化成立即重新入队，而在这两种传输上这意味着延迟丢失。

## stdio 管道 { #stdio-pipelines }

`StdioBroker` 让进程成为 shell 管道里的一环：标准输入是订阅，标准输出是发布者，
`producer | service | consumer` 配合普通的命令行工具就能跑起来。

```rust
--8<-- "crates/ruststream-sea-file/examples/stdio_pipeline.rs:pipeline"
```

每行都遵循客户端的 `[timestamp | stream_key | seq] payload` 格式，因此流键是这一行的一部分，一个
进程可以服务多个键：

```text
echo '[2024-01-01T00:00:00 | jobs | 1] {"id":7}' | ./pipeline run
```

`StdioBroker::new().loopback()` 把本进程的标准输出送回它自己的标准输入，因此 stdio 服务可以在一个
进程里跑测试，不需要外部命令。

## 测试 { #testing }

这个 crate 里的每一个测试集都在本地运行，用的是临时文件和进程内管道，不需要启动任何 Broker。

你自己的处理器，用框架的 `TestApp` 测试套件、对着 `testing` feature 里的 `FileTestBroker` 来测。
它在内存里保留一份带位置的日志，而这正是流文件的处理器所依赖的那一条传输性质。

因此做定位的服务挂到它上面完全不用改动：`FileStream` 在这里可以解析，投递会报出 `FilePosition`，
`FileContext` 和 `FileBatchContext` 在它的投递之上构建，和在文件的投递之上构建是一样的。批也一样，
因此批量处理器看到的批，大小就是它的挂载点要的那个。

挂载点也不用改。`Publish` 的两种形态，`FilePublish` 和 `StdioPublish`，都能在这个 Broker 上构造出
发布者，因此路由文件保留它自带的策略，`.out(Reply, Publish)` 在测试套件下和在生产环境里读起来
一模一样。没有什么只在测试套件下用的策略需要替换进来。

```rust
--8<-- "crates/ruststream-sea-file/tests/seek_context.rs:handler"
```

```rust
--8<-- "crates/ruststream-sea-file/tests/seek_context.rs:test"
```

测试套件负责送入输入，把反应一直跑到没有任何消息还在处理中，并记录下发生了什么，因此测试既不用
等待，也不用自备收集器。断言的写法参见
[用 TestApp 对服务做单元测试](https://powersemmi.github.io/ruststream/latest/guides/testing/#unit-testing-a-service-with-testapp)。

相似到什么程度，是量出来的，不是说出来的。框架的契约套件，也就是生命周期阶梯、定位和批量，既跑在
进程内 Broker 上，也跑在真实的流文件上。因此文件遵守、而进程内 Broker 悄悄破坏了的契约，在这个
仓库里就不通过，而不是等到你服务的测试里才暴露。

进程内 Broker 的结算方式和文件一致：两边的 `ack` 和 `nack` 都返回 `AckError::Unsupported`，因此
读取这个答复的处理器，在哪一边的表现都相同。它没有去复现的，恰恰是文件存在的全部理由：文件和
信标、流结束标记、消息头信封，以及时间特性。这些由本仓库自己的测试集对着真实的流文件来验证。
