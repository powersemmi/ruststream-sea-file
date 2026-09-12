# ruststream-sea-file

**`ruststream-sea-file`** 是 [RustStream](https://powersemmi.github.io/ruststream/) 消息框架的文件
与 stdio 传输，建立在 [`sea-streamer-file`](https://docs.rs/sea-streamer-file) 和
[`sea-streamer-stdio`](https://docs.rs/sea-streamer-stdio) 之上。这里的 Broker 是磁盘上单个 `.ss`
流文件里的一份日志，或者进程自己的标准输入和标准输出。两者都不需要服务器。

流文件上的订阅者可以把日志倒回去重读。这就是框架的 `Seekable` 能力，而这个 crate 是它的参考实现。
标准输入输出上的服务，则是 shell 管道里的一环。

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sea-file = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-sea-file/examples/file_service.rs:app"
```

## 接下来读什么 { #where-to-go-next }

<div class="grid cards" markdown>

- :material-file-document-outline: **[文件与 stdio 指南](file.md)** - 流文件、重放、定位、消息头、管道和测试。
- :material-book-open-variant: **[RustStream 文档](https://powersemmi.github.io/ruststream/)** - 框架本身：订阅者、路由、编解码器、中间件和 CLI。
- :material-language-rust: **[API 参考](https://docs.rs/ruststream-sea-file)** - 该 crate 在 docs.rs 上的 rustdoc。

</div>

## 本站点与 RustStream 文档的关系 { #how-this-site-relates-to-the-ruststream-docs }

本站点只介绍文件与 stdio 传输。适用于每个 Broker 的框架概念（编写订阅者、发布、路由、编解码器、
中间件、可观测性和 CLI）在 [RustStream 文档](https://powersemmi.github.io/ruststream/)里。
