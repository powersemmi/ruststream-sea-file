---
slug: /
sidebar_position: 0
description: "使用 RustStream 的文件与 stdio 传输 ruststream-sea-file，在流文件或 shell 管道上构建服务的指南。"
---

# 指南

`ruststream-sea-file` 是 [RustStream](https://powersemmi.github.io/ruststream/latest/zh/) 的文件与
stdio 传输，基于 [`sea-streamer-file`](https://docs.rs/sea-streamer-file) 和
[`sea-streamer-stdio`](https://docs.rs/sea-streamer-stdio) 构建。它有两种传输。流文件是磁盘上单个
`.ss` 文件里的一份日志：它在重启后依然保留，订阅者可以重放它，或者回到更早的位置。标准输入和标准输出
则让服务成为 shell 管道里的一环。两者都不需要服务器。
