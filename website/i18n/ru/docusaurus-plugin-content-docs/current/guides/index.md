---
slug: /
sidebar_position: 0
description: "Руководства по сервисам на файле потока и в конвейере оболочки с ruststream-sea-file, файловым и stdio-транспортом RustStream."
---

# Руководства

Крейт `ruststream-sea-file` реализует файловый и stdio-транспорт для
[RustStream](https://powersemmi.github.io/ruststream/latest/ru/). Он построен на
[`sea-streamer-file`](https://docs.rs/sea-streamer-file) и
[`sea-streamer-stdio`](https://docs.rs/sea-streamer-stdio). Транспортов два. Файл потока - это
журнал в одном файле `.ss` на диске: он переживает перезапуск, а подписчик может заново начать его
воспроизведение или вернуться к более ранней позиции. Стандартные ввод и вывод делают сервис
ступенью конвейера оболочки. Ни тому, ни другому не нужен сервер.
