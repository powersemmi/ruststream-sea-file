# ruststream-sea-file

**`ruststream-sea-file`** - файловый и stdio-транспорт для фреймворка обмена сообщениями
[RustStream](https://powersemmi.github.io/ruststream/), построенный на
[`sea-streamer-file`](https://docs.rs/sea-streamer-file) и
[`sea-streamer-stdio`](https://docs.rs/sea-streamer-stdio). Брокер здесь - журнал в одном файле
потока `.ss` на диске или собственные стандартные ввод и вывод процесса. Ни тому, ни другому не
нужен сервер.

Подписчик на файле потока перематывает журнал назад. Это трейт-совместимость `Seekable`
фреймворка, а этот крейт - её эталонная реализация. Сервис на стандартных вводе и выводе - ступень
конвейера оболочки.

```toml
ruststream = { version = "0.7", features = ["macros", "json"] }
ruststream-sea-file = "0.7"
serde = { version = "1", features = ["derive"] }
```

```rust
--8<-- "crates/ruststream-sea-file/examples/file_service.rs:app"
```

## Куда идти дальше {#where-to-go-next}

<div class="grid cards" markdown>

- :material-file-document-outline: **[Руководство по файлам и stdio](file.md)** - файлы потоков, воспроизведение, перемотка, заголовки, конвейеры и тестирование.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: подписчики, маршрутизация, кодеки, middleware, CLI.
- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-sea-file)** - rustdoc крейта на docs.rs.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Этот сайт документирует только файловый и stdio-транспорт. Концепции фреймворка, общие для любого
брокера (написание подписчиков, публикация, маршрутизация, кодеки, middleware, наблюдаемость,
CLI), описаны в [документации RustStream](https://powersemmi.github.io/ruststream/).
