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

Подписка - это ключ потока: `FileStream::new("orders")` на файле потока и просто имя на
стандартном вводе. Файл потока воспроизводит записанное и перематывается по требованию, пакеты
собираются на стороне клиента, а отложенная повторная доставка приходит в файл потока под тем
ключом потока, который читает подписка. Ни один транспорт не подтверждает доставку, поэтому
сервис продолжает чтение с позиции, которую сохранил сам. Собственный справочник крейта - на
docs.rs:

<div class="grid cards" markdown>

- :material-file-document-outline: **[Файл потока](https://docs.rs/ruststream-sea-file/latest/ruststream_sea_file/file/index.html)** - дескрипторы, воспроизведение, позиции и перемотка, пакеты, публикация, отложенная повторная доставка.
- :material-console: **[Конвейер](https://docs.rs/ruststream-sea-file/latest/ruststream_sea_file/stdio/index.html)** - формат строки, куда уходит отложенная копия, замыкание вывода на ввод.
- :material-test-tube: **[Тестирование](https://docs.rs/ruststream-sea-file/latest/ruststream_sea_file/index.html#testing)** - рабочее приложение под `TestApp`: внутри процесса или на настоящем файле.
- :material-book-open-variant: **[Документация RustStream](https://powersemmi.github.io/ruststream/)** - сам фреймворк: установка, учебник, список брокеров.
- :material-language-rust: **[Справочник API](https://docs.rs/ruststream-sea-file)** - rustdoc крейта на docs.rs.

</div>

## Как этот сайт связан с документацией RustStream {#how-this-site-relates-to-the-ruststream-docs}

Эта страница - вход в файловый и stdio-транспорт, а всё остальное о них лежит в
[rustdoc крейта](https://docs.rs/ruststream-sea-file). Концепции фреймворка, общие для любого
брокера (написание подписчиков, публикация, маршрутизация, кодеки, middleware, наблюдаемость,
CLI), описаны в
[rustdoc RustStream](https://docs.rs/ruststream/latest/ruststream/runtime/index.html), а входные
страницы самого фреймворка - на
[сайте RustStream](https://powersemmi.github.io/ruststream/).
