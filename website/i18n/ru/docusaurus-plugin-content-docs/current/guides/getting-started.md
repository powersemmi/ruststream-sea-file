---
title: Начало работы
sidebar_position: 1
description: "Установка ruststream-sea-file, первый сервис на файле потока, просмотр содержимого файла, тест сервиса и тот же обработчик в конвейере."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Начало работы

Это руководство устанавливает `ruststream-sea-file`, запускает первый сервис на файле потока и переносит тот же обработчик в конвейер оболочки.
Ни одному из транспортов не нужен сервер.

## Установка {/* #installation */}

```toml title="Cargo.toml"
[dependencies]
ruststream = { version = "0.7", features = ["macros", "json", "logging"] }
ruststream-sea-file = "0.7"
serde = { version = "1", features = ["derive"] }
tracing = "0.1"

[dev-dependencies]
ruststream-sea-file = { version = "0.7", features = ["testing"] }
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

`ruststream` - это сам фреймворк. Фича `macros` даёт атрибуты, `json` - кодек по умолчанию, а `logging` выводит журнал сервиса в терминал.
`ruststream-sea-file` содержит оба транспорта.
Его фича `testing` добавляет [внутрипроцессные транспорты](../components/testing/index.md), на которых работают тесты, а `asyncapi` добавляет вклад крейта в [документ AsyncAPI](../components/asyncapi/index.md).
Обе фичи по умолчанию выключены.
Клиент файла потока собирается только в Unix-системах, поэтому в Windows крейт не собирается.

## Первый сервис {/* #the-first-service */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started.rs#service"
```

</TabItem>
</Tabs>

`take_order` читает заказы в JSON под ключом потока `orders` из файла `/tmp/orders.ss`.
Заказ с суммой он подтверждает, а заказ без суммы отбрасывает.
[`FileBroker::new`](../components/broker/file-broker.md) только запоминает путь.
Файл открывается при запуске сервиса, и если его нет, запуск его создаёт.
[`FileStream::new`](../components/subscriptions/file-stream.md) называет ключ потока, а [`start_at(FilePosition::beginning())`](../components/positions/start-at.md) читает ключ с начала файла, а не с его конца.
`#[ruststream::app]` превращает `app` в `main`. В ручной форме то же делает [`run_main`](api:ruststream::runtime::cli::run_main).

Запустите сервис:

```bash
cargo run -- run
```

Он читает все заказы, которые есть в файле, и ждёт следующего.
Другой сервис дописывает заказ, публикуя его под ключом `orders` в тот же файл, как показывает руководство [Журнал событий на диске](event-log.md).
<kbd>Ctrl</kbd>+<kbd>C</kbd> останавливает сервис.

## Содержимое файла {/* #inside-the-file */}

В составе файлового клиента есть декодер, который печатает файл потока по сообщению на строку:

```bash
cargo install sea-streamer-file --features=executables --bin ss-decode
ss-decode --file /tmp/orders.ss
```

Каждая строка имеет вид `[timestamp | stream key | sequence | shard] payload`, например `[2026-10-18T09:12:03.120 | orders | 1 | 0] {"id":1,"amount_cents":1250}`.
Один файл хранит все ключи потока, и каждый ключ нумеруется с единицы.
Новый файл хранит ещё одно сообщение под ключом `ruststream-internal`: его записывает крейт, когда создаёт файл.

## Первый тест {/* #the-first-test */}

В файле примера есть и два теста.
Каждый запускает то же `app` через `TestApp::start`, публикует заказ и проверяет, как `take_order` завершил его доставку.
Там `FileBroker` работает внутри процесса, поэтому тесты не трогают ни одного файла:

```bash
cargo test
```

Руководство [Тестирование сервиса на файле или в конвейере](testing.md) разбирает обвязку и то, что моделируют внутрипроцессные транспорты.

## Тот же обработчик в конвейере {/* #the-same-handler-in-a-pipeline */}

Второй транспорт читает стандартный ввод и пишет в стандартный вывод.
Обработчик остаётся прежним. Меняются прелюдия, брокер и подписка.

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/getting_started_pipe.rs#service"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/getting_started_pipe.rs#service"
```

</TabItem>
</Tabs>

[`StdioBroker::new`](../components/broker/stdio-broker.md) не принимает настроек, а подписка называет только ключ потока.
Ступень называет ключ, под который уходят её отложенные копии, через [`out_retry(Publish).to(..)`](../components/settlement/out-retry.md), и без него она не запускается.
Политика публикации в обеих прелюдиях называется `Publish`, поэтому код, который публикует, выглядит одинаково на любом транспорте.

Подайте ступени строку:

```bash
echo '[orders] {"id":1,"amount_cents":1250}' | cargo run -- run
```

Квадратные скобки называют ключ потока.
Журнал уходит в стандартный поток ошибок, потому что стандартный вывод передаёт сообщения ступени следующему процессу.
Формат строки и цепочки ступеней разбирает руководство [Конвейеры оболочки](pipelines.md).

## Переход с sea-streamer {/* #coming-from-sea-streamer */}

Сервис на `sea-streamer-file` сам пишет цикл консьюмера и продюсер.
Вот куда переходит каждая часть в RustStream:

| sea-streamer | RustStream |
|---|---|
| `FileStreamer::connect` с `FileConnectOptions` | [`FileBroker::new`](../components/broker/file-broker.md) и его настройки, которые применяются при запуске |
| `create_consumer` и цикл `next()` | обработчик `#[subscriber]` на [`FileStream`](../components/subscriptions/file-stream.md) |
| декодирование полезной нагрузки сообщения | тип параметра обработчика, который декодирует кодек |
| `create_producer` и `send_to` | ответ или параметр `Out`, отправленный через [`Publish`](../components/publishing/file-publish.md) |
| `set_auto_stream_reset` в параметрах консьюмера, затем `seek_to` | [`start_at`](../components/positions/start-at.md) в точке монтирования или [дескриптор перемотки](../components/positions/seek.md) в обработчике ([Возобновление и перемотка](resume-and-seek.md)) |
| `StdioStreamer` | [`StdioBroker`](../components/broker/stdio-broker.md) |
