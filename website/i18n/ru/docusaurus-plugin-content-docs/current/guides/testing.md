---
title: Тестирование сервиса на файле или в конвейере
sidebar_position: 6
description: "Тестирование сервиса на файле потока или в конвейере внутри процесса с TestApp: рабочее приложение, ответы, задержки и тот же тест на настоящем файле потока."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Тестирование сервиса на файле или в конвейере

Тест запускает собственное приложение сервиса через [`TestApp`](api:ruststream::testing::TestApp) на [внутрипроцессном транспорте](../components/testing/index.md) или на настоящем файле потока.

Тестируемый сервис оформляет заказы.
Заказ приходит под ключом `orders`, и обработчик отвечает квитанцией под ключом `receipts`.
Заказ без суммы отбрасывается.
Заказ сверх лимита проверки на мошенничество откладывается: обработчик просит доставить его снова через минуту с [`retry_after`](../components/settlement/retry-after.md).
После трёх доставок заказ уходит под ключ `orders.review`, [ключ dead-letter](../components/settlement/dead-letters.md).
Приложение строит `service(file)`, поэтому тест выбирает файл сам, а `main` берёт рабочий.
Сервис взят из самого файла примера, [`examples/guide/testing.rs`](https://github.com/powersemmi/ruststream-sea-file/blob/main/crates/ruststream-sea-file/examples/guide/testing.rs).
Тестам нужна фича `testing` крейта `ruststream-sea-file` в `[dev-dependencies]`.

## Заказ и его квитанция {/* #an-order-and-its-receipt */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#in_process"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#in_process"
```

</TabItem>
</Tabs>

[`TestApp::start`](api:ruststream::testing::TestApp::start) подключает [`FileBroker`](../components/broker/file-broker.md) к журналу в памяти.
Файл по пути никогда не открывается, поэтому тесты работают параллельно в одном процессе.
Тест обращается к брокеру по его рабочему типу: `tb.broker::<FileBroker>()`.
`publish` возвращает управление, когда обработчик и его ответ завершены.
Поэтому следующие строки проверяют уже готовое состояние: как подписчик завершил доставку и что дошло до `receipts`.
Тело теста принимает запущенное приложение аргументом, поэтому его же выполняет и живой тест ниже.

## Заказ без суммы {/* #an-order-without-an-amount */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#dropped"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#dropped"
```

</TabItem>
</Tabs>

`settled` проверяет исход, который вернул обработчик.
`assert_not_called` проверяет, что квитанция не дописана.

## Задержка без ожидания {/* #a-delay-without-waiting-for-it */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#held"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#held"
```

</TabItem>
</Tabs>

Тест начинается с остановленными часами, а [`advance`](api:ruststream::testing::TestApp::advance) сдвигает их вперёд.
Минута `HOLD` не стоит ничего.
По окончании задержки `retry_after` дописывает копию заказа под ключом `orders`.
Число попыток хранится в заголовке копии, и `published` показывает её под `orders` рядом с собственным заказом теста.
Третья доставка - последняя, которую допускает точка монтирования, поэтому заказ уходит под `orders.review` в том виде, в каком пришёл.
Каждая отложенная доставка записывается как `Outcome::Nack`.

## Что моделируют внутрипроцессные транспорты {/* #what-the-in-process-transports-model */}

Файл потока внутри процесса - это журнал в памяти. Он новый для каждого значения брокера и общий для его клонов.
Он хранит то же, что хранит файл:

- Каждый ключ потока нумеруется с единицы, и живая подписка начинает с конца журнала.
- [Воспроизведение](../components/subscriptions/replay.md) читает то, что хранит журнал, и завершается в его конце.
- [Позиции](../components/positions/index.md) и [перемотка](../components/positions/seek.md) разрешаются по журналу с теми же отказами, что и на файле.
- [`end_with_eos`](../components/broker/end-with-eos.md) при завершении работы заканчивает каждую подписку.

Журнал, который сервис читает при запуске, записывается через клон его брокера до `TestApp::start`, как это делают тесты руководства [Возобновление и перемотка](resume-and-seek.md).

Канал внутри процесса - это стандартный ввод, который подаёт тест, и стандартный вывод, в который пишет сервис.
`tb.broker::<StdioBroker>().message(..)` - это строка, пришедшая под своим ключом, а `published::<T>(key)` читает строку, которую записала ступень.
Ступень читает собственный вывод только при [`loopback`](../components/broker/loopback.md), как и в настоящем канале.
Оба транспорта отвергают то же, что и настоящие: ключ потока, который клиент не принимает, пустую строку в канале, интервал маяков, не кратный 1024.

Байты файла `.ss`, то, что файл сохраняет между запусками, а также формат строки и конец ввода в настоящем канале видны только на настоящем транспорте.

## Тот же тест на файле потока {/* #the-same-test-on-a-stream-file */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/testing.rs#live"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/testing.rs#live"
```

</TabItem>
</Tabs>

[`TestApp::start_live`](../components/testing/live.md) запускает то же приложение на настоящем файле потока во временном каталоге.
Транспорт - это локальная файловая система, поэтому живому тесту не нужен сервер, и он выполняется вместе с остальными в `cargo test`.
Файл называется по процессу, поэтому тест не читает ничего, что дописал прошлый прогон, а в конце тест удаляет файл.
Проверки те же, что и внутри процесса, без изменений.
Ступень конвейера проверяется вживую как дочерний процесс с перенаправленными стандартными вводом и выводом, как показывает руководство [Конвейеры оболочки](pipelines.md#testing-the-stage).
