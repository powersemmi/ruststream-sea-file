---
title: Повторы и ключ dead-letter
sidebar_position: 5
description: "Повторная обработка сообщения позже на файле потока или в канале, предел доставок и ключ потока dead-letter для того, что раз за разом не обрабатывается."
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# Повторы и ключ dead-letter

Это руководство повторяет обработку сообщения позже, ограничивает число его доставок и переносит то, что раз за разом не обрабатывается, под [ключ dead-letter](../components/settlement/dead-letters.md).

Ни один из транспортов не придерживает сообщения и не считает их доставки.
И то и другое делает фреймворк: следующая доставка - это копия, которую среда выполнения публикует по истечении задержки, а счётчик хранится в заголовке этой копии.

Пример - сервис отгрузки, который отгружает заказ, когда его оплата прошла.

## Подписчик {/* #the-subscriber */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#ship"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#ship"
```

</TabItem>
</Tabs>

`Payments` - клиент сервиса платежей, который хранится в состоянии приложения.
Пока оплата заказа не прошла, `ship` отвечает [`retry_after`](../components/settlement/retry-after.md), и сервис спрашивает снова через две секунды.

`retry_after` завершает доставку, выжидает задержку и дописывает копию заказа под ключом, который читает подписка.
Подписка читает копию как новое сообщение, после всего, что уже было в файле.
Поэтому заказы после ожидающего не задерживаются.
У копии есть заголовок с числом попыток ([`RETRY_COUNT_HEADER`](api:ruststream::runtime::RETRY_COUNT_HEADER)), увеличенным на единицу.
До конца задержки копия ждёт в памяти сервиса, поэтому процесс, который остановился раньше, её теряет.
[Воспроизведение](../components/subscriptions/replay.md) читает копию, только если само ещё не дошло до конца файла.

## Предел и ключ dead-letter {/* #the-cap-and-the-dead-letter-key */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#app"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#app"
```

</TabItem>
</Tabs>

[`max_attempts`](../components/settlement/retries.md) считает все доставки одного заказа, включая первую.
Третья доставка не повторяется: [`dead_letter`](api:ruststream::runtime::Mounting::dead_letter) дописывает её под ключом `orders.dead` с полезной нагрузкой и заголовками в том виде, в каком они пришли.
На файле потока ключ dead-letter - ещё один ключ потока того же файла.
Предел без ключа dead-letter рядом с ним никуда не записывает заказ, у которого кончились попытки.

## Чтение ключа dead-letter {/* #reading-the-dead-letters */}

<Tabs groupId="api">
<TabItem value="macros" label="Макросы">

```rust title="src/main.rs" reference="examples/guide/retries.rs#dead_letters"
```

</TabItem>
<TabItem value="manual" label="Вручную">

```rust title="src/main.rs" reference="examples/manual/guide/retries.rs#dead_letters"
```

</TabItem>
</Tabs>

`review` читает `orders.dead`, как любой другой ключ.
Заголовок с числом попыток у сообщения из ключа dead-letter хранит число доставок, через которые прошёл заказ, здесь 3.
Оператор, который просматривает такие сообщения позже, читает их через [воспроизведение](../components/subscriptions/replay.md).

## Повторы в канале {/* #retries-on-a-pipe */}

Стандартный вывод ступени доходит до следующего процесса и никогда до её собственного ввода, поэтому копия не может вернуться на ступень, которая её отложила.
На [`StdioBroker`](../components/broker/stdio-broker.md) точка монтирования называет ключ, под который уходят копии, `b.include(ship).out_retry(Publish).to("orders.retry")`, и их читает одна из следующих ступеней ([out_retry](../components/settlement/out-retry.md)).
Так смонтирована ступень конвейера из руководства [Начало работы](getting-started.md#the-same-handler-in-a-pipeline).

Ступень, которая не называет ключа, не запускается, потому что любой обработчик может ответить `retry_after`.
Предел и ключ dead-letter работают в канале так же, как на файле. Сообщение dead-letter - это строка под своим ключом для одной из следующих ступеней.

## Проверка задержки {/* #testing-the-delay */}

Тесты в файле примера запускают приложение на внутрипроцессном транспорте с остановленными часами, поэтому `tb.advance(RECHECK)` проматывает две секунды мгновенно.
Один тест проводит оплату между двумя доставками, и заказ отгружается на второй.
Тест dead-letter выполняется и внутри процесса, и на настоящем файле потока через `TestApp::start_live`, где задержки проходят по настоящим часам.
Работу с обвязкой разбирает руководство [Тестирование сервиса на файле или в конвейере](testing.md).
