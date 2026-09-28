# Лабораторная — отчёт

Заполняется тобой. Аналог `explain/EXPLAIN.md` из v3: там ты смотрел на планы
запросов, здесь — на поведение распределённой системы под нагрузкой и при отказах.
Всё наблюдается снаружи, через gateway: `lab/load` бьёт в HTTP, остальное — `psql`,
`redis-cli`, Kafka-скрипты из контейнера и кнопки Railway.

Что должно быть в каждом разделе: команда, сырой вывод (в блоках ```` ``` ````),
одна-две фразы, что ты в нём увидел. Вопросы в конце каждого раздела — ответы словами.

Стек для замеров: ☐ локальный compose  ☐ Railway (укажи)

---

## 1. Кэш: hit rate и хвост задержек

Redis работает. Прогрей и нагрузи:

```bash
make load ARGS="-mode get -n 5000 -c 50 -listings 100"
```

Вывод `load` (распределение `200 HIT` / `200 MISS`, p50/p95/p99):

```
(вывод)
```

`redis-cli -u redis://localhost:6380 INFO stats | grep keyspace` до и после:

```
(вывод)
```

Теперь останови Redis (`docker compose -f compose/docker-compose.yml stop redis`,
на Railway — Stop у сервиса redis) и повтори ту же нагрузку:

```
(вывод: должны быть 200 BYPASS и другие перцентили)
```

Верни Redis.

**Вопросы.**
1. Какой hit rate получился на 5000 запросов по 100 ключам с TTL 30 с? Почему не 99%?
2. Насколько изменились p50 и p99 без Redis? Что из этого важнее для пользователя и почему?
3. 100 одновременных GET на один только что инвалидированный ключ — что произойдёт с Postgres? Как называется эффект и как его гасят (single-flight, lock, jitter TTL)?

---

## 2. Outbox: отказ Kafka и at-least-once

Останови Kafka. Создай 300 заказов:

```bash
docker compose -f compose/docker-compose.yml stop kafka
make load ARGS="-mode orders -n 300 -c 20 -stock 1000"
```

Вывод `load` (все 201, задержки такие же, как с живой Kafka?):

```
(вывод)
```

Размер очереди: `psql postgres://app:app@localhost:5433/orders -c "select count(*) from outbox where published_at is null"`:

```
(вывод)
```

Подними Kafka, засеки, за сколько очередь дойдёт до нуля, и сравни `sold` у листинга со `stock`:

```
(вывод: время дренажа, stock/sold)
```

Теперь дубли. Снова 300 заказов при живой Kafka, но убей orders-svc посреди дренажа
жёстко, без SIGTERM:

```bash
make load ARGS="-mode orders -n 300 -c 20 -stock 1000" &
sleep 1; docker compose -f compose/docker-compose.yml kill -s SIGKILL orders-svc
docker compose -f compose/docker-compose.yml start orders-svc
```

Посчитай события в топике и уникальные `order_id`:

```bash
# В native-образе Kafka нет скриптов, берём их из обычного образа в той же сети compose:
docker run --rm --network compose_default apache/kafka:3.9.2 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 --topic order.created --from-beginning --timeout-ms 5000 2>/dev/null \
  | grep -o '"order_id":"[^"]*"' | sort | uniq -c | sort -rn | head
```

```
(вывод: есть ли order_id с count > 1; чему равен sold у листинга)
```

**Вопросы.**
4. Почему заказы создавались при мёртвой Kafka, а `sold` не двигался? Что было бы с `POST /orders`, если бы publish стоял в хендлере?
5. Появились ли дубли после SIGKILL? Если нет — где именно должен упасть процесс, чтобы они появились? Почему `sold` всё равно сошёлся со `stock`?
6. Что даёт `FOR UPDATE SKIP LOCKED` в воркере, если запустить orders-svc в двух экземплярах? Что будет без него?

---

## 3. Идемпотентность и saga

`POST /orders` с одним `Idempotency-Key` 50 раз параллельно (напиши 10 строк на Go
или `xargs -P`), листинг со `stock=1`:

```
(вывод: сколько 201, сколько 200 с Idempotent-Replayed, сколько 409; итоговый stock)
```

**Вопросы.**
7. Reserve в catalog прошёл, а `INSERT INTO orders` упал. Что со стоком? Как бы ты компенсировал (какой RPC добавить в proto, что делать, если и он упал)?
8. Почему ключ идемпотентности резервируется до Reserve, а не записывается после COMMIT?

---

## 4. Один заказ в логах трёх сервисов

gateway и catalog-svc пишут JSON через `slog`, orders-svc пишет текстом через `log.Printf`.
Задача: по одному `X-Request-ID` собрать путь одного заказа через все три сервиса.

```bash
RID=$(uuidgen)
curl -s -X POST $GATEWAY_URL/orders -H "Authorization: Bearer $TOKEN" \
     -H "Idempotency-Key: $(uuidgen)" -H "X-Request-ID: $RID" \
     -H 'Content-Type: application/json' -d '{"listing_id":"...","qty":1}'
make logs | grep "$RID"
```

Дальше сложнее. Дай нагрузку `lab/load -mode orders` на 30 секунд и ответь, показав команды и вывод:

1. Сколько заказов за это время получили `code=Unavailable` от catalog? Для gateway это одна
   строка с `jq`. Напиши её.
2. Тот же вопрос для orders-svc. Что пришлось делать с `grep`/`awk`, и что случится с твоей
   командой, если коллега поменяет текст сообщения или добавит поле в середину строки?
3. Найди в логах orders-svc, сколько раз воркер outbox отправлял батч, пока Kafka была
   остановлена, и медиану размера батча. Сколько времени ушло?
4. Перепиши (у себя в отчёте, не в коде) три строки логов orders-svc в JSON. Какие поля стали
   бы обязательными и почему `request_id` среди них?
5. Где живут логи на Railway после `stdout` и что с ними через сутки? Что бы ты поставил
   рядом (Loki, ELK, Vector, что угодно), чтобы вопрос 1 решался одним запросом по всем
   трём сервисам?

Что писать: команды, куски логов (по 3–5 строк каждого вида), ответы. Главный вывод
одним абзацем: за что платят структурой и когда `Printf` всё-таки норм.

## 5. Партиции и порядок (для пятёрки)

Пересоздай топик с тремя партициями и повтори 30 заказов одного покупателя на один листинг:

```bash
docker run --rm --network compose_default apache/kafka:3.9.2 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --delete --topic order.created
docker run --rm --network compose_default apache/kafka:3.9.2 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 --create --topic order.created --partitions 3
```

```
(вывод: в каких партициях оказались события; из consumer-логов — порядок обработки)
```

**Вопросы.**
9. Ключ сообщения — `order_id`. Гарантирован ли порядок событий одного покупателя? Одного листинга? Что нужно взять ключом, чтобы события по листингу шли по порядку, и чем это плохо для «горячего» листинга?
