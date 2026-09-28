# Контракт

Всё, что проверяют acceptance-тесты, и всё, что должно быть внутри стека, чтобы они проходили. Задание, тесты, правила репозитория и вопросы на собеседовании в [README.md](README.md). Контракт приведён целиком, без отсылок к прошлым версиям: репозиторий самодостаточен.

Оглавление:

1. [Запуск и переменные](#запуск-и-переменные)
2. [HTTP (gateway)](#http-gateway): роуты, каждый эндпоинт, JWT, ошибки, middleware, graceful shutdown, фронтенд
3. [gRPC](#grpc)
4. [Kafka](#kafka)
5. [Redis](#redis)
6. [Outbox](#outbox)
7. [Антипаттерны](#антипаттерны)

## Запуск и переменные

Три бинаря из одного модуля: `go build ./gateway`, `./catalog-svc`, `./orders-svc`. Каждый читает своё окружение и без обязательных переменных не стартует: код возврата не ноль, имя переменной в stderr.

| Сервис | Обязательные | Необязательные |
|---|---|---|
| gateway | `JWT_SECRET`, `DATABASE_URL` (база auth), `CATALOG_GRPC_ADDR`, `ORDERS_GRPC_ADDR` | `PORT` (по умолчанию 8080) |
| catalog-svc | `DATABASE_URL`, `REDIS_URL`, `KAFKA_BROKERS` | `GRPC_ADDR` (`:50051`) |
| orders-svc | `DATABASE_URL`, `KAFKA_BROKERS`, `CATALOG_GRPC_ADDR` | `GRPC_ADDR` (`:50052`) |

Форматы: `DATABASE_URL=postgres://user:pass@host:port/db?sslmode=…` (локальный compose с `sslmode=disable`, Neon требует `sslmode=require`, код про это не знает), `REDIS_URL=redis://host:6379`, `KAFKA_BROKERS=host:9092[,host2:9092]`, `*_GRPC_ADDR=host:port` без схемы. Локальные значения лежат в `.env.example`, `make setup` копирует его в `.env`.

Сервис с базой сначала накатывает миграции (версионированные, с таблицей версий) и только потом начинает слушать. gateway начинает отвечать на `/healthz` только когда сам готов; тесты ждут `/healthz` до 90 секунд.

## HTTP (gateway)

### Роуты

Открытые, токен не нужен:

| Метод | Путь | Что делает |
|---|---|---|
| GET | `/` | Отдаёт фронтенд из `web/index.html` |
| GET | `/healthz` | `200 ok`, если catalog-svc и orders-svc отвечают `SERVING`; иначе `503 unavailable` |
| POST | `/auth/register` | Создаёт пользователя, выдаёт пару токенов |
| POST | `/auth/login` | Выдаёт пару токенов |
| POST | `/auth/refresh` | Обновляет пару по refresh-токену |
| GET | `/listings` | Список листингов с пагинацией |
| GET | `/listings/{id}` | Один листинг, с заголовком `X-Cache` |

Приватные, нужен access-токен в заголовке `Authorization: Bearer <token>`:

| Метод | Путь | Кому |
|---|---|---|
| POST | `/listings` | только `seller` |
| PATCH | `/listings/{id}` | только `seller`, создавший листинг |
| DELETE | `/listings/{id}` | только `seller`, создавший листинг |
| POST | `/orders` | только `buyer` |
| GET | `/orders` | только `buyer`, только свои |

Покупка теперь заказ, отдельного роута `/listings/{id}/buy` нет. Любой другой путь даёт `404` в JSON-формате ошибок, включая заглушки файл-сервера вроде `text/plain 404 page not found`. Корень единственный HTML-роут.

Порядок проверок на приватных роутах всегда один: токен (401), роль (403), тело (400).

### Пользователи

#### `POST /auth/register`

Все три поля обязательны. `role` только `buyer` или `seller`.

```json
// запрос
{"email": "a@a.com", "password": "hunter22", "role": "seller"}
// ответ 201
{"access_token": "...", "refresh_token": "...", "user": {"id": "uuid", "email": "a@a.com", "role": "seller"}}
```

Занятый email даёт `409 conflict`. Плохое тело даёт `400 validation_failed`. Конфликт лови по ошибке unique-индекса от базы (у `pgx` это `*pgconn.PgError` с кодом `23505`), а не по `SELECT` перед `INSERT`: два параллельных запроса с одним email пройдут такой `SELECT` оба.

Пользователи живут в базе `auth`, которой владеет gateway. Это осознанный компромисс, а не «так получилось»: разбор в «Шаге первом» README.

#### `POST /auth/login`

```json
// запрос
{"email": "a@a.com", "password": "hunter22"}
// ответ 200
{"access_token": "...", "refresh_token": "..."}
```

Неверный пароль или несуществующий email дают `401 unauthenticated`. Access и refresh в паре всегда разные строки.

#### `POST /auth/refresh`

```json
// запрос
{"refresh_token": "..."}
// ответ 200
{"access_token": "...", "refresh_token": "..."}
```

Принимает только токен с `typ=refresh`. Access-токен, битый или истёкший токен дают `401`.

### Листинги

Листинг в любом ответе выглядит так:

```json
{"id":"uuid","title":"Молоко","price":50.00,"stock":7,"sold":3,"seller_id":"uuid","created_at":"2025-05-15T10:00:00Z"}
```

`stock` уменьшается синхронно при заказе. `sold` растёт асинхронно, через Kafka, и догоняет за секунды. У свежего листинга `sold` равен нулю.

#### `POST /listings`

Только `seller`. Все три поля обязательны: `title` не пустой, `price` больше нуля, `stock` не отрицательный.

```json
// запрос
{"title": "Молоко", "price": 50.00, "stock": 10}
// ответ 201
{"id": "uuid", "title": "Молоко", "price": 50.00, "stock": 10, "sold": 0, "seller_id": "uuid", "created_at": "2025-05-15T10:00:00Z"}
```

При невалидном теле в `fields` перечисляются все плохие поля сразу: на тело с пустым `title`, отрицательными `price` и `stock` тест ждёт три записи.

#### `GET /listings?limit=20&offset=0`

Оба параметра необязательны. По умолчанию `limit=20`, `offset=0`.

- `limit` от 1 до 100 включительно, иначе `400 validation_failed`.
- `offset` ноль или больше, иначе `400`.
- Порядок строго по `created_at` по возрастанию.
- Заголовок `X-Total-Count` содержит общее число листингов, а не размер страницы.

```
200
X-Total-Count: 42
[{"id":"...","title":"...","price":50.00,"stock":10,"sold":0,"seller_id":"...","created_at":"..."}]
```

Внутри это `Catalog.List`, который возвращает и страницу, и `total`.

#### `GET /listings/{id}`

`200` с телом листинга. Неизвестный id даёт `404 not_found`. Невалидный uuid тоже `404`, а не `500`.

Ответ несёт заголовок `X-Cache`, он показывает, откуда взят листинг:

| Значение | Когда |
|---|---|
| `MISS` | В Redis не было, взят из Postgres и положен в кэш |
| `HIT` | Из Redis |
| `BYPASS` | Redis недоступен или ответил ошибкой; взят из Postgres, ответ всё равно `200` |

Сразу после `PATCH` или `DELETE` следующий `GET` даёт `MISS` с новыми данными (или `404`). Батчированная инвалидация допустима, тест ждёт до 2 секунд.

#### `PATCH /listings/{id}`

Только владелец. Частичное обновление: поля те же, что при создании, каждое необязательно, но присланные проверяются по тем же правилам. Ответ `200` с обновлённым листингом. Чужой листинг даёт `403`, неизвестный id даёт `404`.

#### `DELETE /listings/{id}`

Только владелец. Ответ `204` без тела. Чужой листинг даёт `403`, неизвестный id даёт `404`. После удаления `GET /listings/{id}` даёт `404`.

### Заказы

#### `POST /orders`

Только `buyer`. Необязательный заголовок `Idempotency-Key`, до 200 символов.

```json
// запрос
{"listing_id": "uuid", "qty": 3}
// ответ 201
{"id":"uuid","buyer_id":"uuid","listing_id":"uuid","qty":3,"created_at":"2025-05-15T10:00:00Z"}
```

Что происходит внутри: gateway вызывает `Orders.Create`, orders-svc синхронно вызывает `Catalog.Reserve` (там `SELECT … FOR UPDATE` и уменьшение `stock`), потом в одной транзакции пишет заказ и строку outbox и отвечает. Сразу после `201` листинг показывает уменьшенный `stock`. Kafka в этом пути не участвует: с остановленной Kafka заказ создаётся, событие уедет позже.

| Ситуация | Ответ |
|---|---|
| `qty` не положительное, `listing_id` не uuid | `400 validation_failed` с `fields` |
| Листинг не найден | `404 not_found` |
| Остатка не хватает | `409 conflict`, остаток не меняется, заказ не создаётся |
| Повтор с тем же `Idempotency-Key` от того же покупателя | `200` с тем же заказом и заголовком `Idempotent-Replayed: true` (`201` тест тоже принимает) |
| catalog-svc недоступен или не ответил за 2 с | `503 unavailable` не позже чем через 5 с |

Про идемпотентность: ключ привязан к покупателю. Тот же ключ от другого покупателя это его собственный новый заказ. Другой ключ от того же покупателя это новый заказ. Повтор не трогает остаток. 100 параллельных заказов по одной штуке при остатке 10 дают ровно 10 ответов `201` и 90 ответов `409`, `stock` в конце ноль.

#### `GET /orders?limit=20&offset=0`

Только `buyer`, только свои заказы, новые сверху. Правила `limit` и `offset` как у листингов. `X-Total-Count` содержит число заказов этого покупателя. Формат элементов тот же, что в ответе `POST /orders`. Без токена `401`.

### JWT

- Алгоритм HS256, секрет из `JWT_SECRET`.
- Claims: `sub` (id пользователя), `role`, `exp`, `iat`, `typ` со значением `access` или `refresh`.
- Access живёт 5 минут. Тест допускает от 1 до 15 минут.
- Refresh живёт 7 дней. Тест требует не меньше суток и строго дольше access.
- Приватные роуты принимают только `typ=access`, refresh-роут только `typ=refresh`.
- Проверяются подпись, `exp` и `typ`. Любая проблема даёт `401`.
- Токен разбирает только gateway. В gRPC-запросы он кладёт `seller_id`/`buyer_id` из `sub`, сервисы ему верят (см. gRPC ниже).

### Формат ошибок

Все ответы 4xx и 5xx имеют одну форму:

```json
{
  "error": "validation_failed",
  "message": "request body is invalid",
  "fields": [
    {"field": "title", "message": "must not be empty"},
    {"field": "price", "message": "must be positive"}
  ]
}
```

`fields` присутствует только при `validation_failed`.

| Статус | `error` | Когда |
|---|---|---|
| 400 | `validation_failed` | плохое тело или query-параметры |
| 401 | `unauthenticated` | нет токена, битый, истёк, не тот `typ`, неверный пароль |
| 403 | `forbidden` | не та роль или не владелец |
| 404 | `not_found` | нет такого листинга, заказа или пути |
| 409 | `conflict` | не хватает товара, email занят |
| 503 | `unavailable` | gRPC-сервис недоступен или не уложился в дедлайн (`Unavailable`, `DeadlineExceeded`); `/healthz` при нездоровом бэкенде |
| 500 | `internal` | паника, внутренняя ошибка, база gateway недоступна |

Маппинг gRPC-кодов в HTTP делает gateway, в одном месте:

| Код gRPC | HTTP |
|---|---|
| `NotFound` | 404 |
| `InvalidArgument` | 400 |
| `PermissionDenied` | 403 |
| `FailedPrecondition`, `AlreadyExists`, `Aborted` | 409 |
| `Unavailable`, `DeadlineExceeded` | 503 |
| остальное | 500 |

### Middleware

Порядок для приватных роутов: request-id → logger → recover → CORS → auth → require-role → handler. Для открытых те же первые четыре.

1. **request-id.** Берёт `X-Request-ID` из запроса или генерирует UUID. Пишет его в ответный заголовок `X-Request-ID`, в контекст и дальше в gRPC-metadata как `x-request-id`.
2. **logger.** После ответа пишет одну JSON-строку через `slog` уровня INFO с полями `request_id`, `method`, `path`, `status`, `dur_ms`, `remote_addr`. При статусе 500 и выше уровень ERROR.
3. **recover.** Ловит панику, пишет ERROR в лог и отдаёт `500 internal`.
4. **CORS.** `Access-Control-Allow-Origin: *`. `OPTIONS` с заголовком `Origin` обрабатывается здесь же: 2xx с `Access-Control-Allow-Origin` и `Access-Control-Allow-Methods`, в котором есть `POST`. `Access-Control-Allow-Headers` содержит `Authorization`, `Content-Type`, `Idempotency-Key`. `Access-Control-Expose-Headers` содержит `X-Total-Count`, `X-Request-ID`, `X-Cache`.
5. **auth.** Разбирает `Authorization`, проверяет JWT, кладёт `user_id` и `role` в контекст. Проблема даёт `401`.
6. **require-role.** Сравнивает роль из контекста с требуемой. Не совпала, `403`.

Все gRPC-вызовы из gateway с дедлайном 3 секунды. Из orders-svc в catalog-svc (`Reserve`) 2 секунды. Без дедлайна лежащий catalog-svc превращается в 30-секундное зависание, и `TestAcc20` это ловит.

### Graceful shutdown

- По SIGTERM или SIGINT gateway вызывает `http.Server.Shutdown` с дедлайном 10 секунд, gRPC-серверы делают `GracefulStop` с тем же дедлайном.
- Запросы, которые уже выполняются, дозавершаются. Новые соединения не принимаются.
- orders-svc после остановки gRPC даёт outbox-воркеру ещё до 5 секунд дослать очередь. catalog-svc останавливает консюмер и коммитит оффсеты обработанного.
- Потом закрываются пулы и клиенты. Процесс выходит с кодом 0. Не уложился в дедлайн: ERROR в лог и код 1.

### Фронтенд

`web/index.html` готов, ты его не пишешь и не меняешь, но обязан отдавать:

| Метод | Путь | Ответ |
|---|---|---|
| GET | `/` | `200`, `Content-Type: text/html; charset=utf-8`, содержимое `web/index.html` |

Файл вшивается в бинарь через `embed`, пакет `web` уже есть. Контейнер собирается из одного бинаря, чтение с диска в рантайме не сработает.

## gRPC

Контракта в репозитории нет, есть требования к нему. Пиши `proto/catalog.proto` и `proto/orders.proto` сам, генерируй в `gen/` через `make gen` (`buf.yaml`, `buf.gen.yaml` уже лежат) и коммить сгенерированное: Railway собирает `go build`, генерировать на сборке нечем. Acceptance-тесты ходят только через HTTP, Kafka и Redis, поэтому имена RPC и полей внутри стека твои. Что обязано быть:

```
Catalog:  Create, Get, List, Update, Delete
          Reserve(listing_id, qty) → remaining        // атомарно, FOR UPDATE
Orders:   Create(buyer_id, listing_id, qty, idempotency_key) → Order
          List(buyer_id, limit, offset) → items, total
```

Примерный `Listing`: `id`, `seller_id`, `title`, `price`, `stock`, `sold`, `created_at`. Примерный `Order`: `id`, `buyer_id`, `listing_id`, `qty`, `created_at`. Что вернёт HTTP, зафиксировано выше; как это выглядит в proto, решаешь ты и объясняешь на защите.

Требования:

- Пакеты с версией (`marketplace.catalog.v1`), `go_package` внутрь модуля `marketplace`. Один сервис, один файл.
- `price` не `float` и не `double`. Строка с decimal или целые минорные единицы, одно и то же во всём стеке, а наружу в JSON число, как в примерах выше.
- Частичный `Update`: клиент прислал только `title`, `stock` не обнулился. Как выразить это в proto3 (`optional`, `FieldMask`, wrapper-типы) — вопрос 2 на собеседовании, выбери и обоснуй.
- `List` возвращает `total` для `X-Total-Count`.
- Оба сервиса регистрируют стандартный `grpc.health.v1.Health`; gateway опрашивает его на `/healthz`.
- Коды: `Reserve` при нехватке остатка `FailedPrecondition`. `Get`/`Update`/`Delete` неизвестного id `NotFound`. Чужой листинг в `Update`/`Delete` `PermissionDenied`. Невалидный ввод `InvalidArgument`. Как они превращаются в HTTP, в таблице выше.
- `seller_id` и `buyer_id` в запросы кладёт gateway из JWT: gRPC-сервисы токен не разбирают и gateway доверяют. Это внутренняя сеть, и это вопрос 4 на собеседовании.
- `Get` отдаёт состояние кэша в response header `x-cache` (metadata), gateway превращает его в `X-Cache`.
- `Orders.Create` при повторе по ключу отдаёт header `x-idempotent-replay: true`, gateway превращает его в `Idempotent-Replayed: true` и статус `200`.
- Входящий `x-request-id` логируется и пробрасывается дальше (orders-svc → catalog-svc). Одна строка лога на RPC в каждом сервисе.
- Изменение контракта после первого релиза это отдельный PR с пометкой в описании, что сломалось бы у старого клиента и почему нет. Номера полей не переиспользуются.

## Kafka

- Топик `order.created`, автосоздание включено в compose и в шаблоне Railway.
- Ключ сообщения: `order_id`. Значение, JSON:

```json
{"event_id":"uuid","order_id":"uuid","buyer_id":"uuid","listing_id":"uuid","qty":3,"created_at":"2025-05-15T10:00:00Z"}
```

- `event_id` уникален на событие и нужен консюмеру для дедупликации. Гарантия at-least-once: воркер может опубликовать и упасть до `UPDATE published_at`.
- Каждый заказ в штатном режиме попадает в топик ровно один раз. Публикует отдельный воркер из outbox, никогда не хендлер.
- Consumer group в catalog-svc одна, с ручным коммитом оффсета после успешной обработки. Консюмер увеличивает `sold` листинга и инвалидирует его в Redis. Повтор события счётчик не меняет.
- Kafka не на синхронном пути: `POST /orders` брокера не ждёт, `/healthz` от Kafka не зависит.

## Redis

- Ключ `listing:{id}`, значение сериализованный листинг, TTL 30 секунд.
- Чтение: `Get` идёт в Redis, при промахе в Postgres и кладёт результат с TTL.
- Инвалидация: `DEL` после commit в `Update`, `Delete`, `Reserve` и после применения события в консюмере. Четыре точки, все обязательны. Инвалидация до commit это гонка: параллельный `Get` успеет положить старую строку.
- Redis недоступен: `PING` на старте только предупреждает, `Get` идёт в Postgres с `X-Cache: BYPASS`, мутации логируют `WARN` и продолжают. Все операции с таймаутом 200 мс, без ретраев. Ошибка Redis это никогда не `500`.
- `/healthz` от Redis не зависит.

## Outbox

То, ради чего эта версия существует.

**Проблема.** Пишешь заказ в базу, потом публикуешь событие. Процесс упал между ними: заказ есть, события нет, `sold` никогда не сойдётся. Наоборот (сначала событие, потом база): событие про заказ, которого нет.

**Решение.** Событие записывается в таблицу `outbox` в той же транзакции, что и заказ:

```sql
BEGIN;
INSERT INTO orders (…) VALUES (…);
INSERT INTO outbox (event_id, topic, key, payload) VALUES ($1, 'order.created', $order_id, $json);
COMMIT;
```

Отдельная горутина каждые 200 мс:

```sql
SELECT id, topic, key, payload FROM outbox
 WHERE published_at IS NULL ORDER BY id LIMIT 100
   FOR UPDATE SKIP LOCKED;
-- publish в Kafka, дождаться acks
UPDATE outbox SET published_at = now() WHERE id = ANY($ids);
```

Публикация не удалась: `WARN`, строки остаются, следующий тик повторит. Kafka лежит час, outbox растёт час, заказы создаются. Kafka вернулась, очередь уезжает. `FOR UPDATE SKIP LOCKED` позволяет запустить второй экземпляр orders-svc без дублирования работы.

**Идемпотентность запроса** живёт рядом. Ключ `(buyer_id, key)` резервируется первым делом, до `Reserve`, через `INSERT … ON CONFLICT DO NOTHING`. Вставка прошла: ты первый, идёшь дальше и после успеха записываешь `order_id`. Не прошла: ждёшь или сразу отдаёшь готовый заказ. Заказ не удался: ключ снимается, чтобы повтор мог пройти.

**На стороне консюмера.** В той же транзакции, что и `UPDATE listings SET sold = sold + qty`, делается `INSERT INTO processed_events (event_id) … ON CONFLICT DO NOTHING`; ноль вставленных строк означает дубль, `UPDATE` пропускается. Оффсет коммитится после `COMMIT`.

## Антипаттерны

Каждый из них ловит конкретный тест.

- `producer.Publish` в хендлере после `COMMIT`. `TestAcc21` создаст заказ при остановленной Kafka и увидит либо 500, либо заказ без события.
- `UPDATE published_at` до `WriteMessages`. Событие теряется при первой же ошибке публикации.
- Консюмер без `processed_events`: после любого дубля `sold` уезжает. `TestAcc16` ждёт 3 секунды после схождения и проверяет, что счётчик не сдвинулся.
- `DEL` из Redis до `COMMIT`. Гонка с параллельным `Get`, кэш отдаёт старую цену. `TestAcc14`.
- gRPC-вызов без `context.WithTimeout`. `TestAcc20` ждёт ответ не дольше 5 секунд.
- `redis.Get` вернул ошибку и хендлер ответил 500. Нужен fallback в Postgres. `TestAcc19`.
- Ключ идемпотентности записывается после `COMMIT`. Два параллельных запроса с одним ключом оба пройдут `Reserve`. `TestAcc13`.
- `/healthz` возвращает 200 всегда. Railway и `TestAcc20` хотят знать, что gateway реально дотягивается до бэкендов.
- `Reserve` без `FOR UPDATE`. `TestAcc06` получит 11–13 успехов вместо 10.
- Пользователи в catalog-svc «потому что там уже есть база». Владелец данных определяется тем, кто их использует, а не наличием подключения.
