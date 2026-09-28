# Деплой

В v2 и v3 деплой был по желанию. В v4 он часть задания: стек из пяти сервисов и
трёх баз на разных хостах — это ровно та среда, где ломается всё, что «локально
работало». Настраивать руками нужно один раз; дальше каждый push в `develop`
пересобирает dev-стек, а merge в `main` — прод.

Что нужно: аккаунт GitHub, Railway (триал: $5 на 30 дней, без карты), Neon (бесплатно,
без карты). Docker на ноутбуке для деплоя не нужен: образы собирает Railway по
`Dockerfile.*` из репозитория.

---

## Как это устроено

```
                          Railway-проект (5 сервисов, приватная сеть *.railway.internal)
                        ┌──────────────────────────────────────────────────────────┐
  браузер ── HTTPS ───► │ gateway ──gRPC──► catalog-svc ──► redis                  │
                        │    │                  ▲   │                              │
                        │    └───gRPC──► orders-svc ─┘  ──outbox──► kafka ──► consumer в catalog-svc
                        └───────┬───────────────┬──────────┬───────────────────────┘
                                │ TLS           │          │
                             Neon: auth      catalog     orders   (три базы в одном проекте)
```

Триал Railway даёт 5 сервисов и 1 ГБ RAM на проект. Kafka (native-сборка, ~180 МБ),
Redis и три Go-бинаря укладываются примерно в 300 МБ. Postgres в этот лимит уже не
влезает по числу сервисов, поэтому базы в Neon. Всё, что сервису нужно знать, он
получает через переменные окружения; ни одного адреса в коде.

---

## Шаг 1. Neon: три базы (потом ещё три)

1. <https://neon.tech>, вход через GitHub, **New project**. Регион — ближайший к Railway
   (для `europe-west4` это Frankfurt).
2. В проекте создай ещё две базы: **Databases → New database**: `catalog`, `orders`.
   Третью назови `auth` (или переименуй дефолтную `neondb`). Итого три базы, один проект.
   Это базы прода. Для окружения `develop` на шаге 5 понадобятся ещё три:
   `auth_dev`, `catalog_dev`, `orders_dev`, можно создать сразу.
3. Для каждой базы возьми **Connection string** с pooled-подключением и `sslmode=require`:

```
postgres://user:password@ep-xxx-pooler.eu-central-1.aws.neon.tech/catalog?sslmode=require
```

Проверь строки с ноутбука до деплоя:

```bash
psql "<строка>" -c 'select current_database()'
```

Neon засыпает через несколько минут простоя и просыпается на первом запросе за секунду.
Бесплатный план имеет лимит на compute-часы в месяц; для учебной нагрузки его хватает.

---

## Шаг 2. Railway: проект из шаблона

Railway — площадка хостинга. После регистрации 30 дней бесплатно ($5 кредита, карта
не нужна); этого хватает на весь стек v4 в режиме «почти всегда включён».

1. <https://railway.com>, **Login** → через Google или GitHub. В настройках аккаунта
   подключи GitHub (он понадобится для сборки из твоего форка) и пройди верификацию
   на <https://railway.com/verify>, иначе триал будет с урезанной сетью.
2. Нажми кнопку **Deploy on Railway** из README (или открой ссылку на шаблон от
   ментора) → **Deploy**. Railway создаст проект с пятью сервисами: `kafka`, `redis`,
   `gateway`, `catalog-service`, `orders-service` (папки в репозитории называются
   `catalog-svc` и `orders-svc`, это одно и то же). Kafka и Redis уже настроены и
   стартуют. Три твоих сервиса пока заглушки.
3. Дождись, пока `kafka` станет зелёной (10–20 секунд). В её логах ищи
   `Kafka Server started`.

Шаблон не привязан к твоему коду специально: код подключается следующим шагом через
Infrastructure as Code, чтобы конфигурация лежала в репозитории, а не в кликах.

---

## Шаг 3. Подключить репозиторий: `.railway/railway.ts`

В репозитории лежит `.railway/railway.ts` — описание всех пяти сервисов на TypeScript.
Railway CLI сравнивает файл с проектом и показывает diff перед применением.

1. Установи CLI и войди:

```bash
npm i -g @railway/cli
railway login
cd <репозиторий>
railway link          # выбери проект из шаблона и окружение production
```

2. В `.railway/railway.ts` поменяй одну строку: `const REPO = "OWNER/marketplace-v4"`
   на свой форк.

3. Установи SDK для файла и посмотри план:

```bash
cd .railway && npm install && cd ..
railway config plan
```

План должен показывать только изменения источника у трёх твоих сервисов
(заглушка → твой GitHub-репозиторий с `Dockerfile.*`) и, возможно, healthcheck.
Если план хочет удалить `kafka` или `redis` — остановись и перечитай файл, что-то не так.

4. Примени:

```bash
railway config apply
```

Railway попросит доступ к репозиторию (GitHub App), соберёт три образа и задеплоит.
Первая сборка 2–4 минуты.

5. Секреты. В файле они помечены `preserve()`: значение живёт в Railway, в git не
   попадает. Задай их один раз в дашборде (**Variables** у сервиса):

| Сервис | Переменная | Значение |
|---|---|---|
| gateway | `JWT_SECRET` | любая длинная случайная строка |
| gateway | `DATABASE_URL` | строка Neon для базы `auth` |
| catalog-service | `DATABASE_URL` | строка Neon для базы `catalog` |
| orders-service | `DATABASE_URL` | строка Neon для базы `orders` |

Остальное (`KAFKA_BROKERS`, `REDIS_URL`, `*_GRPC_ADDR`) файл выставляет сам через
ссылки на приватные домены соседних сервисов.

6. У `gateway` включи публичный домен: **Settings → Networking → Generate Domain**,
   порт 8080. Открой `https://<домен>/` — это фронт из `web/index.html`. Регистрация
   должна пройти: значит, gateway дошёл до Neon и накатил миграции.

Дальше каждый merge в `main` пересобирает те сервисы, чьи файлы изменились.
Окружение `develop` появится на шаге 5.

---

## Шаг 4. Проверить acceptance против облака

```bash
# в .env
GATEWAY_URL=https://<домен gateway>
make acceptance-remote
```

Пойдут тесты 01–14 и 16: всё, что видно через HTTP, включая цепочку
outbox → Kafka → consumer (тест 16 ждёт, пока `sold` догонит `stock`). Тесты, которым
нужен прямой доступ к Kafka/Redis или `docker compose stop`, пропускаются: на Railway
Kafka и Redis наружу не торчат, а отказы ты устраиваешь руками через Stop/Restart
в дашборде (см. `lab/LAB.md`).

---

## Шаг 5. Окружение `develop` и защита веток

Процесс из README «Ветки, коммиты, релизы» физически выглядит так:

```
feat/x ──PR──► develop ──push──► Railway develop   (базы *_dev)
                  │
                  └──PR──► main ──push──► Railway production (базы auth/catalog/orders)
```

Ветки в git:

```bash
git switch -c develop main
git push -u origin develop
```

На GitHub: **Settings → General → Default branch** → `develop`. Теперь PR по умолчанию
целятся в `develop`, а `main` меняется только релизными PR.

Окружение в Railway (шаблон создаёт только `production`):

1. В дашборде: выпадающий список окружений сверху → **New Environment** →
   **Duplicate** от `production`, имя `develop`. Railway скопирует все пять сервисов
   с переменными и покажет staged changes — нажми **Deploy**.
2. Переменные `develop` сейчас указывают на прод-базы. Поменяй у `gateway`,
   `catalog-service`, `orders-service` `DATABASE_URL` на строки `*_dev` из Neon
   и задай другой `JWT_SECRET`. Не пропусти: иначе dev-стек будет писать в прод.
3. Переключи CLI на новое окружение и примени тот же файл:

```bash
railway environment develop
railway config plan     # у трёх сервисов источник: ветка develop вместо main
railway config apply
```

`railway.ts` один: он смотрит на `ctx.environment` и подставляет ветку `main` для
`production` и `develop` для остального. Переменные окружения в нём `preserve()`,
поэтому dev-строки Neon останутся dev-строками.

4. У `gateway` в `develop` сгенерируй свой публичный домен (Settings → Networking).
   Проверь: `GATEWAY_URL=https://<dev-домен> make acceptance-remote`.

Память: два окружения — это два набора сервисов, около 600 МБ из 1 ГБ триала.
Влезает. PR-environments (по окружению на каждый PR) не включай: третий набор
уже не поместится, и они не нужны — feature-ветки проверяет CI.

Защита веток на GitHub, чтобы push мимо PR не прошёл и у тех, кто снёс хуки:
**Settings → Rules → Rulesets → New branch ruleset**, target `main` и `develop`:
**Require a pull request before merging**, **Require status checks** (`ci`, `gitflow`),
**Block force pushes**. На бесплатном плане rulesets работают в публичных
репозиториях; репозиторий пет-проекта делай публичным.

---

## Serverless и сон

В шаблоне у сервисов включён режим Serverless: сервис засыпает через
5–10 минут без исходящего трафика. С двумя окружениями это и позволяет
уложиться в память триала. На практике этот стек почти не спит: orders-svc
поллит outbox и держит соединение с Kafka, catalog-svc держит пул к Neon и Redis.
Заснуть может только gateway. Первый запрос после сна может вернуть 502; повтори
через несколько секунд. Acceptance-тесты перед стартом ждут `/healthz` до 90 секунд
как раз на этот случай.

Полезный вопрос на подумать: почему сервис с outbox-воркером в принципе нельзя
делать serverless, даже если бы платформа умела будить его по расписанию?

---

## Три грабли Kafka на Railway (уже обойдены в шаблоне)

Знать про них стоит, потому что на любой другой платформе ты наступишь на те же:

| Симптом | Причина | Что сделано |
|---|---|---|
| `AccessDeniedException` при записи в log dir | Образ работает от non-root, том смонтирован под root | `RAILWAY_RUN_UID=0` |
| `Found directory .../lost+found, not in the form of topic-partition` | В корне ext4-тома лежит `lost+found`, Kafka требует чистую директорию | `KAFKA_LOG_DIRS` в поддиректории тома |
| Брокер занимает 700+ МБ и проект упирается в 1 ГБ | JVM-образ | `apache/kafka-native` (GraalVM), ~180 МБ |

И четвёртая, общая: `KAFKA_ADVERTISED_LISTENERS` должен указывать на приватный домен
`kafka.railway.internal`, потому что именно этот адрес брокер отдаёт клиентам в
metadata-ответе. Локально это `kafka:9092` внутри compose-сети и `localhost:9094`
снаружи — два listener-а в `compose/docker-compose.yml`.

---

## Если не завелось

| Симптом | Что происходит |
|---|---|
| `railway config plan` хочет удалить kafka/redis | В файле нет этих сервисов или изменены имена. Имена в файле должны совпадать с именами в проекте. |
| Сборка падает на `COPY go.sum` | Нет `go.sum` в репозитории. `go mod tidy`, закоммить. |
| gateway падает на старте, в логах `DATABASE_URL is required` | Не задана переменная в дашборде (см. таблицу секретов). |
| В логах `SSL is required` | В строке Neon нет `sslmode=require`. |
| `/healthz` отдаёт 503 `catalog-svc is not serving` | catalog-svc не поднялся (смотри его логи) или в `CATALOG_GRPC_ADDR` не приватный домен. |
| orders-svc: `dial tcp ... kafka.railway.internal: connection refused` | Kafka ещё стартует, или `KAFKA_ADVERTISED_LISTENERS` смотрит на localhost. Воркер переподключится сам. |
| Заказы создаются, `sold` не растёт | Outbox не дренируется: смотри логи orders-svc (`publish failed`) и catalog-svc (`consuming`). |
| Данные пропали после редеплоя kafka | Нет тома, или `KAFKA_LOG_DIRS` вне тома. |
| Первый запрос после паузы 502 | Сон сервиса или Neon. Не баг. |
| В браузере CORS-ошибка на `POST /orders` | В `Access-Control-Allow-Headers` нет `Idempotency-Key`. См. `TestAcc11`. |
