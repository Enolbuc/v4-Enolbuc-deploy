# Deploy and Host Marketplace v4 with Railway

Marketplace v4 — это финальный пет-проект школы [romanovich.school](https://romanovich.school): тот самый «монстр», к которому ведёт путь от CLI через REST и Postgres. Три Go-сервиса, gRPC между ними, Kafka с transactional outbox, Redis-кэш, JWT, два окружения и git flow. Всё, о чём спрашивают на собеседовании на Middle Go Backend, — в одном репозитории, который ты пишешь сам и показываешь в портфолио.

## About Hosting Marketplace v4

Шаблон поднимает готовую инфраструктуру, а код — твой. Из коробки: Apache Kafka в KRaft-режиме на persistent volume, Redis и три заглушки под `gateway`, `catalog-service`, `orders-service`, уже связанные приватной сетью и reference-переменными. Ты подключаешь свой репозиторий одним файлом `.railway/railway.ts`, вставляешь три строки Neon Postgres и получаешь стек, который пересобирается на каждый push: `develop` в dev-окружение, `main` в прод. Никаких адресов в коде, только переменные. Всё укладывается в бесплатный триал Railway: 30 дней, карта не нужна.

## Common Use Cases

- **Пет-проект уровня Middle.** Не десятый CRUD, а распределённая система: gateway → gRPC → Kafka → консюмер, с отказами, которые ты устраиваешь сам.
- **Портфолио на GitHub с живым продом.** Ссылка на работающий сервис в резюме вместо «локально всё работало».
- **Полигон для собеседований.** Outbox, идемпотентность, cache-aside, deadline propagation — вопросы, за которые платят, разобраны на собственном коде.
- **Отработка git flow.** Feature-ветки, Conventional Commits, релизы через PR, два окружения с разными базами. Так, как в команде.
- **Лабораторная по отказам.** Останови Kafka, убей воркер, выключи Redis и посмотри, что переживёт система.

## Dependencies for Marketplace v4 Hosting

- **Apache Kafka 3.9 (KRaft, native)** — брокер событий `order.created`, ~180 МБ RAM.
- **Redis 7** — cache-aside для каталога с инвалидацией после commit.
- **Neon Postgres** — три базы (`auth`, `catalog`, `orders`) у внешнего провайдера, бесплатно.
- **Go 1.26, gRPC, protobuf** — контракт сервисов зафиксирован в `proto/`.
- **Railway IaC** — `.railway/railway.ts` описывает весь стек и деплоит из твоего форка.

### Deployment Dependencies

- Репозиторий задания: [github.com/art-r00m/v4](https://github.com/art-r00m/v4) — README с контрактом и 21 acceptance-тестом, `DEPLOY.md` с пошаговым деплоем.
- [Neon](https://neon.tech) — Postgres без карты, три базы в одном проекте.
- [Railway CLI](https://docs.railway.com/cli) — `railway config apply` подключает твой код к шаблону.
- Школа: [romanovich.school](https://romanovich.school) — менторство до оффера, 80% практики.

### Implementation Details

Kafka на Railway требует трёх неочевидных настроек, и все они уже в шаблоне: `KAFKA_ADVERTISED_LISTENERS` через `RAILWAY_PRIVATE_DOMAIN`, лог-директория в поддиректории тома (в корне живёт `lost+found`) и `RAILWAY_RUN_UID=0` для non-root образа. Соседние сервисы получают адрес брокера через `${{kafka.KAFKA_BROKERS}}`, Redis через `${{redis.REDIS_URL}}`, gRPC-адреса через приватные домены. После деплоя шаблона остаётся заменить три `DATABASE_URL` и одну строку `REPO` в `railway.ts`:

```bash
railway link && railway config apply
```

### Why Deploy Marketplace v4 on Railway?

Railway — это одна платформа для всего стека: брокер, кэш, три сервиса и приватная сеть между ними без единого yaml для Kubernetes. Сервисы спят, когда не нужны, и просыпаются на первом запросе, поэтому учебный стек умещается в триал. Два окружения, деплой по веткам и логи в одном месте — ровно та среда, где ломается всё, что «локально работало», и где это учатся чинить.

Задеплоив Marketplace v4 на Railway, ты получаешь прод для пет-проекта за один клик и 30 дней, чтобы довести его до состояния «показать на собеседовании».
