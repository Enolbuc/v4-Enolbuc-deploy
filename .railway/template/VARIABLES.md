# Описания переменных для редактора шаблона

Вставляются в поле Description у каждой переменной. Колонка Required — что выставить в редакторе.
Значения по умолчанию у переменных ниже уже стоят в проекте, менять их не нужно.

## gateway

| Переменная | Required | Description |
|---|---|---|
| `DATABASE_URL` | да | Neon Postgres, база `auth` (пользователи). Строка с пулером и `sslmode=require`: `postgres://user:pass@ep-xxx-pooler.../auth?sslmode=require`. См. DEPLOY.md, шаг 1. |
| `JWT_SECRET` | нет (`${{secret(48)}}`) | Секрет подписи JWT. Генерируется при деплое. В окружении develop задай другой. |
| `PORT` | нет | Порт HTTP-сервера gateway. Railway пробрасывает на него публичный домен. По умолчанию 8080. |
| `CATALOG_GRPC_ADDR` | нет | Адрес gRPC catalog-service в приватной сети: `${{catalog-service.RAILWAY_PRIVATE_DOMAIN}}:50051`. Не менять. |
| `ORDERS_GRPC_ADDR` | нет | Адрес gRPC orders-service в приватной сети: `${{orders-service.RAILWAY_PRIVATE_DOMAIN}}:50052`. Не менять. |

## catalog-service

| Переменная | Required | Description |
|---|---|---|
| `DATABASE_URL` | да | Neon Postgres, база `catalog` (объявления, остатки, processed_events). Строка с пулером и `sslmode=require`. |
| `GRPC_ADDR` | нет | Адрес, который слушает gRPC-сервер. По умолчанию `:50051`. |
| `REDIS_URL` | нет | Redis для cache-aside каталога, ссылка на сервис redis: `${{redis.REDIS_URL}}`. |
| `KAFKA_BROKERS` | нет | Брокер для консюмера `order.created`, ссылка на сервис kafka: `${{kafka.KAFKA_BROKERS}}`. |

## orders-service

| Переменная | Required | Description |
|---|---|---|
| `DATABASE_URL` | да | Neon Postgres, база `orders` (заказы, outbox, idempotency keys). Строка с пулером и `sslmode=require`. |
| `GRPC_ADDR` | нет | Адрес, который слушает gRPC-сервер. По умолчанию `:50052`. |
| `KAFKA_BROKERS` | нет | Брокер для outbox-воркера, ссылка на сервис kafka: `${{kafka.KAFKA_BROKERS}}`. |
| `CATALOG_GRPC_ADDR` | нет | Адрес catalog-service для синхронного `Reserve`: `${{catalog-service.RAILWAY_PRIVATE_DOMAIN}}:50051`. |
| `REDIS_URL` | нет | Ссылка на redis: `${{redis.REDIS_URL}}`. По контракту orders-service Redis не обязателен; оставлена для экспериментов. |

## redis

| Переменная | Required | Description |
|---|---|---|
| `REDIS_URL` | нет | Экспорт адреса для соседей: `redis://${{RAILWAY_PRIVATE_DOMAIN}}:6379`. Сервисы ссылаются на неё как `${{redis.REDIS_URL}}`. |

## kafka

Всё ниже — готовая конфигурация KRaft на одном брокере. Ничего из этого менять не нужно, поле Required у всех снять.

| Переменная | Description |
|---|---|
| `KAFKA_BROKERS` | Экспорт адреса брокера для соседей: `${{RAILWAY_PRIVATE_DOMAIN}}:9092`. Сама Kafka её не читает. |
| `KAFKA_NODE_ID` | ID узла в KRaft-кластере. Один брокер, значение 1. |
| `KAFKA_PROCESS_ROLES` | Роли узла: `broker,controller`. Один процесс делает и то и другое. |
| `KAFKA_LISTENERS` | Что слушает брокер: PLAINTEXT на 9092 для клиентов, CONTROLLER на 9093 для кворума. |
| `KAFKA_ADVERTISED_LISTENERS` | Адрес, который брокер сообщает клиентам: приватный домен сервиса. Именно по нему подключаются соседи. |
| `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` | Протоколы листенеров. Внутри приватной сети шифрование не нужно, PLAINTEXT. |
| `KAFKA_CONTROLLER_LISTENER_NAMES` | Какой листенер используется контроллером: CONTROLLER. |
| `KAFKA_INTER_BROKER_LISTENER_NAME` | Листенер для трафика между брокерами: PLAINTEXT. |
| `KAFKA_CONTROLLER_QUORUM_VOTERS` | Участники кворума KRaft: `1@localhost:9093`, единственный узел. |
| `KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR` | Фактор репликации служебного топика оффсетов. Брокер один, значит 1. |
| `KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR` | Фактор репликации лога транзакций. Брокер один, значит 1. |
| `KAFKA_TRANSACTION_STATE_LOG_MIN_ISR` | Минимум in-sync реплик для лога транзакций. Брокер один, значит 1. |
| `KAFKA_AUTO_CREATE_TOPICS_ENABLE` | Создавать топик при первой записи. Включено, чтобы `order.created` появился сам. |
| `KAFKA_LOG_DIRS` | Директория данных на томе. Поддиректория, а не корень тома: в корне лежит `lost+found`, на нём Kafka падает. |
| `CLUSTER_ID` | Идентификатор KRaft-кластера для форматирования хранилища. Любая base64-строка из 22 символов, фиксирована, чтобы том переживал редеплой. |
| `RAILWAY_RUN_UID` | Запуск контейнера от root (0): образ работает от non-root, а том принадлежит root. Без этого AccessDeniedException на старте. |
