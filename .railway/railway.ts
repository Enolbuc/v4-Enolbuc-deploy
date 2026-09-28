// Railway Infrastructure as Code: весь стек v4 в одном файле.
// Применяется через `railway config apply` (см. DEPLOY.md). Railway сравнивает
// этот файл с проектом и показывает diff перед применением.
//
// Что менять тебе: только REPO. Секреты и строки подключения к Neon задаются
// в дашборде (отдельно для каждого окружения) и здесь помечены preserve(),
// в git они не попадают.
import {
  defineRailway,
  github,
  image,
  preserve,
  project,
  service,
  volume,
} from "railway/iac";

// ← твой репозиторий, owner/repo
const REPO = "OWNER/marketplace-v4";

export default defineRailway((ctx) => {
  // Окружение выбирается при `railway link` / `railway environment`:
  //   production ← ветка main, develop ← ветка develop.
  // Один файл, два применения. Всё, что отличается между окружениями
  // (строки Neon, JWT_SECRET), задаётся в дашборде и здесь preserve().
  const BRANCH = ctx.isEnvironment("production") ? "main" : "develop";

  // --- Инфраструктура ---------------------------------------------------

  // Kafka в KRaft-режиме, native-сборка: ~180 МБ RAM. Данные на томе, иначе
  // после сна/редеплоя брокер заново форматируется и топик пропадает.
  const kafkaData = volume("kafka-data", { sizeMB: 1024 });

  const kafka = service("kafka", {
    source: image("apache/kafka-native:3.9.2"),
    volumeMounts: { "/var/lib/kafka/data": kafkaData },
    env: {
      KAFKA_NODE_ID: "1",
      KAFKA_PROCESS_ROLES: "broker,controller",
      KAFKA_LISTENERS: "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093",
      // Приватный домен сервиса: kafka.railway.internal. Клиенты в проекте
      // получат именно его в metadata-ответе брокера.
      KAFKA_ADVERTISED_LISTENERS: "PLAINTEXT://${{RAILWAY_PRIVATE_DOMAIN}}:9092",
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT",
      KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER",
      KAFKA_INTER_BROKER_LISTENER_NAME: "PLAINTEXT",
      KAFKA_CONTROLLER_QUORUM_VOTERS: "1@localhost:9093",
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: "1",
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: "1",
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: "1",
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true",
      // Поддиректория, а не корень тома: в корне лежит lost+found, на нём Kafka падает.
      KAFKA_LOG_DIRS: "/var/lib/kafka/data/kraft",
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk",
      // Образ работает от non-root, том принадлежит root. Railway-способ обойти.
      RAILWAY_RUN_UID: "0",
      // Это не читает Kafka, это «экспорт» для других сервисов: ${{kafka.KAFKA_BROKERS}}.
      KAFKA_BROKERS: "${{RAILWAY_PRIVATE_DOMAIN}}:9092",
    },
  });

  const redis = service("redis", {
    source: image("redis:7-alpine"),
    env: {
      REDIS_URL: "redis://${{RAILWAY_PRIVATE_DOMAIN}}:6379",
    },
  });

  // Postgres не здесь: три базы (auth, catalog, orders) живут в одном проекте Neon, см. DEPLOY.md.
  // Лимит триала — 5 сервисов в проекте, и это ровно kafka + redis + три твоих.

  // --- Твои сервисы -------------------------------------------------------

  const catalog = service("catalog-service", {
    source: github(REPO, { branch: BRANCH }),
    build: { dockerfilePath: "Dockerfile.catalog" },
    env: {
      GRPC_ADDR: ":50051",
      DATABASE_URL: preserve(), // Neon, база catalog, sslmode=require
      REDIS_URL: redis.env.REDIS_URL,
      KAFKA_BROKERS: kafka.env.KAFKA_BROKERS,
    },
  });

  const orders = service("orders-service", {
    source: github(REPO, { branch: BRANCH }),
    build: { dockerfilePath: "Dockerfile.orders" },
    env: {
      GRPC_ADDR: ":50052",
      DATABASE_URL: preserve(), // Neon, база orders, sslmode=require
      KAFKA_BROKERS: kafka.env.KAFKA_BROKERS,
      CATALOG_GRPC_ADDR: "${{catalog-service.RAILWAY_PRIVATE_DOMAIN}}:50051",
    },
  });

  const gateway = service("gateway", {
    source: github(REPO, { branch: BRANCH }),
    build: { dockerfilePath: "Dockerfile.gateway" },
    healthcheck: "/healthz",
    healthcheckTimeout: 60,
    env: {
      PORT: "8080",
      JWT_SECRET: preserve(),
      DATABASE_URL: preserve(), // Neon, база auth (users), sslmode=require
      CATALOG_GRPC_ADDR: "${{catalog-service.RAILWAY_PRIVATE_DOMAIN}}:50051",
      ORDERS_GRPC_ADDR: "${{orders-service.RAILWAY_PRIVATE_DOMAIN}}:50052",
    },
  });

  return project("romanovich-school-marketplace", {
    resources: [kafka, kafkaData, redis, catalog, orders, gateway],
  });
});
