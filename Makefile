.PHONY: setup gen build test lint infra up down reset acceptance acceptance-remote run-gateway run-catalog run-orders logs load

# Переменные берутся из .env (скопируй .env.example), их можно переопределить в командной строке.
-include .env
export

COMPOSE := docker compose -f compose/docker-compose.yml
GATEWAY_URL ?= http://localhost:8080

setup:
	git config core.hooksPath .githooks
	@echo "Git-хуки включены: .githooks/pre-commit, commit-msg, pre-push"
	@test -f .env || (cp .env.example .env && echo ".env создан из .env.example")

# Сгенерировать gen/ из proto/. Нужны buf, protoc-gen-go, protoc-gen-go-grpc:
#   go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
#   go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
#   go install github.com/bufbuild/buf/cmd/buf@latest
# gen/ коммитится: Railway собирает образ через go build, buf там нет.
gen:
	buf generate

build:
	go build ./...

# Твои unit- и интеграционные тесты. Переменные из .env прокидываются,
# чтобы интеграционные тесты против реальных Postgres/Redis/Kafka тоже шли (см. README).
test:
	go test -race -count=1 -cover ./...

lint:
	golangci-lint run ./...

# Только инфраструктура: Postgres (5433), Redis (6380), Kafka (9094). Сервисы запускаешь сам.
infra:
	$(COMPOSE) up -d --wait

# Инфраструктура + три твоих сервиса, собранные из Dockerfile.*. Gateway на :8080.
up:
	$(COMPOSE) --profile services up -d --build --wait

down:
	$(COMPOSE) --profile services down -v

# Пересоздать стек пустым. Acceptance ждёт пустые базы и пустой топик.
reset: down up

run-gateway:
	DATABASE_URL=$(DATABASE_URL_AUTH) CATALOG_GRPC_ADDR=localhost:50051 ORDERS_GRPC_ADDR=localhost:50052 PORT=8080 go run ./gateway

run-catalog:
	DATABASE_URL=$(DATABASE_URL_CATALOG) GRPC_ADDR=:50051 go run ./catalog-svc

run-orders:
	DATABASE_URL=$(DATABASE_URL_ORDERS) CATALOG_GRPC_ADDR=localhost:50051 GRPC_ADDR=:50052 go run ./orders-svc

logs:
	$(COMPOSE) --profile services logs -f --tail=100

# Полный прогон против compose-стека: reset, потом все тесты, включая resilience.
acceptance: reset
	cd acceptance && GATEWAY_URL=http://localhost:8080 KAFKA_BROKERS=localhost:9094 REDIS_URL=redis://localhost:6380 \
		COMPOSE_FILE=$(CURDIR)/compose/docker-compose.yml \
		go test -race -count=1 -v ./...

# Прогон против задеплоенного стека (Railway): только то, что видно через HTTP.
# GATEWAY_URL берётся из .env. Kafka/Redis снаружи недоступны, эти тесты пропустятся.
acceptance-remote:
	cd acceptance && GATEWAY_URL=$(GATEWAY_URL) KAFKA_BROKERS= REDIS_URL= COMPOSE_FILE= \
		go test -count=1 -v ./...

# Лабораторная: нагрузка на GATEWAY_URL. Параметры см. в lab/load/main.go.
load:
	cd lab/load && go run . -base $(GATEWAY_URL) $(ARGS)
