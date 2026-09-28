// Resilience: останавливаем часть стека и проверяем контракт. Нужен COMPOSE_FILE.
// На Railway то же самое делается кнопкой Stop/Restart руками, см. lab/LAB.md.
package acceptance_test

import (
	"testing"
	"time"
)

// Redis остановлен → GET /listings/{id} всё равно 200, X-Cache: BYPASS, /healthz 200.
func TestAcc19_ReadsSurviveRedisDown(t *testing.T) {
	skipIfNoCompose(t)
	seller := registerUser(t, "seller")
	id := createListing(t, seller, "redis-down", 10, 5)

	composeStop(t, "redis")
	t.Cleanup(func() {
		composeStart(t, "redis")
		waitHealthy(t, 30*time.Second)
	})
	time.Sleep(2 * time.Second)

	start := time.Now()
	l, r := getListing(t, id)
	if got := r.headers.Get("X-Cache"); got != "BYPASS" {
		t.Errorf("with Redis down expected X-Cache: BYPASS, got %q", got)
	}
	if l.Stock != 5 {
		t.Errorf("listing body wrong with Redis down: %+v", l)
	}
	if el := time.Since(start); el > 3*time.Second {
		t.Errorf("GET took %v with Redis down — no timeout on Redis calls (spec: 200ms)", el)
	}
	if h := doHTTP(t, "GET", gatewayURL(t)+"/healthz", "", nil); h.status != 200 {
		t.Errorf("/healthz must not depend on Redis, got %d", h.status)
	}
}

// catalog-svc остановлен → /healthz 503, POST /orders 5xx быстро (есть gRPC deadline).
func TestAcc20_CatalogDownFailsFast(t *testing.T) {
	skipIfNoCompose(t)
	seller := registerUser(t, "seller")
	buyer := registerUser(t, "buyer")
	id := createListing(t, seller, "needs-catalog", 10, 5)

	composeStop(t, "catalog-svc")
	t.Cleanup(func() {
		composeStart(t, "catalog-svc")
		waitHealthy(t, 60*time.Second)
	})
	time.Sleep(2 * time.Second)

	if h := doHTTP(t, "GET", gatewayURL(t)+"/healthz", "", nil); h.status != 503 {
		t.Errorf("/healthz with catalog down: expected 503, got %d", h.status)
	}

	start := time.Now()
	r := doHTTP(t, "POST", gatewayURL(t)+"/orders", buyer.Access, map[string]any{"listing_id": id, "qty": 1})
	elapsed := time.Since(start)
	if r.status < 500 || r.status > 599 {
		t.Errorf("catalog down: expected 5xx, got %d. body: %s", r.status, r.body)
	}
	if elapsed > 5*time.Second {
		t.Errorf("request took %v with catalog down — gRPC calls have no deadline (spec: ≤ 2s)", elapsed)
	}
}

// Kafka остановлена → POST /orders всё равно 201 (outbox буферизует).
// Kafka вернулась → событие доезжает, sold догоняет. Главный тест v4.
func TestAcc21_OutboxSurvivesKafkaOutage(t *testing.T) {
	skipIfNoCompose(t)
	seller := registerUser(t, "seller")
	buyer := registerUser(t, "buyer")
	id := createListing(t, seller, "outbox-target", 10, 5)

	composeStop(t, "kafka")
	restored := false
	t.Cleanup(func() {
		if !restored {
			composeStart(t, "kafka")
		}
		waitHealthy(t, 60*time.Second)
	})
	time.Sleep(3 * time.Second)

	start := time.Now()
	o := createOrder(t, buyer, id, 2) // 201 несмотря на мёртвую Kafka
	if el := time.Since(start); el > 5*time.Second {
		t.Errorf("POST /orders took %v with Kafka down — Kafka is on the synchronous path", el)
	}
	if l, _ := getListing(t, id); l.Stock != 3 {
		t.Errorf("stock reserved synchronously even with Kafka down: expected 3, got %d", l.Stock)
	}
	// sold не может обновиться без Kafka
	time.Sleep(3 * time.Second)
	if l, _ := getListing(t, id); l.Sold != 0 {
		t.Errorf("sold updated while Kafka was down?! got %d", l.Sold)
	}

	composeStart(t, "kafka")
	restored = true

	if !waitUntil(60*time.Second, 500*time.Millisecond, func() bool {
		l, _ := getListing(t, id)
		return l.Sold == 2
	}) {
		t.Fatalf("order %s created during Kafka outage never reached the consumer — outbox isn't draining after reconnect", o.ID)
	}
}
