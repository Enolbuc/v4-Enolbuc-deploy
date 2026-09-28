// Прямая проверка топика order.created. Нужен KAFKA_BROKERS, иначе skip.
package acceptance_test

import (
	"testing"
	"time"
)

func TestAcc17_OrderCreatedEventSchema(t *testing.T) {
	kafkaBrokers(t)
	seller := registerUser(t, "seller")
	buyer := registerUser(t, "buyer")
	id := createListing(t, seller, "event-target", 10, 5)
	o := createOrder(t, buyer, id, 2)

	evs := waitForOrderEvents(t, o.ID, 15*time.Second)
	if len(evs) == 0 {
		t.Fatalf("no order.created event for order %s within 15s — outbox worker not publishing?", o.ID)
	}
	e := evs[0]
	if e.key != o.ID {
		t.Errorf("message key must be order_id (%s), got %q", o.ID, e.key)
	}
	for _, f := range []string{"event_id", "order_id", "buyer_id", "listing_id", "qty", "created_at"} {
		if _, ok := e.raw[f]; !ok {
			t.Errorf("event missing field %q: %v", f, e.raw)
		}
	}
	if q, _ := e.raw["qty"].(float64); int(q) != 2 {
		t.Errorf("event qty: expected 2, got %v", e.raw["qty"])
	}
	if e.raw["buyer_id"] != buyer.ID || e.raw["listing_id"] != id {
		t.Errorf("event ids mismatch: %v", e.raw)
	}
}

// В штатном режиме один заказ = одно событие. Дубли допустимы только после
// падения воркера между publish и UPDATE (лабораторная), не в steady state.
func TestAcc18_OneEventPerOrder(t *testing.T) {
	kafkaBrokers(t)
	seller := registerUser(t, "seller")
	buyer := registerUser(t, "buyer")
	id := createListing(t, seller, "one-event", 10, 1)
	o := createOrder(t, buyer, id, 1)

	if evs := waitForOrderEvents(t, o.ID, 15*time.Second); len(evs) == 0 {
		t.Fatalf("event for %s never arrived", o.ID)
	}
	time.Sleep(3 * time.Second) // дать воркеру шанс опубликовать дубль
	all := readEvents(t, 5*time.Second)
	count := 0
	for _, e := range all {
		if e.raw["order_id"] == o.ID {
			count++
		}
	}
	if count != 1 {
		t.Errorf("expected exactly 1 event for the order, got %d — worker publishes before marking or retries without checking published_at", count)
	}
}
