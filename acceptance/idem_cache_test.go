// Идемпотентность, кэш и eventual consistency — всё наблюдаемо через HTTP,
// поэтому эти тесты тоже идут против Railway. 15 дополнительно заглядывает в Redis.
package acceptance_test

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
)

// Один и тот же Idempotency-Key дважды → один заказ, остаток списан один раз.
func TestAcc13_IdempotencyKey(t *testing.T) {
	seller := registerUser(t, "seller")
	buyer := registerUser(t, "buyer")
	id := createListing(t, seller, "idem", 10, 10)
	key := uuid.NewString()
	body := map[string]any{"listing_id": id, "qty": 2}

	first := doHTTPHeaders(t, "POST", gatewayURL(t)+"/orders", buyer.Access, body, map[string]string{"Idempotency-Key": key})
	if first.status != 201 {
		t.Fatalf("first POST: expected 201, got %d. body: %s", first.status, first.body)
	}
	second := doHTTPHeaders(t, "POST", gatewayURL(t)+"/orders", buyer.Access, body, map[string]string{"Idempotency-Key": key})
	if second.status != 200 && second.status != 201 {
		t.Fatalf("retry with same key: expected 200/201, got %d. body: %s", second.status, second.body)
	}
	var o1, o2 order
	first.decode(t, &o1)
	second.decode(t, &o2)
	if o1.ID != o2.ID {
		t.Errorf("same Idempotency-Key must return the same order: %s vs %s", o1.ID, o2.ID)
	}
	if l, _ := getListing(t, id); l.Stock != 8 {
		t.Errorf("stock must be reserved once: expected 8, got %d", l.Stock)
	}

	// другой ключ — другой заказ
	third := doHTTPHeaders(t, "POST", gatewayURL(t)+"/orders", buyer.Access, body, map[string]string{"Idempotency-Key": uuid.NewString()})
	if third.status != 201 {
		t.Fatalf("new key: expected 201, got %d", third.status)
	}
	var o3 order
	third.decode(t, &o3)
	if o3.ID == o1.ID {
		t.Errorf("different key must create a different order")
	}
	if l, _ := getListing(t, id); l.Stock != 6 {
		t.Errorf("stock after second distinct order: expected 6, got %d", l.Stock)
	}

	// чужой ключ: тот же key от другого покупателя — это его собственный заказ
	buyer2 := registerUser(t, "buyer")
	fourth := doHTTPHeaders(t, "POST", gatewayURL(t)+"/orders", buyer2.Access, body, map[string]string{"Idempotency-Key": key})
	if fourth.status != 201 {
		t.Errorf("same key, different buyer: expected 201 (keys are per buyer), got %d. body: %s", fourth.status, fourth.body)
	}
}

// X-Cache: первый GET после создания — MISS, второй — HIT, после PATCH снова MISS.
func TestAcc14_CacheHeaderMissHitInvalidate(t *testing.T) {
	seller := registerUser(t, "seller")
	id := createListing(t, seller, "cache-hdr", 10, 5)

	_, r := getListing(t, id)
	if got := r.headers.Get("X-Cache"); got != "MISS" {
		t.Fatalf("first GET: expected X-Cache: MISS, got %q", got)
	}
	_, r = getListing(t, id)
	if got := r.headers.Get("X-Cache"); got != "HIT" {
		t.Fatalf("second GET: expected X-Cache: HIT, got %q — cache not populated on miss", got)
	}

	if p := doHTTP(t, "PATCH", gatewayURL(t)+"/listings/"+id, seller.Access, map[string]any{"price": 99.99}); p.status != 200 {
		t.Fatalf("PATCH: %d", p.status)
	}
	// После инвалидации первый GET снова MISS и уже с новой ценой. Допускаем
	// до 2 секунд на батчированную инвалидацию.
	ok := waitUntil(2*time.Second, 50*time.Millisecond, func() bool {
		l, r := getListing(t, id)
		return r.headers.Get("X-Cache") == "MISS" && l.Price == 99.99
	})
	if !ok {
		l, r := getListing(t, id)
		t.Fatalf("after PATCH: expected MISS with price 99.99, got %q price %v — stale cache, invalidation broken", r.headers.Get("X-Cache"), l.Price)
	}
	// удаление тоже инвалидирует
	if d := doHTTP(t, "DELETE", gatewayURL(t)+"/listings/"+id, seller.Access, nil); d.status != 204 {
		t.Fatalf("DELETE: %d", d.status)
	}
	if !waitUntil(2*time.Second, 50*time.Millisecond, func() bool {
		return doHTTP(t, "GET", gatewayURL(t)+"/listings/"+id, "", nil).status == 404
	}) {
		t.Errorf("deleted listing still served from cache")
	}
}

// Ключ listing:{id} появляется в Redis с TTL ≈ 30 с и исчезает после PATCH.
func TestAcc15_RedisKeyAndTTL(t *testing.T) {
	rc := newRedis(t)
	ctx := context.Background()

	seller := registerUser(t, "seller")
	id := createListing(t, seller, "cache-me", 10, 5)
	key := "listing:" + id
	rc.Del(ctx, key)

	getListing(t, id) // miss → SET

	var ttl time.Duration
	if !waitUntil(2*time.Second, 50*time.Millisecond, func() bool {
		v, err := rc.TTL(ctx, key).Result()
		ttl = v
		return err == nil && v > 0
	}) {
		t.Fatalf("key %q not in Redis or has no TTL", key)
	}
	if ttl < 5*time.Second || ttl > 35*time.Second {
		t.Errorf("TTL %v outside expected ~30s", ttl)
	}

	if p := doHTTP(t, "PATCH", gatewayURL(t)+"/listings/"+id, seller.Access, map[string]any{"price": 12.5}); p.status != 200 {
		t.Fatalf("PATCH: %d", p.status)
	}
	if !waitUntil(2*time.Second, 50*time.Millisecond, func() bool {
		_, err := rc.Get(ctx, key).Result()
		return err == redis.Nil
	}) {
		t.Errorf("key %q still present 2s after PATCH — no DEL on mutation", key)
	}
}

// Заказ → outbox → Kafka → консюмер в catalog-svc → поле sold у листинга.
// Единственный тест, который проверяет весь асинхронный путь без Kafka-клиента,
// поэтому он идёт и против Railway.
func TestAcc16_SoldIsEventuallyConsistent(t *testing.T) {
	seller := registerUser(t, "seller")
	buyer := registerUser(t, "buyer")
	id := createListing(t, seller, "sold-counter", 10, 10)

	if l, _ := getListing(t, id); l.Sold != 0 {
		t.Fatalf("fresh listing: expected sold=0, got %d", l.Sold)
	}
	createOrder(t, buyer, id, 3)
	createOrder(t, buyer, id, 2)

	// stock уменьшается синхронно, sold догоняет асинхронно
	if l, _ := getListing(t, id); l.Stock != 5 {
		t.Errorf("stock right after orders: expected 5, got %d", l.Stock)
	}
	if !waitUntil(15*time.Second, 200*time.Millisecond, func() bool {
		l, _ := getListing(t, id)
		return l.Sold == 5
	}) {
		l, _ := getListing(t, id)
		t.Fatalf("sold did not reach 5 within 15s (got %d) — outbox worker, Kafka or the consumer isn't wired", l.Sold)
	}
	// ещё 3 секунды: sold не должен уехать выше 5 (дубли событий обработаны идемпотентно)
	time.Sleep(3 * time.Second)
	if l, _ := getListing(t, id); l.Sold != 5 {
		t.Errorf("sold drifted to %d — consumer is not idempotent", l.Sold)
	}
}
