// Хелперы acceptance-тестов marketplace v4.
//
// В отличие от v1–v3 тесты НЕ запускают бинарь сами: стек из трёх сервисов,
// Kafka, Redis и Postgres поднимается заранее (docker compose или Railway),
// а тесты ходят в него как обычный клиент. Что читается из окружения:
//
//	GATEWAY_URL     обязательно   http://localhost:8080 или https://<gateway>.up.railway.app
//	KAFKA_BROKERS   опционально   localhost:9094 — включает тесты, читающие топик напрямую
//	REDIS_URL       опционально   redis://localhost:6380 — включает тесты, смотрящие в Redis
//	COMPOSE_FILE    опционально   абсолютный путь к compose/docker-compose.yml — включает
//	                              resilience-тесты, которые останавливают части стека
//
// Без опциональных переменных соответствующие тесты делают t.Skip: так один и тот же
// набор гоняется и против локального compose (всё включено), и против Railway
// (только то, что видно через HTTP, а это и есть большая часть контракта).
package acceptance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

const topic = "order.created"

var httpClient = &http.Client{Timeout: 30 * time.Second}

// TestMain ждёт живой gateway до 90 секунд: на Railway первый запрос после сна
// будит цепочку gateway → catalog-svc → redis и может вернуть 502.
func TestMain(m *testing.M) {
	gw := os.Getenv("GATEWAY_URL")
	if gw == "" {
		fmt.Fprintln(os.Stderr, "GATEWAY_URL not set; bring the stack up (make up) and export it, see README")
		os.Exit(2)
	}
	if err := waitHealthyErr(strings.TrimRight(gw, "/"), 90*time.Second); err != nil {
		fmt.Fprintf(os.Stderr, "stack is not healthy: %v\n", err)
		os.Exit(2)
	}
	os.Exit(m.Run())
}

// ----- env -----

func gatewayURL(t *testing.T) string {
	t.Helper()
	return strings.TrimRight(os.Getenv("GATEWAY_URL"), "/")
}

func kafkaBrokers(t *testing.T) []string {
	t.Helper()
	v := os.Getenv("KAFKA_BROKERS")
	if v == "" {
		t.Skip("KAFKA_BROKERS not set — skipping test that reads the topic directly")
	}
	return strings.Split(v, ",")
}

func redisURL(t *testing.T) string {
	t.Helper()
	v := os.Getenv("REDIS_URL")
	if v == "" {
		t.Skip("REDIS_URL not set — skipping test that inspects Redis directly")
	}
	return v
}

// ----- HTTP client -----

type response struct {
	status  int
	headers http.Header
	body    []byte
}

func doHTTP(t *testing.T, method, url, token string, body any) response {
	t.Helper()
	return doHTTPHeaders(t, method, url, token, body, nil)
}

func doHTTPHeaders(t *testing.T, method, url, token string, body any, extra map[string]string) response {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, url, rdr)
	if err != nil {
		t.Fatalf("new req: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for k, v := range extra {
		req.Header.Set(k, v)
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return response{status: resp.StatusCode, headers: resp.Header, body: raw}
}

func (r response) decode(t *testing.T, into any) {
	t.Helper()
	if err := json.Unmarshal(r.body, into); err != nil {
		t.Fatalf("decode: %v\nbody: %s", err, r.body)
	}
}

// ----- fixtures -----

type user struct {
	ID      string
	Email   string
	Role    string
	Access  string
	Refresh string
}

var emailSeq atomic.Int64

func uniqueEmail(role string) string {
	return fmt.Sprintf("%s-%d-%d@test.local", role, time.Now().UnixNano(), emailSeq.Add(1))
}

func registerUser(t *testing.T, role string) *user {
	t.Helper()
	email := uniqueEmail(role)
	r := doHTTP(t, "POST", gatewayURL(t)+"/auth/register", "", map[string]any{
		"email": email, "password": "hunter22", "role": role,
	})
	if r.status != 201 {
		t.Fatalf("register %s: %d. body: %s", role, r.status, r.body)
	}
	var got struct {
		Access  string `json:"access_token"`
		Refresh string `json:"refresh_token"`
		User    struct {
			ID    string `json:"id"`
			Email string `json:"email"`
			Role  string `json:"role"`
		} `json:"user"`
	}
	r.decode(t, &got)
	if got.Access == "" || got.Refresh == "" || got.User.ID == "" {
		t.Fatalf("register response incomplete: %s", r.body)
	}
	return &user{ID: got.User.ID, Email: email, Role: role, Access: got.Access, Refresh: got.Refresh}
}

func createListing(t *testing.T, seller *user, title string, price float64, stock int) string {
	t.Helper()
	r := doHTTP(t, "POST", gatewayURL(t)+"/listings", seller.Access, map[string]any{
		"title": title, "price": price, "stock": stock,
	})
	if r.status != 201 {
		t.Fatalf("create listing: %d. body: %s", r.status, r.body)
	}
	var got struct {
		ID string `json:"id"`
	}
	r.decode(t, &got)
	return got.ID
}

type listing struct {
	ID       string  `json:"id"`
	Title    string  `json:"title"`
	Price    float64 `json:"price"`
	Stock    int     `json:"stock"`
	Sold     int     `json:"sold"`
	SellerID string  `json:"seller_id"`
}

func getListing(t *testing.T, id string) (listing, response) {
	t.Helper()
	r := doHTTP(t, "GET", gatewayURL(t)+"/listings/"+id, "", nil)
	if r.status != 200 {
		t.Fatalf("GET /listings/%s: %d. body: %s", id, r.status, r.body)
	}
	var l listing
	r.decode(t, &l)
	return l, r
}

type order struct {
	ID        string `json:"id"`
	BuyerID   string `json:"buyer_id"`
	ListingID string `json:"listing_id"`
	Qty       int    `json:"qty"`
	CreatedAt string `json:"created_at"`
}

// createOrder делает POST /orders и требует 201. Для сценариев с ожидаемой
// ошибкой используй doHTTP напрямую.
func createOrder(t *testing.T, buyer *user, listingID string, qty int) order {
	t.Helper()
	r := doHTTP(t, "POST", gatewayURL(t)+"/orders", buyer.Access, map[string]any{
		"listing_id": listingID, "qty": qty,
	})
	if r.status != 201 {
		t.Fatalf("POST /orders: expected 201, got %d. body: %s", r.status, r.body)
	}
	var o order
	r.decode(t, &o)
	if o.ID == "" {
		t.Fatalf("order without id: %s", r.body)
	}
	return o
}

// ----- kafka -----

type event struct {
	raw       map[string]any
	key       string
	partition int
	offset    int64
}

// readEvents читает весь топик с начала по всем партициям, без consumer group:
// join группы в kafka-go занимает секунды, а нам нужно просто «всё, что есть».
// Возвращает всё, что удалось прочитать до таймаута.
func readEvents(t *testing.T, timeout time.Duration) []event {
	t.Helper()
	brokers := kafkaBrokers(t)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		t.Fatalf("kafka dial: %v", err)
	}
	parts, err := conn.ReadPartitions(topic)
	conn.Close()
	if err != nil {
		return nil // топика ещё нет
	}

	var out []event
	for _, p := range parts {
		leader, err := kafka.DialLeader(ctx, "tcp", brokers[0], topic, p.ID)
		if err != nil {
			t.Fatalf("dial leader: %v", err)
		}
		last, err := leader.ReadLastOffset()
		leader.Close()
		if err != nil {
			t.Fatalf("last offset: %v", err)
		}
		if last == 0 {
			continue
		}
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:   brokers,
			Topic:     topic,
			Partition: p.ID,
			MinBytes:  1,
			MaxBytes:  10e6,
			MaxWait:   200 * time.Millisecond,
		})
		if err := r.SetOffset(kafka.FirstOffset); err != nil {
			r.Close()
			t.Fatalf("set offset: %v", err)
		}
		for {
			m, err := r.ReadMessage(ctx)
			if err != nil {
				break
			}
			var v map[string]any
			if err := json.Unmarshal(m.Value, &v); err != nil {
				t.Errorf("kafka message is not JSON: %s", m.Value)
			} else {
				out = append(out, event{raw: v, key: string(m.Key), partition: m.Partition, offset: m.Offset})
			}
			if m.Offset >= last-1 {
				break
			}
		}
		r.Close()
	}
	return out
}

// waitForOrderEvents ждёт, пока в топике появится хотя бы одно событие с этим
// order_id, и возвращает все такие события (для подсчёта дублей).
func waitForOrderEvents(t *testing.T, orderID string, timeout time.Duration) []event {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil
		}
		window := 4 * time.Second
		if remaining < window {
			window = remaining
		}
		var found []event
		for _, e := range readEvents(t, window) {
			if e.raw["order_id"] == orderID {
				found = append(found, e)
			}
		}
		if len(found) > 0 {
			return found
		}
	}
}

// ----- redis -----

func newRedis(t *testing.T) *redis.Client {
	t.Helper()
	opt, err := redis.ParseURL(redisURL(t))
	if err != nil {
		t.Fatalf("parse REDIS_URL: %v", err)
	}
	cli := redis.NewClient(opt)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cli.Ping(ctx).Err(); err != nil {
		t.Fatalf("redis ping: %v", err)
	}
	t.Cleanup(func() { cli.Close() })
	return cli
}

// ----- docker compose orchestration -----

func composeFile() string { return os.Getenv("COMPOSE_FILE") }

func skipIfNoCompose(t *testing.T) {
	t.Helper()
	if composeFile() == "" {
		t.Skip("COMPOSE_FILE not set — skipping test that stops parts of the stack")
	}
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	full := append([]string{"compose", "-f", composeFile(), "--profile", "services"}, args...)
	cmd := exec.Command("docker", full...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("docker %s failed: %v\n%s", strings.Join(full, " "), err, out)
	}
}

func composeStop(t *testing.T, service string)  { t.Helper(); compose(t, "stop", "-t", "15", service) }
func composeStart(t *testing.T, service string) { t.Helper(); compose(t, "start", service) }

// waitHealthy ждёт 200 от /healthz. Используется после рестартов частей стека.
func waitHealthy(t *testing.T, timeout time.Duration) {
	t.Helper()
	if err := waitHealthyErr(gatewayURL(t), timeout); err != nil {
		t.Fatal(err)
	}
}

func waitHealthyErr(gw string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		resp, err := httpClient.Get(gw + "/healthz")
		if err == nil {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if resp.StatusCode == 200 {
				return nil
			}
			last = fmt.Sprintf("%d %s", resp.StatusCode, strings.TrimSpace(string(body)))
		} else {
			last = err.Error()
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("%s/healthz did not become 200 within %v (last: %s)", gw, timeout, last)
}

// waitUntil поллит cond до таймаута.
func waitUntil(timeout, step time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(step)
	}
	return false
}
