// End-to-end через HTTP gateway. Тестам всё равно, какой сервис что делает:
// снаружи это один API. Тесты 01–12 работают против любого стека, включая Railway.
package acceptance_test

import (
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

func TestAcc01_Healthz(t *testing.T) {
	r := doHTTP(t, "GET", gatewayURL(t)+"/healthz", "", nil)
	if r.status != 200 {
		t.Fatalf("expected 200 (gateway + both gRPC backends up), got %d. body: %s", r.status, r.body)
	}
}

func TestAcc02_RegisterLoginRefresh(t *testing.T) {
	u := registerUser(t, "buyer")
	if u.Access == u.Refresh {
		t.Errorf("access and refresh tokens must differ")
	}

	r := doHTTP(t, "POST", gatewayURL(t)+"/auth/login", "", map[string]any{
		"email": u.Email, "password": "hunter22",
	})
	if r.status != 200 {
		t.Fatalf("login: %d. body: %s", r.status, r.body)
	}
	if r = doHTTP(t, "POST", gatewayURL(t)+"/auth/login", "", map[string]any{
		"email": u.Email, "password": "wrong",
	}); r.status != 401 {
		t.Errorf("login with wrong password: expected 401, got %d", r.status)
	}

	r = doHTTP(t, "POST", gatewayURL(t)+"/auth/refresh", "", map[string]any{"refresh_token": u.Refresh})
	if r.status != 200 {
		t.Fatalf("refresh: %d. body: %s", r.status, r.body)
	}
	// refresh-токен на приватном роуте не принимается
	if r = doHTTP(t, "GET", gatewayURL(t)+"/orders", u.Refresh, nil); r.status != 401 {
		t.Errorf("refresh token on private route: expected 401, got %d", r.status)
	}
}

func TestAcc03_ListingCRUDProxied(t *testing.T) {
	seller := registerUser(t, "seller")
	buyer := registerUser(t, "buyer")
	gw := gatewayURL(t)

	if r := doHTTP(t, "POST", gw+"/listings", "", map[string]any{"title": "x", "price": 10.0, "stock": 1}); r.status != 401 {
		t.Errorf("no token POST /listings: expected 401, got %d", r.status)
	}
	if r := doHTTP(t, "POST", gw+"/listings", buyer.Access, map[string]any{"title": "x", "price": 10.0, "stock": 1}); r.status != 403 {
		t.Errorf("buyer POST /listings: expected 403, got %d", r.status)
	}

	id := createListing(t, seller, "Молоко", 50, 10)

	l, _ := getListing(t, id)
	if l.Title != "Молоко" || l.Stock != 10 || l.SellerID != seller.ID || l.Price != 50 {
		t.Errorf("GET listing mismatch: %+v", l)
	}

	other := registerUser(t, "seller")
	if r := doHTTP(t, "PATCH", gw+"/listings/"+id, other.Access, map[string]any{"price": 999.0}); r.status != 403 {
		t.Errorf("foreign PATCH: expected 403, got %d", r.status)
	}
	if r := doHTTP(t, "DELETE", gw+"/listings/"+id, other.Access, nil); r.status != 403 {
		t.Errorf("foreign DELETE: expected 403, got %d", r.status)
	}
	if r := doHTTP(t, "PATCH", gw+"/listings/"+id, seller.Access, map[string]any{"price": 55.0}); r.status != 200 {
		t.Errorf("own PATCH: expected 200, got %d. body: %s", r.status, r.body)
	}
	if l, _ = getListing(t, id); l.Price != 55 {
		t.Errorf("price after PATCH: expected 55, got %v", l.Price)
	}
	if r := doHTTP(t, "DELETE", gw+"/listings/"+id, seller.Access, nil); r.status != 204 {
		t.Errorf("own DELETE: expected 204, got %d", r.status)
	}
	if r := doHTTP(t, "GET", gw+"/listings/"+id, "", nil); r.status != 404 {
		t.Errorf("GET after DELETE: expected 404, got %d", r.status)
	}
}

func TestAcc04_OrderHappyPath(t *testing.T) {
	seller := registerUser(t, "seller")
	buyer := registerUser(t, "buyer")
	id := createListing(t, seller, "happy", 10, 10)

	o := createOrder(t, buyer, id, 3)
	if o.BuyerID != buyer.ID || o.ListingID != id || o.Qty != 3 || o.CreatedAt == "" {
		t.Errorf("order body mismatch: %+v", o)
	}

	// остаток уменьшен синхронно (Reserve через gRPC до ответа клиенту)
	if l, _ := getListing(t, id); l.Stock != 7 {
		t.Errorf("stock after order qty=3: expected 7, got %d", l.Stock)
	}

	// seller заказы не создаёт
	if r := doHTTP(t, "POST", gatewayURL(t)+"/orders", seller.Access, map[string]any{"listing_id": id, "qty": 1}); r.status != 403 {
		t.Errorf("seller POST /orders: expected 403, got %d", r.status)
	}
}

func TestAcc05_OrderOversell409(t *testing.T) {
	seller := registerUser(t, "seller")
	buyer := registerUser(t, "buyer")
	id := createListing(t, seller, "x", 10, 2)

	r := doHTTP(t, "POST", gatewayURL(t)+"/orders", buyer.Access, map[string]any{"listing_id": id, "qty": 5})
	if r.status != 409 {
		t.Fatalf("oversell: expected 409, got %d. body: %s", r.status, r.body)
	}
	var e struct {
		Error string `json:"error"`
	}
	r.decode(t, &e)
	if e.Error != "conflict" {
		t.Errorf("oversell error code: expected conflict, got %q", e.Error)
	}
	if l, _ := getListing(t, id); l.Stock != 2 {
		t.Errorf("failed order must not change stock: expected 2, got %d", l.Stock)
	}
	// заказ не должен был появиться
	r = doHTTP(t, "GET", gatewayURL(t)+"/orders", buyer.Access, nil)
	if r.status != 200 || strings.Contains(string(r.body), id) {
		t.Errorf("failed order must not be persisted: %d %s", r.status, r.body)
	}
}

// Stock=10, 100 параллельных заказов qty=1 → ровно 10 успехов.
// То же, что в v3, но теперь через HTTP → gateway → gRPC → catalog → FOR UPDATE.
func TestAcc06_OversellUnderConcurrency(t *testing.T) {
	seller := registerUser(t, "seller")
	buyer := registerUser(t, "buyer")
	id := createListing(t, seller, "hot", 10, 10)

	const n = 100
	var ok, conflict, other atomic.Int64
	var wg sync.WaitGroup
	start := make(chan struct{})
	for range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			r := doHTTP(t, "POST", gatewayURL(t)+"/orders", buyer.Access, map[string]any{"listing_id": id, "qty": 1})
			switch r.status {
			case 201:
				ok.Add(1)
			case 409:
				conflict.Add(1)
			default:
				other.Add(1)
				t.Logf("unexpected %d body=%s", r.status, r.body)
			}
		}()
	}
	close(start)
	wg.Wait()

	if other.Load() != 0 {
		t.Errorf("got %d responses outside {201,409}", other.Load())
	}
	if ok.Load() != 10 || conflict.Load() != 90 {
		t.Errorf("expected 10 created + 90 conflict, got %d + %d — catalog.Reserve isn't using row locking", ok.Load(), conflict.Load())
	}
	if l, _ := getListing(t, id); l.Stock != 0 {
		t.Errorf("stock after 10 successful orders: expected 0, got %d", l.Stock)
	}
}

func TestAcc07_ListOrdersScopedToBuyer(t *testing.T) {
	seller := registerUser(t, "seller")
	buyerA := registerUser(t, "buyer")
	buyerB := registerUser(t, "buyer")
	id := createListing(t, seller, "shared", 10, 10)

	oa := createOrder(t, buyerA, id, 1)
	createOrder(t, buyerB, id, 2)

	r := doHTTP(t, "GET", gatewayURL(t)+"/orders", buyerA.Access, nil)
	if r.status != 200 {
		t.Fatalf("list orders: %d. body: %s", r.status, r.body)
	}
	var rows []order
	r.decode(t, &rows)
	if len(rows) != 1 || rows[0].ID != oa.ID {
		t.Errorf("buyerA must see exactly their one order, got %+v", rows)
	}
	if got := r.headers.Get("X-Total-Count"); got != "1" {
		t.Errorf("X-Total-Count for buyerA: expected 1, got %q", got)
	}
	if r = doHTTP(t, "GET", gatewayURL(t)+"/orders", "", nil); r.status != 401 {
		t.Errorf("GET /orders without token: expected 401, got %d", r.status)
	}
}

func TestAcc08_PaginationAndTotalCount(t *testing.T) {
	seller := registerUser(t, "seller")
	gw := gatewayURL(t)

	base := doHTTP(t, "GET", gw+"/listings?limit=1&offset=0", "", nil)
	before, _ := strconv.Atoi(base.headers.Get("X-Total-Count"))

	for i := range 5 {
		createListing(t, seller, "page-"+strconv.Itoa(i), 1, 1)
	}
	r := doHTTP(t, "GET", gw+"/listings?limit=2&offset=0", "", nil)
	if r.status != 200 {
		t.Fatalf("list: %d", r.status)
	}
	var rows []listing
	r.decode(t, &rows)
	if len(rows) != 2 {
		t.Errorf("expected 2 rows, got %d", len(rows))
	}
	after, _ := strconv.Atoi(r.headers.Get("X-Total-Count"))
	if after-before != 5 {
		t.Errorf("X-Total-Count delta: expected 5, got %d (before %d, after %d)", after-before, before, after)
	}
	if r = doHTTP(t, "GET", gw+"/listings?limit=999", "", nil); r.status != 400 {
		t.Errorf("limit=999: expected 400, got %d", r.status)
	}
}

func TestAcc09_ErrorsAreJSON(t *testing.T) {
	gw := gatewayURL(t)
	seller := registerUser(t, "seller")

	r := doHTTP(t, "GET", gw+"/nope/nothing", "", nil)
	if r.status != 404 || !strings.Contains(r.headers.Get("Content-Type"), "application/json") {
		t.Errorf("unknown path: expected JSON 404, got %d %q", r.status, r.headers.Get("Content-Type"))
	}

	r = doHTTP(t, "POST", gw+"/listings", seller.Access, map[string]any{"title": "", "price": -1, "stock": -5})
	if r.status != 400 {
		t.Fatalf("invalid body: expected 400, got %d. body: %s", r.status, r.body)
	}
	var e struct {
		Error  string `json:"error"`
		Fields []struct {
			Field string `json:"field"`
		} `json:"fields"`
	}
	r.decode(t, &e)
	if e.Error != "validation_failed" || len(e.Fields) != 3 {
		t.Errorf("expected validation_failed with 3 fields, got %s", r.body)
	}

	// невалидный uuid — 404, а не 500 из gRPC
	if r = doHTTP(t, "GET", gw+"/listings/not-a-uuid", "", nil); r.status != 404 {
		t.Errorf("GET /listings/not-a-uuid: expected 404, got %d", r.status)
	}
	buyer := registerUser(t, "buyer")
	if r = doHTTP(t, "POST", gw+"/orders", buyer.Access, map[string]any{"listing_id": "00000000-0000-0000-0000-000000000000", "qty": 1}); r.status != 404 {
		t.Errorf("order for unknown listing: expected 404, got %d. body: %s", r.status, r.body)
	}
}

func TestAcc10_ServesFrontend(t *testing.T) {
	r := doHTTP(t, "GET", gatewayURL(t)+"/", "", nil)
	if r.status != 200 || !strings.Contains(r.headers.Get("Content-Type"), "text/html") {
		t.Fatalf("GET /: expected 200 text/html, got %d %q", r.status, r.headers.Get("Content-Type"))
	}
	if !strings.Contains(string(r.body), "Marketplace v4") {
		t.Errorf("GET / is not web/index.html from the repo (marker missing)")
	}
}

func TestAcc11_CORSPreflight(t *testing.T) {
	r := doHTTPHeaders(t, "OPTIONS", gatewayURL(t)+"/orders", "", nil, map[string]string{
		"Origin":                         "http://localhost:5173",
		"Access-Control-Request-Method":  "POST",
		"Access-Control-Request-Headers": "authorization,content-type,idempotency-key",
	})
	if r.status < 200 || r.status > 299 {
		t.Fatalf("preflight: expected 2xx, got %d", r.status)
	}
	if r.headers.Get("Access-Control-Allow-Origin") == "" || !strings.Contains(r.headers.Get("Access-Control-Allow-Methods"), "POST") {
		t.Errorf("preflight missing Access-Control-Allow-* headers: %v", r.headers)
	}
	if !strings.Contains(strings.ToLower(r.headers.Get("Access-Control-Allow-Headers")), "idempotency-key") {
		t.Errorf("Access-Control-Allow-Headers must include Idempotency-Key, got %q", r.headers.Get("Access-Control-Allow-Headers"))
	}
}

func TestAcc12_RequestIDEcho(t *testing.T) {
	r := doHTTPHeaders(t, "GET", gatewayURL(t)+"/healthz", "", nil, map[string]string{"X-Request-ID": "acc-12-fixed"})
	if got := r.headers.Get("X-Request-ID"); got != "acc-12-fixed" {
		t.Errorf("X-Request-ID echo: expected acc-12-fixed, got %q", got)
	}
	r = doHTTP(t, "GET", gatewayURL(t)+"/healthz", "", nil)
	if r.headers.Get("X-Request-ID") == "" {
		t.Errorf("server must generate X-Request-ID when absent")
	}
}
