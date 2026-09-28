// Нагрузочная утилита для лабораторной (lab/LAB.md). Только стандартная библиотека,
// в базу, Redis и Kafka не ходит: всё через HTTP gateway, как обычный клиент.
//
//	go run . -base http://localhost:8080 -mode get    -n 5000 -c 50
//	go run . -base http://localhost:8080 -mode orders -n 500  -c 20 -stock 1000
//
// Режимы:
//
//	get     — создаёт -listings листингов и бьёт GET /listings/{id} случайно по ним.
//	          Печатает распределение X-Cache (HIT/MISS/BYPASS) и перцентили.
//	orders  — создаёт один листинг с -stock и шлёт POST /orders qty=1 от -buyers покупателей.
//	          Печатает статусы (200/409/5xx), перцентили, id листинга для проверки sold.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var client = &http.Client{Timeout: 15 * time.Second}

func main() {
	base := flag.String("base", "http://localhost:8080", "gateway URL")
	mode := flag.String("mode", "get", "get | orders")
	n := flag.Int("n", 2000, "total requests")
	c := flag.Int("c", 20, "concurrency")
	listings := flag.Int("listings", 100, "get: how many listings to create and read")
	stock := flag.Int("stock", 1000, "orders: stock of the single listing")
	buyers := flag.Int("buyers", 10, "orders: how many buyer accounts")
	flag.Parse()
	*base = strings.TrimRight(*base, "/")

	if err := ping(*base); err != nil {
		fail("gateway not healthy: %v", err)
	}
	seller := register(*base, "seller")

	switch *mode {
	case "get":
		ids := make([]string, 0, *listings)
		for i := range *listings {
			ids = append(ids, createListing(*base, seller, fmt.Sprintf("load-%d", i), 10))
		}
		fmt.Printf("created %d listings, firing %d GETs with concurrency %d\n", len(ids), *n, *c)
		run(*n, *c, func(r *rand.Rand) result {
			id := ids[r.IntN(len(ids))]
			t0 := time.Now()
			resp, err := client.Get(*base + "/listings/" + id)
			if err != nil {
				return result{err: err, dur: time.Since(t0)}
			}
			_, _ = io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
			return result{status: resp.StatusCode, tag: resp.Header.Get("X-Cache"), dur: time.Since(t0)}
		})
	case "orders":
		id := createListing(*base, seller, "load-hot", *stock)
		toks := make([]string, 0, *buyers)
		for range *buyers {
			toks = append(toks, register(*base, "buyer"))
		}
		fmt.Printf("listing %s stock=%d, firing %d POST /orders with concurrency %d\n", id, *stock, *n, *c)
		run(*n, *c, func(r *rand.Rand) result {
			tok := toks[r.IntN(len(toks))]
			t0 := time.Now()
			status, _, err := do("POST", *base+"/orders", tok, map[string]any{"listing_id": id, "qty": 1})
			return result{status: status, err: err, dur: time.Since(t0)}
		})
		fmt.Printf("check eventual consistency: curl %s/listings/%s   (stock vs sold)\n", *base, id)
	default:
		fail("unknown -mode %q", *mode)
	}
}

type result struct {
	status int
	tag    string
	dur    time.Duration
	err    error
}

func run(n, c int, one func(*rand.Rand) result) {
	var (
		mu       sync.Mutex
		durs     = make([]time.Duration, 0, n)
		statuses = map[string]int{}
		next     atomic.Int64
		wg       sync.WaitGroup
	)
	start := time.Now()
	for w := range c {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), uint64(time.Now().UnixNano())))
			for {
				if next.Add(1) > int64(n) {
					return
				}
				res := one(r)
				key := fmt.Sprint(res.status)
				if res.err != nil {
					key = "error"
				}
				if res.tag != "" {
					key += " " + res.tag
				}
				mu.Lock()
				durs = append(durs, res.dur)
				statuses[key]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	total := time.Since(start)

	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	p := func(q float64) time.Duration { return durs[int(float64(len(durs)-1)*q)] }
	fmt.Printf("\n%d requests in %v  (%.0f req/s)\n", len(durs), total.Round(time.Millisecond), float64(len(durs))/total.Seconds())
	fmt.Printf("p50 %v   p95 %v   p99 %v   max %v\n", p(0.5).Round(100*time.Microsecond), p(0.95).Round(100*time.Microsecond), p(0.99).Round(100*time.Microsecond), durs[len(durs)-1].Round(100*time.Microsecond))
	keys := make([]string, 0, len(statuses))
	for k := range statuses {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-12s %6d  (%.1f%%)\n", k, statuses[k], 100*float64(statuses[k])/float64(len(durs)))
	}
}

func ping(base string) error {
	resp, err := client.Get(base + "/healthz")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("/healthz returned %d", resp.StatusCode)
	}
	return nil
}

func register(base, role string) string {
	email := fmt.Sprintf("load-%s-%d-%d@test.local", role, time.Now().UnixNano(), rand.IntN(1<<30))
	status, body, err := do("POST", base+"/auth/register", "", map[string]any{"email": email, "password": "hunter22", "role": role})
	if err != nil || status != 201 {
		fail("register %s: status=%d err=%v body=%s", role, status, err, body)
	}
	var got struct {
		Access string `json:"access_token"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		fail("register: bad json: %v", err)
	}
	return got.Access
}

func createListing(base, token, title string, stock int) string {
	status, body, err := do("POST", base+"/listings", token, map[string]any{"title": title, "price": 10.00, "stock": stock})
	if err != nil || status != 201 {
		fail("create listing: status=%d err=%v body=%s", status, err, body)
	}
	var got struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(body, &got); err != nil {
		fail("create listing: bad json: %v", err)
	}
	return got.ID
}

func do(method, url, token string, body any) (int, []byte, error) {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest(method, url, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw, nil
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
