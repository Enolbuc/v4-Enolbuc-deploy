package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	pb "marketplace/gen/catalogpb"
)

const cacheTTL = 30 * time.Second

func cacheKey(id string) string {
	return "listing:" + id
}

// cacheGetListing возвращает листинг из Redis и статус: "HIT" (нашли),
// "MISS" (Redis доступен, но ключа нет) или "BYPASS" (Redis недоступен —
// кэш в этом запросе не участвует вообще).
func cacheGetListing(ctx context.Context, rdb *redis.Client, id string) (*pb.Listing, string) {
	cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	data, err := rdb.Get(cctx, cacheKey(id)).Bytes()
	switch {
	case err == nil:
		var l pb.Listing
		if jsonErr := json.Unmarshal(data, &l); jsonErr != nil {
			slog.Warn("cache: corrupt value", "id", id, "error", jsonErr)
			return nil, "BYPASS"
		}
		return &l, "HIT"
	case errors.Is(err, redis.Nil):
		return nil, "MISS"
	default:
		slog.Warn("cache: get failed", "id", id, "error", err)
		return nil, "BYPASS"
	}
}

func cacheSetListing(ctx context.Context, rdb *redis.Client, l *pb.Listing) {
	data, err := json.Marshal(l)
	if err != nil {
		slog.Warn("cache: marshal failed", "id", l.Id, "error", err)
		return
	}

	cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	if err := rdb.Set(cctx, cacheKey(l.Id), data, cacheTTL).Err(); err != nil {
		slog.Warn("cache: set failed", "id", l.Id, "error", err)
	}
}

func cacheDelListing(ctx context.Context, rdb *redis.Client, id string) {
	cctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	if err := rdb.Del(cctx, cacheKey(id)).Err(); err != nil {
		slog.Warn("cache: del failed", "id", id, "error", err)
	}
}
