package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

type orderCreatedEvent struct {
	EventID   string `json:"event_id"`
	OrderID   string `json:"order_id"`
	BuyerID   string `json:"buyer_id"`
	ListingID string `json:"listing_id"`
	Qty       int64  `json:"qty"`
	CreatedAt string `json:"created_at"`
}

func runOrderConsumer(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client, brokers []string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:           brokers,
		Topic:             "order.created",
		GroupID:           "catalog-svc",
		SessionTimeout:    6 * time.Second,
		RebalanceTimeout:  6 * time.Second,
		HeartbeatInterval: 2 * time.Second,
		Dialer: &kafka.Dialer{
			Timeout: 2 * time.Second,
		},
	})
	defer reader.Close()

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("consumer: fetch", "error", err)
			continue
		}

		if err := handleOrderCreated(ctx, pool, rdb, msg.Value); err != nil {
			slog.Error("consumer: handle", "error", err)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			slog.Error("consumer: commit offset", "error", err)
		}
	}
}

func handleOrderCreated(ctx context.Context, pool *pgxpool.Pool, rdb *redis.Client, payload []byte) error {
	var event orderCreatedEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		return err
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	tag, err := tx.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1) ON CONFLICT DO NOTHING`, event.EventID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return tx.Commit(ctx)
	}

	if _, err := tx.Exec(ctx, `UPDATE listings SET sold = sold + $1 WHERE id = $2`, event.Qty, event.ListingID); err != nil {
		return err
	}

	if err := tx.Commit(ctx); err != nil {
		return err
	}

	cacheDelListing(ctx, rdb, event.ListingID)
	return nil
}
