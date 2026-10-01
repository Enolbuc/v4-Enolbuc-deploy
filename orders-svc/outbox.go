package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

type outboxRow struct {
	ID      int64
	Topic   string
	Key     string
	Payload []byte
}

func runOutboxWorker(ctx context.Context, pool *pgxpool.Pool, writer *kafka.Writer) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publishPending(ctx, pool, writer)
		}
	}
}

func publishPending(ctx context.Context, pool *pgxpool.Pool, writer *kafka.Writer) {
	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Printf("outbox: begin tx: %v", err)
		return
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, topic, key, payload FROM outbox
		WHERE published_at IS NULL ORDER BY id LIMIT 100
		FOR UPDATE SKIP LOCKED
	`)
	if err != nil {
		log.Printf("outbox: select: %v", err)
		return
	}

	var pending []outboxRow
	for rows.Next() {
		var r outboxRow
		if err := rows.Scan(&r.ID, &r.Topic, &r.Key, &r.Payload); err != nil {
			rows.Close()
			log.Printf("outbox: scan: %v", err)
			return
		}
		pending = append(pending, r)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		log.Printf("outbox: rows: %v", err)
		return
	}
	if len(pending) == 0 {
		return
	}

	messages := make([]kafka.Message, len(pending))
	ids := make([]int64, len(pending))
	for i, r := range pending {
		messages[i] = kafka.Message{Topic: r.Topic, Key: []byte(r.Key), Value: r.Payload}
		ids[i] = r.ID
	}

	if err := writer.WriteMessages(ctx, messages...); err != nil {
		log.Printf("outbox: publish: %v", err)
		return
	}

	if _, err := tx.Exec(ctx, `UPDATE outbox SET published_at = now() WHERE id = ANY($1)`, ids); err != nil {
		log.Printf("outbox: mark published: %v", err)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		log.Printf("outbox: commit: %v", err)
	}
}
