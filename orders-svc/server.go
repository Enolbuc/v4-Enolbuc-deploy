package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	catalogpb "marketplace/gen/catalogpb"
	pb "marketplace/gen/orderspb"
)

type orderCreatedEvent struct {
	EventID   string `json:"event_id"`
	OrderID   string `json:"order_id"`
	BuyerID   string `json:"buyer_id"`
	ListingID string `json:"listing_id"`
	Qty       int64  `json:"qty"`
	CreatedAt string `json:"created_at"`
}

type server struct {
	pb.UnimplementedOrdersServer
	pool    *pgxpool.Pool
	catalog catalogpb.CatalogClient
}

func (s *server) getOrder(ctx context.Context, id string) (*pb.Order, error) {
	var (
		o         pb.Order
		createdAt time.Time
	)
	err := s.pool.QueryRow(ctx, `
		SELECT id, buyer_id, listing_id, qty, created_at FROM orders WHERE id = $1
	`, id).Scan(&o.Id, &o.BuyerId, &o.ListingId, &o.Qty, &createdAt)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "load order: %v", err)
	}
	o.CreatedAt = createdAt.UTC().Format(time.RFC3339)
	return &o, nil
}

// reserveIdempotencyKey резервирует пару (buyer_id, key) раньше, чем мы трогаем
// склад. Если пара новая — вызывающий код идёт создавать заказ как обычно.
// Если пара уже была — возвращает уже существующий заказ и replay=true.
func (s *server) reserveIdempotencyKey(ctx context.Context, buyerID, key string) (order *pb.Order, replay bool, err error) {
	tag, err := s.pool.Exec(ctx, `
		INSERT INTO idempotency_keys (buyer_id, key) VALUES ($1, $2)
		ON CONFLICT DO NOTHING
	`, buyerID, key)
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "reserve idempotency key: %v", err)
	}
	if tag.RowsAffected() > 0 {
		return nil, false, nil
	}

	var orderID *string
	err = s.pool.QueryRow(ctx, `
		SELECT order_id FROM idempotency_keys WHERE buyer_id = $1 AND key = $2
	`, buyerID, key).Scan(&orderID)
	if err != nil {
		return nil, false, status.Errorf(codes.Internal, "lookup idempotency key: %v", err)
	}
	if orderID == nil {
		return nil, false, status.Error(codes.Aborted, "order with this idempotency key is still being created, retry shortly")
	}

	existing, err := s.getOrder(ctx, *orderID)
	if err != nil {
		return nil, false, err
	}
	return existing, true, nil
}

// releaseIdempotencyKey снимает ключ, если заказ так и не был создан —
// чтобы повтор с тем же ключом мог пройти заново.
func (s *server) releaseIdempotencyKey(ctx context.Context, buyerID, key string) {
	if _, err := s.pool.Exec(ctx, `
		DELETE FROM idempotency_keys WHERE buyer_id = $1 AND key = $2 AND order_id IS NULL
	`, buyerID, key); err != nil {
		log.Printf("release idempotency key: %v", err)
	}
}

func (s *server) Create(ctx context.Context, req *pb.CreateOrderRequest) (*pb.CreateOrderResponse, error) {
	if req.Qty <= 0 {
		return nil, status.Error(codes.InvalidArgument, "qty must be positive")
	}

	key := req.IdempotencyKey
	if key != "" {
		existing, replay, err := s.reserveIdempotencyKey(ctx, req.BuyerId, key)
		if err != nil {
			return nil, err
		}
		if replay {
			grpc.SetHeader(ctx, metadata.Pairs("x-idempotent-replay", "true"))
			return &pb.CreateOrderResponse{Order: existing}, nil
		}
	}

	reserveCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if _, err := s.catalog.Reserve(reserveCtx, &catalogpb.ReserveRequest{
		ListingId: req.ListingId,
		Qty:       req.Qty,
	}); err != nil {
		if key != "" {
			s.releaseIdempotencyKey(ctx, req.BuyerId, key)
		}
		return nil, err
	}

	id := uuid.NewString()
	eventID := uuid.NewString()

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		if key != "" {
			s.releaseIdempotencyKey(ctx, req.BuyerId, key)
		}
		return nil, status.Errorf(codes.Internal, "begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	var createdAt time.Time
	err = tx.QueryRow(ctx, `
		INSERT INTO orders (id, buyer_id, listing_id, qty)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at
	`, id, req.BuyerId, req.ListingId, req.Qty).Scan(&createdAt)
	if err != nil {
		if key != "" {
			s.releaseIdempotencyKey(ctx, req.BuyerId, key)
		}
		return nil, status.Errorf(codes.Internal, "create order: %v", err)
	}

	payload, err := json.Marshal(orderCreatedEvent{
		EventID:   eventID,
		OrderID:   id,
		BuyerID:   req.BuyerId,
		ListingID: req.ListingId,
		Qty:       req.Qty,
		CreatedAt: createdAt.UTC().Format(time.RFC3339),
	})
	if err != nil {
		if key != "" {
			s.releaseIdempotencyKey(ctx, req.BuyerId, key)
		}
		return nil, status.Errorf(codes.Internal, "marshal event: %v", err)
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO outbox (event_id, topic, key, payload) VALUES ($1, $2, $3, $4)
	`, eventID, "order.created", id, payload); err != nil {
		if key != "" {
			s.releaseIdempotencyKey(ctx, req.BuyerId, key)
		}
		return nil, status.Errorf(codes.Internal, "write outbox: %v", err)
	}

	if key != "" {
		if _, err := tx.Exec(ctx, `
			UPDATE idempotency_keys SET order_id = $1 WHERE buyer_id = $2 AND key = $3
		`, id, req.BuyerId, key); err != nil {
			return nil, status.Errorf(codes.Internal, "link idempotency key: %v", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		if key != "" {
			s.releaseIdempotencyKey(ctx, req.BuyerId, key)
		}
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}

	return &pb.CreateOrderResponse{
		Order: &pb.Order{
			Id:        id,
			BuyerId:   req.BuyerId,
			ListingId: req.ListingId,
			Qty:       req.Qty,
			CreatedAt: createdAt.UTC().Format(time.RFC3339),
		},
	}, nil
}

func (s *server) List(ctx context.Context, req *pb.ListOrdersRequest) (*pb.ListOrdersResponse, error) {
	var total int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM orders WHERE buyer_id = $1`, req.BuyerId).Scan(&total); err != nil {
		return nil, status.Errorf(codes.Internal, "count orders: %v", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, buyer_id, listing_id, qty, created_at
		FROM orders WHERE buyer_id = $1
		ORDER BY created_at DESC LIMIT $2 OFFSET $3
	`, req.BuyerId, req.Limit, req.Offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list orders: %v", err)
	}
	defer rows.Close()

	items := make([]*pb.Order, 0)
	for rows.Next() {
		var (
			o         pb.Order
			createdAt time.Time
		)
		if err := rows.Scan(&o.Id, &o.BuyerId, &o.ListingId, &o.Qty, &createdAt); err != nil {
			return nil, status.Errorf(codes.Internal, "scan order: %v", err)
		}
		o.CreatedAt = createdAt.UTC().Format(time.RFC3339)
		items = append(items, &o)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "list orders: %v", err)
	}

	return &pb.ListOrdersResponse{Items: items, Total: total}, nil
}
