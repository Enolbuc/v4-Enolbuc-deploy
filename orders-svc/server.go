package main

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	catalogpb "marketplace/gen/catalogpb"
	pb "marketplace/gen/orderspb"
)

type server struct {
	pb.UnimplementedOrdersServer
	pool    *pgxpool.Pool
	catalog catalogpb.CatalogClient
}

func (s *server) Create(ctx context.Context, req *pb.CreateOrderRequest) (*pb.CreateOrderResponse, error) {
	if req.Qty <= 0 {
		return nil, status.Error(codes.InvalidArgument, "qty must be positive")
	}

	reserveCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	if _, err := s.catalog.Reserve(reserveCtx, &catalogpb.ReserveRequest{
		ListingId: req.ListingId,
		Qty:       req.Qty,
	}); err != nil {
		return nil, err
	}

	id := uuid.NewString()
	var createdAt time.Time
	err := s.pool.QueryRow(ctx, `
		INSERT INTO orders (id, buyer_id, listing_id, qty)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at
	`, id, req.BuyerId, req.ListingId, req.Qty).Scan(&createdAt)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create order: %v", err)
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
