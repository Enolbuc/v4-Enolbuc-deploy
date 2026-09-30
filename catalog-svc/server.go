package main

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "marketplace/gen/catalogpb"
)

type server struct {
	pb.UnimplementedCatalogServer
	pool *pgxpool.Pool
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanListing(row rowScanner) (*pb.Listing, error) {
	var (
		l         pb.Listing
		createdAt time.Time
	)
	if err := row.Scan(&l.Id, &l.SellerId, &l.Title, &l.Price, &l.Stock, &l.Sold, &createdAt); err != nil {
		return nil, err
	}
	l.CreatedAt = createdAt.UTC().Format(time.RFC3339)
	return &l, nil
}
func (s *server) Create(ctx context.Context, req *pb.CreateListingRequest) (*pb.CreateListingResponse, error) {
	if req.Title == "" || req.Price == "" || req.Stock < 0 {
		return nil, status.Error(codes.InvalidArgument, "title, price and non-negative stock are required")
	}

	id := uuid.NewString()

	row := s.pool.QueryRow(ctx, `
		INSERT INTO listings (id, seller_id, title, price, stock, sold)
		VALUES ($1, $2, $3, $4, $5, 0)
		RETURNING id, seller_id, title, price, stock, sold, created_at
	`, id, req.SellerId, req.Title, req.Price, req.Stock)

	listing, err := scanListing(row)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "create listing: %v", err)
	}

	return &pb.CreateListingResponse{Listing: listing}, nil
}
func (s *server) Get(ctx context.Context, req *pb.GetListingRequest) (*pb.GetListingResponse, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT id, seller_id, title, price, stock, sold, created_at
		FROM listings WHERE id = $1
	`, req.Id)

	listing, err := scanListing(row)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "listing not found")
		}
		return nil, status.Errorf(codes.Internal, "get listing: %v", err)
	}

	return &pb.GetListingResponse{Listing: listing}, nil
}
