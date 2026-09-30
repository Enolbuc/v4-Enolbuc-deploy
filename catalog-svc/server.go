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

func (s *server) checkOwner(ctx context.Context, id, sellerID string) error {
	var ownerID string
	err := s.pool.QueryRow(ctx, `SELECT seller_id FROM listings WHERE id = $1`, id).Scan(&ownerID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return status.Error(codes.NotFound, "listing not found")
		}
		return status.Errorf(codes.Internal, "check listing: %v", err)
	}
	if ownerID != sellerID {
		return status.Error(codes.PermissionDenied, "not the owner")
	}
	return nil
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
func (s *server) Update(ctx context.Context, req *pb.UpdateListingRequest) (*pb.UpdateListingResponse, error) {
	if err := s.checkOwner(ctx, req.Id, req.SellerId); err != nil {
		return nil, err
	}

	row := s.pool.QueryRow(ctx, `
		UPDATE listings SET
			title = COALESCE($2, title),
			price = COALESCE($3, price),
			stock = COALESCE($4, stock)
		WHERE id = $1
		RETURNING id, seller_id, title, price, stock, sold, created_at
	`, req.Id, req.Title, req.Price, req.Stock)

	listing, err := scanListing(row)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "update listing: %v", err)
	}

	return &pb.UpdateListingResponse{Listing: listing}, nil
}
func (s *server) List(ctx context.Context, req *pb.ListListingsRequest) (*pb.ListListingsResponse, error) {
	var total int64
	if err := s.pool.QueryRow(ctx, `SELECT count(*) FROM listings`).Scan(&total); err != nil {
		return nil, status.Errorf(codes.Internal, "count listings: %v", err)
	}

	rows, err := s.pool.Query(ctx, `
		SELECT id, seller_id, title, price, stock, sold, created_at
		FROM listings ORDER BY created_at ASC LIMIT $1 OFFSET $2
	`, req.Limit, req.Offset)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "list listings: %v", err)
	}
	defer rows.Close()

	items := make([]*pb.Listing, 0)
	for rows.Next() {
		listing, err := scanListing(rows)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "scan listing: %v", err)
		}
		items = append(items, listing)
	}
	if err := rows.Err(); err != nil {
		return nil, status.Errorf(codes.Internal, "list listings: %v", err)
	}

	return &pb.ListListingsResponse{Items: items, Total: total}, nil
}
func (s *server) Delete(ctx context.Context, req *pb.DeleteListingRequest) (*pb.DeleteListingResponse, error) {
	if err := s.checkOwner(ctx, req.Id, req.SellerId); err != nil {
		return nil, err
	}

	if _, err := s.pool.Exec(ctx, `DELETE FROM listings WHERE id = $1`, req.Id); err != nil {
		return nil, status.Errorf(codes.Internal, "delete listing: %v", err)
	}

	return &pb.DeleteListingResponse{}, nil
}
func (s *server) Reserve(ctx context.Context, req *pb.ReserveRequest) (*pb.ReserveResponse, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "begin tx: %v", err)
	}
	defer tx.Rollback(ctx)

	var stock int64
	err = tx.QueryRow(ctx, `SELECT stock FROM listings WHERE id = $1 FOR UPDATE`, req.ListingId).Scan(&stock)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, status.Error(codes.NotFound, "listing not found")
		}
		return nil, status.Errorf(codes.Internal, "lock listing: %v", err)
	}

	if stock < req.Qty {
		return nil, status.Error(codes.FailedPrecondition, "insufficient stock")
	}

	remaining := stock - req.Qty
	if _, err := tx.Exec(ctx, `UPDATE listings SET stock = $2 WHERE id = $1`, req.ListingId, remaining); err != nil {
		return nil, status.Errorf(codes.Internal, "update stock: %v", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, status.Errorf(codes.Internal, "commit: %v", err)
	}

	return &pb.ReserveResponse{Remaining: remaining}, nil
}
