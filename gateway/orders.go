package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	pb "marketplace/gen/orderspb"
)

type ordersServer struct {
	orders pb.OrdersClient
}

type orderResponse struct {
	ID        string `json:"id"`
	BuyerID   string `json:"buyer_id"`
	ListingID string `json:"listing_id"`
	Qty       int64  `json:"qty"`
	CreatedAt string `json:"created_at"`
}

func toOrderResponse(o *pb.Order) orderResponse {
	return orderResponse{
		ID: o.Id, BuyerID: o.BuyerId, ListingID: o.ListingId,
		Qty: o.Qty, CreatedAt: o.CreatedAt,
	}
}

type createOrderRequest struct {
	ListingID string `json:"listing_id"`
	Qty       int64  `json:"qty"`
}

func (s *ordersServer) create(w http.ResponseWriter, r *http.Request) {
	var req createOrderRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "validation_failed", "invalid json body")
		return
	}

	if req.Qty <= 0 {
		writeError(w, http.StatusBadRequest, "validation_failed", "request body is invalid", fieldError{"qty", "must be positive"})
		return
	}

	buyerID, _ := r.Context().Value(userIDKey).(string)

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	var md metadata.MD
	resp, err := s.orders.Create(ctx, &pb.CreateOrderRequest{
		BuyerId:        buyerID,
		ListingId:      req.ListingID,
		Qty:            req.Qty,
		IdempotencyKey: r.Header.Get("Idempotency-Key"),
	}, grpc.Header(&md))
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	httpStatus := http.StatusCreated
	if replay := md.Get("x-idempotent-replay"); len(replay) > 0 && replay[0] == "true" {
		w.Header().Set("Idempotent-Replayed", "true")
		httpStatus = http.StatusOK
	}

	writeJSON(w, httpStatus, toOrderResponse(resp.Order))
}

func (s *ordersServer) list(w http.ResponseWriter, r *http.Request) {
	limit := int64(20)
	offset := int64(0)

	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 1 || n > 100 {
			writeError(w, http.StatusBadRequest, "validation_failed", "request body is invalid", fieldError{"limit", "must be between 1 and 100"})
			return
		}
		limit = n
	}
	if v := r.URL.Query().Get("offset"); v != "" {
		n, err := strconv.ParseInt(v, 10, 64)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, "validation_failed", "request body is invalid", fieldError{"offset", "must not be negative"})
			return
		}
		offset = n
	}

	buyerID, _ := r.Context().Value(userIDKey).(string)

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	resp, err := s.orders.List(ctx, &pb.ListOrdersRequest{BuyerId: buyerID, Limit: limit, Offset: offset})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	items := make([]orderResponse, 0, len(resp.Items))
	for _, o := range resp.Items {
		items = append(items, toOrderResponse(o))
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(resp.Total, 10))
	writeJSON(w, http.StatusOK, items)
}
