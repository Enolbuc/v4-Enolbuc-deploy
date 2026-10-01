package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	pb "marketplace/gen/catalogpb"
)

type listingsServer struct {
	catalog pb.CatalogClient
}

type listingResponse struct {
	ID        string  `json:"id"`
	SellerID  string  `json:"seller_id"`
	Title     string  `json:"title"`
	Price     float64 `json:"price"`
	Stock     int64   `json:"stock"`
	Sold      int64   `json:"sold"`
	CreatedAt string  `json:"created_at"`
}

func toListingResponse(l *pb.Listing) (listingResponse, error) {
	price, err := strconv.ParseFloat(l.Price, 64)
	if err != nil {
		return listingResponse{}, err
	}
	return listingResponse{
		ID: l.Id, SellerID: l.SellerId, Title: l.Title,
		Price: price, Stock: l.Stock, Sold: l.Sold, CreatedAt: l.CreatedAt,
	}, nil
}

func writeGRPCError(w http.ResponseWriter, err error) {
	st, ok := status.FromError(err)
	if !ok {
		writeError(w, http.StatusInternalServerError, "internal", "internal server error")
		return
	}
	switch st.Code() {
	case codes.NotFound:
		writeError(w, http.StatusNotFound, "not_found", st.Message())
	case codes.InvalidArgument:
		writeError(w, http.StatusBadRequest, "validation_failed", st.Message())
	case codes.PermissionDenied:
		writeError(w, http.StatusForbidden, "forbidden", st.Message())
	case codes.FailedPrecondition, codes.AlreadyExists, codes.Aborted:
		writeError(w, http.StatusConflict, "conflict", st.Message())
	case codes.Unavailable, codes.DeadlineExceeded:
		writeError(w, http.StatusServiceUnavailable, "unavailable", st.Message())
	default:
		writeError(w, http.StatusInternalServerError, "internal", "internal server error")
	}
}

type createListingRequest struct {
	Title string  `json:"title"`
	Price float64 `json:"price"`
	Stock int64   `json:"stock"`
}

func (s *listingsServer) create(w http.ResponseWriter, r *http.Request) {
	var req createListingRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "validation_failed", "invalid json body")
		return
	}

	var fields []fieldError
	if req.Title == "" {
		fields = append(fields, fieldError{"title", "must not be empty"})
	}
	if req.Price <= 0 {
		fields = append(fields, fieldError{"price", "must be positive"})
	}
	if req.Stock < 0 {
		fields = append(fields, fieldError{"stock", "must not be negative"})
	}
	if len(fields) > 0 {
		writeError(w, http.StatusBadRequest, "validation_failed", "request body is invalid", fields...)
		return
	}

	sellerID, _ := r.Context().Value(userIDKey).(string)

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	resp, err := s.catalog.Create(ctx, &pb.CreateListingRequest{
		SellerId: sellerID,
		Title:    req.Title,
		Price:    strconv.FormatFloat(req.Price, 'f', 2, 64),
		Stock:    req.Stock,
	})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	out, err := toListingResponse(resp.Listing)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "bad price from catalog")
		return
	}
	writeJSON(w, http.StatusCreated, out)
}

func (s *listingsServer) get(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	var md metadata.MD
	resp, err := s.catalog.Get(ctx, &pb.GetListingRequest{Id: id}, grpc.Header(&md))
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	out, err := toListingResponse(resp.Listing)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "bad price from catalog")
		return
	}

	if cache := md.Get("x-cache"); len(cache) > 0 {
		w.Header().Set("X-Cache", cache[0])
	}
	writeJSON(w, http.StatusOK, out)
}
func (s *listingsServer) list(w http.ResponseWriter, r *http.Request) {
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

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	resp, err := s.catalog.List(ctx, &pb.ListListingsRequest{Limit: limit, Offset: offset})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	items := make([]listingResponse, 0, len(resp.Items))
	for _, l := range resp.Items {
		out, err := toListingResponse(l)
		if err != nil {
			writeError(w, http.StatusInternalServerError, "internal", "bad price from catalog")
			return
		}
		items = append(items, out)
	}

	w.Header().Set("X-Total-Count", strconv.FormatInt(resp.Total, 10))
	writeJSON(w, http.StatusOK, items)
}

func (s *listingsServer) update(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var req struct {
		Title *string  `json:"title"`
		Price *float64 `json:"price"`
		Stock *int64   `json:"stock"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "validation_failed", "invalid json body")
		return
	}

	var fields []fieldError
	if req.Title != nil && *req.Title == "" {
		fields = append(fields, fieldError{"title", "must not be empty"})
	}
	if req.Price != nil && *req.Price <= 0 {
		fields = append(fields, fieldError{"price", "must be positive"})
	}
	if req.Stock != nil && *req.Stock < 0 {
		fields = append(fields, fieldError{"stock", "must not be negative"})
	}
	if len(fields) > 0 {
		writeError(w, http.StatusBadRequest, "validation_failed", "request body is invalid", fields...)
		return
	}

	sellerID, _ := r.Context().Value(userIDKey).(string)

	grpcReq := &pb.UpdateListingRequest{Id: id, SellerId: sellerID}
	if req.Title != nil {
		grpcReq.Title = req.Title
	}
	if req.Price != nil {
		priceStr := strconv.FormatFloat(*req.Price, 'f', 2, 64)
		grpcReq.Price = &priceStr
	}
	if req.Stock != nil {
		grpcReq.Stock = req.Stock
	}

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	resp, err := s.catalog.Update(ctx, grpcReq)
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	out, err := toListingResponse(resp.Listing)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal", "bad price from catalog")
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *listingsServer) delete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	sellerID, _ := r.Context().Value(userIDKey).(string)

	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()

	_, err := s.catalog.Delete(ctx, &pb.DeleteListingRequest{Id: id, SellerId: sellerID})
	if err != nil {
		writeGRPCError(w, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
