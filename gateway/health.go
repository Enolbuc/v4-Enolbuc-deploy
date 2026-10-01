package main

import (
	"context"
	"net/http"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func healthHandler(catalogConn, ordersConn *grpc.ClientConn) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
		defer cancel()

		catalogHealth := healthpb.NewHealthClient(catalogConn)
		ordersHealth := healthpb.NewHealthClient(ordersConn)

		cResp, cErr := catalogHealth.Check(ctx, &healthpb.HealthCheckRequest{})
		oResp, oErr := ordersHealth.Check(ctx, &healthpb.HealthCheckRequest{})

		if cErr != nil || oErr != nil ||
			cResp.Status != healthpb.HealthCheckResponse_SERVING ||
			oResp.Status != healthpb.HealthCheckResponse_SERVING {
			writeError(w, http.StatusServiceUnavailable, "unavailable", "backend not healthy")
			return
		}

		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
}
