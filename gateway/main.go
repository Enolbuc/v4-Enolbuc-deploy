package main

import (
	"context"
	"embed"
	"fmt"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "marketplace/gen/catalogpb"
	"marketplace/internal/migrate"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

func requireEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		fmt.Fprintf(os.Stderr, "%s is required\n", key)
		os.Exit(1)
	}
	return v
}

func main() {
	jwtSecret := requireEnv("JWT_SECRET")
	databaseURL := requireEnv("DATABASE_URL")
	catalogAddr := requireEnv("CATALOG_GRPC_ADDR")
	ordersAddr := requireEnv("ORDERS_GRPC_ADDR")

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect to postgres: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := migrate.Run(ctx, pool, migrationsFS, "migrations"); err != nil {
		fmt.Fprintf(os.Stderr, "run migrations: %v\n", err)
		os.Exit(1)
	}

	catalogConn, err := grpc.NewClient(catalogAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial catalog-svc: %v\n", err)
		os.Exit(1)
	}
	defer catalogConn.Close()

	ordersConn, err := grpc.NewClient(ordersAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial orders-svc: %v\n", err)
		os.Exit(1)
	}
	defer ordersConn.Close()

	auth := &authServer{pool: pool, jwtSecret: []byte(jwtSecret)}
	listings := &listingsServer{catalog: pb.NewCatalogClient(catalogConn)}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", healthHandler(catalogConn, ordersConn))
	mux.HandleFunc("POST /auth/register", auth.register)
	mux.HandleFunc("POST /auth/login", auth.login)
	mux.HandleFunc("POST /auth/refresh", auth.refresh)

	mux.Handle("POST /listings", auth.requireAuth(requireRole("seller", http.HandlerFunc(listings.create))))
	mux.HandleFunc("GET /listings", listings.list)
	mux.HandleFunc("GET /listings/{id}", listings.get)
	mux.Handle("PATCH /listings/{id}", auth.requireAuth(requireRole("seller", http.HandlerFunc(listings.update))))
	mux.Handle("DELETE /listings/{id}", auth.requireAuth(requireRole("seller", http.HandlerFunc(listings.delete))))

	var handler http.Handler = mux
	handler = cors(handler)
	handler = recoverMiddleware(handler)
	handler = logger(handler)
	handler = requestID(handler)

	fmt.Printf("gateway listening on :%s\n", port)
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		fmt.Fprintf(os.Stderr, "serve: %v\n", err)
		os.Exit(1)
	}
}
