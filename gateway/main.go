package main

import (
	"context"
	"embed"
	"fmt"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"

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
	_ = requireEnv("CATALOG_GRPC_ADDR")
	_ = requireEnv("ORDERS_GRPC_ADDR")

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

	auth := &authServer{pool: pool, jwtSecret: []byte(jwtSecret)}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /auth/register", auth.register)
	mux.HandleFunc("POST /auth/login", auth.login)
	mux.HandleFunc("POST /auth/refresh", auth.refresh)

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
