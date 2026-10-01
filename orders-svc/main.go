package main

import (
	"context"
	"embed"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	catalogpb "marketplace/gen/catalogpb"
	orderspb "marketplace/gen/orderspb"
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
	databaseURL := requireEnv("DATABASE_URL")
	kafkaBrokers := requireEnv("KAFKA_BROKERS")
	catalogAddr := requireEnv("CATALOG_GRPC_ADDR")

	addr := os.Getenv("GRPC_ADDR")
	if addr == "" {
		addr = ":50052"
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

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(strings.Split(kafkaBrokers, ",")...),
		Balancer:               &kafka.LeastBytes{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	go runOutboxWorker(ctx, pool, writer)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "listen %s: %v\n", addr, err)
		os.Exit(1)
	}

	srv := grpc.NewServer()
	orderspb.RegisterOrdersServer(srv, &server{
		pool:    pool,
		catalog: catalogpb.NewCatalogClient(catalogConn),
	})

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(srv, healthSrv)

	fmt.Printf("orders-svc listening on %s\n", addr)
	if err := srv.Serve(lis); err != nil {
		fmt.Fprintf(os.Stderr, "serve: %v\n", err)
		os.Exit(1)
	}
}
