package main

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/okezieobi/go-idempotent-outbox/internal/usecase"
)

func main() {
	connStr := "host=localhost port=5432 user=postgres password=postgrespassword dbname=outbox_db sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.Ping(); err != nil {
		log.Fatalf("Database ping failed: %v", err)
	}

	log.Println("Connected to PostgreSQL successfully")

	repo := usecase.NewPostgresProcessRepo(db)
	useCase := usecase.NewProcessUseCase(repo)

	// Worker configuration: batch size 10, poll every 2s, lease duration 30s
	workerPool := usecase.NewWorkerPool(useCase, 10, 2*time.Second, 30*time.Second)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go workerPool.Start(ctx)

	<-ctx.Done()
	log.Println("Shutting down worker pool gracefully...")
	workerPool.Stop()
	log.Println("Shutdown complete.")
}
