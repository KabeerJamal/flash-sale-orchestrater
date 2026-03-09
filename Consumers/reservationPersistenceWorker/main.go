package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"myproject/Consumers/reservationPersistenceWorker/worker"

	"github.com/joho/godotenv"
)

func main() {
	opts := &slog.HandlerOptions{
		AddSource: true, // This enables the file name and line number
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	slog.SetDefault(logger) // This makes 'logger' the global default

	err := godotenv.Load("../.env")
	if err != nil {
		slog.Warn("No .env file found, relying on environment variables", "error", err)
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := worker.ReservationPersistenceWorker(ctx); err != nil {
		// Replace log.Fatal with slog.Error + os.Exit
		slog.Error("Worker stopped with error", "error", err)
		os.Exit(1)
	}

}
