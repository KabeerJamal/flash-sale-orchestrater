package main

import (
	"context"
	"log/slog"
	"myproject/Consumers/reservationWorker/worker"
	"os"

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
	if err := worker.ReservationWorker(context.Background()); err != nil {
		// Replace log.Fatal with slog.Error + os.Exit
		slog.Error("Worker stopped with error", "error", err)
		os.Exit(1)
	}
}
