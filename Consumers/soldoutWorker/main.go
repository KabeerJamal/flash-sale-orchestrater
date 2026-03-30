package main

import (
	"context"
	"log/slog"
	"myproject/Consumers/soldoutWorker/worker"
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

	if err := worker.SoldOutWorker(context.Background()); err != nil {
		slog.Error("Worker stopped with error", "error", err)
		os.Exit(1)
	}
}
