package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"myproject/Consumers/reservationPersistenceWorker/worker"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load("../.env")
	if err != nil {
		log.Println("No .env file found, relying on environment variables")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	if err := worker.ReservationPersistenceWorker(ctx); err != nil {
		log.Fatal(err)
	}

}
