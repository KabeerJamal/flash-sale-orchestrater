package main

import (
	"context"
	"log"
	"myproject/Consumers/reservationWorker/worker"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load("../.env")
	if err != nil {
		log.Println("No .env file found, relying on environment variables")
	}
	if err := worker.ReservationWorker(context.Background()); err != nil {
		log.Fatal(err)
	}
}
