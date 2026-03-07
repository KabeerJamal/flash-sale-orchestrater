package main

import (
	"context"
	"log"
	"myproject/Consumers/rollbackWorker/worker"

	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load("../.env")

	if err != nil {
		log.Println("No .env file found, relying on environment variables")
	}

	if err := worker.RollbackWorker(context.Background()); err != nil {
		log.Fatal(err)
	}

}
