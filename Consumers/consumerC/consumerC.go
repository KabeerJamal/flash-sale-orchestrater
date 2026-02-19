package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func main() {
	_ = godotenv.Load("../.env")
	// if err != nil {
	// 	panic(err)
	// }

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")

	// 1. Create reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokerAddress}, //TODO: this is hardcoded, need to fix that
		Topic:   "flashsale-events",
		GroupID: "flashsale-consumer-group-3",
	})

	fmt.Println("Consumer C started")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for {
		msg, err := r.ReadMessage(ctx)

		if err != nil {
			log.Println("Error while reading:", err)
			continue
		}

		// 3. Print message
		fmt.Printf("Received message: key=%s value=%s\n", string(msg.Key), string(msg.Value))
	}

}
