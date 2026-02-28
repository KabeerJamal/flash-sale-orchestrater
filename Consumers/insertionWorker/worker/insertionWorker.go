package worker

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

func InsertionWorker(ctx context.Context) error {

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")

	// 1. Create reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokerAddress}, //TODO: this is hardcoded, need to fix that
		Topic:   "Reservation-successful",
		GroupID: "InsertionToSQL-group",
	})

	fmt.Println("Consumer B started")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	//consume forever
	for {
		msg, err := r.ReadMessage(ctx)

		if err != nil {
			log.Println("Error while reading:", err)
			return err
		}

		// 3. Print message
		fmt.Printf("Received message: key=%s value=%s\n", string(msg.Key), string(msg.Value))

		//TOMMOROW: insert into SQL(also write code for inserting into phone and user first(only for demo mode))

	}
}
