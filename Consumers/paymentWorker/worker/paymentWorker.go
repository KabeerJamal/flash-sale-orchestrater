package worker

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

func PaymentWorker(ctx context.Context) error {

	//read from kafka

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")

	// 1. Create reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokerAddress}, //TODO: this is hardcoded, need to fix that
		Topic:   "Payment",
		GroupID: "Payment-group",
	})

	fmt.Println("Payment Worker started")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		msg, err := r.ReadMessage(ctx)

		if err != nil {
			log.Println("Error while reading:", err)
			return err
		}

		// 3. Print message
		fmt.Printf("Received message: key=%s value=%s\n", string(msg.Key), string(msg.Value))
		//if success, update db status to paid
		//if failed, write to kafka, which will be read by undo worker
	}

}
