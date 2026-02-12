package main

import (
	"context"

	"github.com/segmentio/kafka-go"
	// other imports
)

// Example function you want to call from main.go
func StartProducer() {
	//create a write pointing to RedPanda
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{"localhost:9092"},
		Topic:    "flashsale-events",
		Balancer: &kafka.LeastBytes{},
	})
	//write one message
	println("Writer ready:", w != nil)

	err := w.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte("order-id-1"),
			Value: []byte("Flash Sale started"),
		},
	)
	if err != nil {
		panic(err)
	} else {
		println("Message Sent")
	}

}
