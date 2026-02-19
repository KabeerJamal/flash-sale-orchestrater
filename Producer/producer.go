package main

import (
	"context"

	"github.com/segmentio/kafka-go"
	// other imports
)

// Example function you want to call from main.go
func StartProducer(kafkaBrokerAddress string, topic string, key string, value string) {
	//create a write pointing to RedPanda
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokerAddress},
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	})
	//write one message
	println("Writer ready:", w != nil)

	err := w.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: []byte(value),
		},
	)
	if err != nil {
		panic(err)
	} else {
		println("Message Sent")
	}

}
