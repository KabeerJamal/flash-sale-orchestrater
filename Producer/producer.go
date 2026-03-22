package producer

import (
	"context"

	"github.com/segmentio/kafka-go"
	// other imports
)

// Example function you want to call from main.go
func StartProducer(w *kafka.Writer, key string, message []byte) {
	//write one message
	println("Writer ready:", w != nil)

	err := w.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte(key),
			Value: message,
		},
	)
	if err != nil {
		println("failed to write to kafka: %w", err)
	} else {
		println("Message Sent")
	}

}
