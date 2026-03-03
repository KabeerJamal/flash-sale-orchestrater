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
			Key:   []byte(key), //TODO: hardcoded, need to fix this.
			Value: message,
		},
	)
	defer w.Close()
	if err != nil {
		panic(err)
	} else {
		println("Message Sent")
	}

}
