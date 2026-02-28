package producer

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
	// other imports
)

// Example function you want to call from main.go
func StartProducer(kafkaBrokerAddress string, topic string, message map[string]string) {
	//create a write pointing to RedPanda
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBrokerAddress},
		Topic:   topic,
		//Balancer: &kafka.LeastBytes{},
		Balancer: &kafka.Hash{},
	})
	//write one message
	println("Writer ready:", w != nil)

	//basically convert the map to bytes because KAFKA needs bytes
	valueBytes, err := json.Marshal(message)
	if err != nil {
		// handle error
	}

	err = w.WriteMessages(
		context.Background(),
		kafka.Message{
			Key:   []byte(message["userUUID"]), //TODO: hardcoded, need to fix this.
			Value: valueBytes,
		},
	)
	defer w.Close()
	if err != nil {
		panic(err)
	} else {
		println("Message Sent")
	}

}
