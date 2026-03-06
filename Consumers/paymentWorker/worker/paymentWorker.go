package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

func PaymentWorker(ctx context.Context) error {

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")

	// w := kafka.NewWriter(kafka.WriterConfig{
	// 	Brokers:  []string{kafkaBrokerAddress},
	// 	Topic:    "Payment-Successful",
	// 	Balancer: &kafka.Hash{},
	// })

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
		//if success, send topic to insertion worker to update database
		var paymentSuccessMessage PaymentSuccessMessage

		//call a function which takes in msg.Value and returns paymentMessage
		paymentSuccessMessage, err = convertToPaymentMessage(msg.Value)
		if err != nil {
			log.Fatal(err)
			return err
		}
		fmt.Print(paymentSuccessMessage.Status)
		return nil
		// if paymentMessage.Status == "completed" {
		// 	valueBytes, err := json.Marshal(paymentMessage)
		// 	if err != nil {
		// 		log.Fatal(err)
		// 		return err
		// 	}

		// 	w.WriteMessages(
		// 		ctx,
		// 		kafka.Message{
		// 			Key:   msg.Key,
		// 			Value: valueBytes,
		// 		},
		// 	)
		// }

		//do idempotency check
		//update redis too
		//if failed, write to kafka, which will be read by undo worker
		//upadte redis too
	}

}

type PaymentSuccessMessage struct {
	TicketUUID string `json:"TicketUUID"`
	PhoneUUID  string `json:"PhoneUUID"`
	UserUUID   string `json:"UserUUID"`
	Status     string `json:"Status"`
}

// When you use json.Unmarshal into a struct, extra fields in the JSON are ignored automatically.
// So if msg.Value contains many more fields, Go will just fill the ones that match your struct and skip the rest
func convertToPaymentMessage(data []byte) (PaymentSuccessMessage, error) {
	var pm PaymentSuccessMessage

	err := json.Unmarshal(data, &pm)
	if err != nil {
		return PaymentSuccessMessage{}, err
	}

	return pm, nil
}
