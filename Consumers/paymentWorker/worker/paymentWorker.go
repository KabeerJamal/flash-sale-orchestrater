package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/segmentio/kafka-go"
)

// No need for idempotency here. Do it in the workers that perform the side-effect (DB update / rollback).
func PaymentWorker(ctx context.Context) error {

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokerAddress},
		Topic:    "Payment-Successful",
		Balancer: &kafka.Hash{},
	})
	defer w.Close()

	rollBackWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokerAddress},
		Topic:    "Payment-Failed",
		Balancer: &kafka.Hash{},
	})
	defer rollBackWriter.Close()

	// 1. Create reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokerAddress}, //TODO: this is hardcoded, need to fix that
		Topic:   "Payment",
		GroupID: "Payment-group",
	})
	defer r.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		msg, err := r.ReadMessage(ctx)

		if err != nil {
			slog.Error("Error while reading Kafka message", "error", err)
			return err
		}

		// 3. Print message
		slog.Info("Received message",
			"key", string(msg.Key),
			"value", string(msg.Value),
		)
		//if success, send topic to insertion worker to update database
		var paymentMessage PaymentEvent

		//call a function which takes in msg.Value and returns paymentMessage
		paymentMessage, err = convertToPaymentMessage(msg.Value)
		if err != nil {
			slog.Error("Failed to convert message to PaymentEvent",
				"error", err,
				"raw_message", string(msg.Value),
			)
			continue
		}

		valueBytes, err := json.Marshal(paymentMessage)
		if err != nil {
			slog.Error("Failed to marshal PaymentEvent",
				"error", err,
				"ticketUUID", paymentMessage.TicketUUID,
			)
			continue
		}

		if paymentMessage.Status == "paid" {

			w.WriteMessages(
				ctx,
				kafka.Message{
					Key:   msg.Key,
					Value: valueBytes,
				},
			)
		} else {

			rollBackWriter.WriteMessages(
				ctx,
				kafka.Message{
					Key:   msg.Key,
					Value: valueBytes,
				},
			)
		}
	}

}

type PaymentEvent struct {
	TicketUUID      string `json:"ticketUUID"`
	PhoneUUID       string `json:"phoneUUID"`
	UserUUID        string `json:"userUUID"`
	PaymentIntentID string `json:"paymentIntentID"`
	Amount          int64  `json:"amount"`
	Currency        string `json:"currency"`
	Status          string `json:"status"`
}

// When you use json.Unmarshal into a struct, extra fields in the JSON are ignored automatically.
// So if msg.Value contains many more fields, Go will just fill the ones that match your struct and skip the rest
func convertToPaymentMessage(data []byte) (PaymentEvent, error) {
	var pm PaymentEvent

	err := json.Unmarshal(data, &pm)
	if err != nil {
		return PaymentEvent{}, err
	}

	return pm, nil
}
