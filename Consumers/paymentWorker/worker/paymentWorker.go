package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"myproject/shared"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

// No need for idempotency here. Do it in the workers that perform the side-effect (DB update / rollback).
func PaymentWorker(ctx context.Context) error {

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokerAddress},
		Topic:    shared.TopicPaymentSuccessful,
		Balancer: &kafka.Hash{},
	})
	defer w.Close()

	rollBackWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokerAddress},
		Topic:    shared.TopicPaymentFailure,
		Balancer: &kafka.Hash{},
	})
	defer rollBackWriter.Close()

	// 1. Create reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokerAddress},
		Topic:   shared.TopicPayment,
		GroupID: shared.TopicPaymentGroup,
	})
	defer r.Close()
	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokerAddress},
		Balancer: &kafka.Hash{},
		Topic:    shared.TopicDeadLetterQueue,
	})
	defer dlqWriter.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	for {
		msg, err := r.FetchMessage(ctx)

		if err != nil {
			slog.Error("Error while reading message", "error", err)
			time.Sleep(3 * time.Second)
			continue
		}

		// 3. Print message
		slog.Info("Received message in Payment Worker",
			"key", string(msg.Key),
			"value", string(msg.Value),
		)
		//if success, send topic to insertion worker to update database
		var paymentMessage shared.PaymentEvent

		//call a function which takes in msg.Value and returns paymentMessage
		paymentMessage, err = convertToPaymentMessage(msg.Value)
		if err != nil {
			slog.Error("Failed to convert message to PaymentEvent",
				"error", err,
				"raw_message", string(msg.Value),
			)
			shared.SendToDLQ(ctx, dlqWriter, r, msg) // ← DLQ + commit
			continue
		}

		valueBytes, err := json.Marshal(paymentMessage)
		if err != nil {
			slog.Error("Failed to marshal PaymentEvent",
				"error", err,
				"ticketUUID", paymentMessage.TicketUUID,
			)
			shared.SendToDLQ(ctx, dlqWriter, r, msg) // ← DLQ + commit
			continue
		}

		if paymentMessage.Status == shared.StripePaid {

			for {
				err := w.WriteMessages(
					ctx,
					kafka.Message{
						Key:   msg.Key,
						Value: valueBytes,
					},
				)
				if err == nil {
					break // Success! The trigger was safely sent to Kafka.
				}

				// Kafka is currently unreachable. Don't move on!
				slog.Error("Failed to publish Sucessful payment message trigger, retrying...", "error", err)
				time.Sleep(2 * time.Second)
			}
			r.CommitMessages(ctx, msg)
		} else {

			for {
				err := rollBackWriter.WriteMessages(
					ctx,
					kafka.Message{
						Key:   msg.Key,
						Value: valueBytes,
					},
				)
				if err == nil {
					break // Success! The trigger was safely sent to Kafka.
				}

				// Kafka is currently unreachable. Don't move on!
				slog.Error("Failed to publish Rollback message trigger, retrying...", "error", err)
				time.Sleep(2 * time.Second)
			}
			r.CommitMessages(ctx, msg)
		}
	}

}

// When you use json.Unmarshal into a struct, extra fields in the JSON are ignored automatically.
// So if msg.Value contains many more fields, Go will just fill the ones that match your struct and skip the rest
func convertToPaymentMessage(data []byte) (shared.PaymentEvent, error) {
	var pm shared.PaymentEvent

	err := json.Unmarshal(data, &pm)
	if err != nil {
		return shared.PaymentEvent{}, err
	}

	return pm, nil
}
