package shared

import (
	"context"
	"log/slog"

	"github.com/segmentio/kafka-go"
)

func SendToDLQ(ctx context.Context, dlqWriter *kafka.Writer, r *kafka.Reader, msg kafka.Message) {
	if err := dlqWriter.WriteMessages(ctx, kafka.Message{
		Key:   msg.Key,
		Value: msg.Value,
	}); err != nil {
		slog.Error("DLQ write failed", "error", err)
	}
	if err := r.CommitMessages(ctx, msg); err != nil {
		slog.Error("Commit failed after DLQ write, message will be redelivered", "error", err)
	}
}
