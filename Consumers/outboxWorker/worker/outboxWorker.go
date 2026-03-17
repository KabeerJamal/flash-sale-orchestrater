package worker

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

// No need for idempotency here. Do it in the workers that perform the side-effect (DB update / rollback).
func OutboxWorker(ctx context.Context) error {

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")
	redisAddress := os.Getenv("REDIS_docker")

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer rdb.Close()

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokerAddress},
		Balancer: &kafka.Hash{},
		// no Topic here
	})
	defer w.Close()

	for {
		keys, err := rdb.Keys(ctx, "outbox:*").Result()
		if err != nil {
			slog.Error("Failed to scan outbox", "error", err)
			time.Sleep(1 * time.Second)
			continue
		}

		for _, key := range keys {
			fields, err := rdb.HGetAll(ctx, key).Result()
			if err != nil {
				slog.Error("Failed to read outbox entry", "key", key, "error", err)
				continue
			}

			err = w.WriteMessages(ctx, kafka.Message{
				Topic: fields["topic"],
				Key:   []byte(fields["key"]),
				Value: []byte(fields["value"]),
			})
			if err != nil {
				slog.Error("Failed to publish outbox message, will retry", "key", key, "error", err)
				continue
			}

			rdb.Del(ctx, key)
		}

		time.Sleep(1 * time.Second)
	}

	return nil
}
