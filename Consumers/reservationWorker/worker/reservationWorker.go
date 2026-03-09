package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func ReservationWorker(ctx context.Context) error {

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")
	redisAddress := os.Getenv("REDIS_docker")

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer rdb.Close()

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBrokerAddress},
		Topic:   "Reservation-successful",
		//Balancer: &kafka.LeastBytes{},
		Balancer: &kafka.Hash{},
	})

	// 1. Create reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokerAddress}, //TODO: this is hardcoded, need to fix that
		Topic:   "Reservations",
		GroupID: "Reservation-group",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// consume forever
	for {
		msg, err := r.ReadMessage(ctx) //this is blocking

		if err != nil {
			slog.Error("Error while reading message", "error", err)

			return err
		}

		// print message..
		slog.Info("Received message", "key", string(msg.Key), "value", string(msg.Value))

		var data map[string]string

		err = json.Unmarshal(msg.Value, &data)
		if err != nil {
			slog.Error("Error unmarshaling JSON", "error", err)
			return err
		}
		ticketUUID := data["ticketUUID"]

		// Idempotency Check
		status, err := rdb.Get(ctx, ticketUUID).Result()
		if err == nil && (status == "SUCCESSFUL_RESERVATION" || status == "WAITING_LIST") {
			slog.Info("Duplicate message for ticket, skipping", "ticketID", ticketUUID, "status", status)
			continue // Skip processing!
		}

		//You send message to two places, frontend(via redis) and backend(using redpanda)
		// if reservation in redis is less than 10, do reservation in redis 10 - 1. Send message to kafka for new topic
		script := redis.NewScript(`
			local stock = tonumber(redis.call("GET", KEYS[1]))
			if stock == nil then
				return -2
			end

			if stock <= 0 then
				return -1
			end

			return redis.call("DECR", KEYS[1])
		`) // script is basically -2,-1 or remaining stock after decrement

		res, err := script.Run(ctx, rdb, []string{"reservation"}).Result()
		if err != nil {
			slog.Error("Some problem with redis database", "error", err)
			return err
		}

		// Redis returns numbers as int64 through go-redis
		//TODO: I think this code will break if we have bad input data
		n := res.(int64)

		if n == -2 {
			//error handling
			slog.Error("Some problem with redis database")
			return err
		} else if n == -1 {
			//TODO:
			/*
				Because Set and RPush are two separate network calls, if your worker crashes exactly after the Set but before the RPush:
				The frontend will see "WAITING_LIST" and keep polling forever.
				The user will never actually be in the waitlist_queue, so they will never get a ticket if one opens up.
				For a practice project: It is fine to leave it as is.
				For enterprise: You would wrap both commands inside a Redis Pipeline or a Lua script to make them execute atomically (all-or-nothing).
			*/
			rdb.Set(ctx, ticketUUID, "WAITING_LIST", 0).Err()
			rdb.RPush(ctx, "waitlist_queue", ticketUUID)
		} else {

			err = w.WriteMessages(
				ctx,

				kafka.Message{
					Key:   msg.Key,
					Value: msg.Value,
				},
			)
			if err != nil {
				slog.Error("failed to write message", "error", err)
			}

			//update redis too
			err = rdb.Set(ctx, ticketUUID, "SUCCESSFUL_RESERVATION", 0).Err()
			if err != nil {
				slog.Error("Failed to update reservation status in redis",
					"ticketUUID", ticketUUID,
					"error", err,
				)
			}
		}

	}
	return nil
}
