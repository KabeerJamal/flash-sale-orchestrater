package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"myproject/shared"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func RollbackWorker(ctx context.Context) error {
	dbUser := os.Getenv("POSTGRES_USER")
	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	dbName := os.Getenv("POSTGRES_DB")
	dbAddress := os.Getenv("DB_HOST_docker")
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", dbUser, dbPassword, dbAddress, dbName)

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	redisAddress := os.Getenv("REDIS_docker")
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "",
		DB:       0,
	})
	defer rdb.Close()

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")

	// 1. Create reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokerAddress},
		Topic:   shared.TopicPaymentFailure,
		GroupID: shared.TopicPaymentFailureGroup,
	})

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBrokerAddress},
		Topic:   shared.TopicReservation,
		//Balancer: &kafka.LeastBytes{},
		Balancer: &kafka.Hash{},
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Consume forever
	for {
		msg, err := r.ReadMessage(ctx) //this is blocking
		if err != nil {
			slog.Error("Error while reading message", "error", err)
			return err
		}

		// Print message
		slog.Info("Received message", "key", string(msg.Key), "value", string(msg.Value))

		// Unmarshal the message
		var pm shared.PaymentEvent
		err = json.Unmarshal(msg.Value, &pm)
		if err != nil {
			slog.Error("Error unmarshaling message", "error", err)
			continue
		}

		// Retry Logic for Race Condition
		maxRetries := 5
		success := false

		for i := 1; i <= maxRetries; i++ {
			// Attempt to delete the reservation
			res, err := db.Exec("DELETE FROM RESERVATIONS WHERE ticketID = $1", pm.TicketUUID)

			if err != nil {
				slog.Error("DB query error",
					"attempt", i,
					"ticketUUID", pm.TicketUUID,
					"error", err,
				)
			} else {
				// Check if a row was actually deleted
				rowsAffected, _ := res.RowsAffected()

				if rowsAffected > 0 {
					// 3. We successfully deleted the row! Increment the stock back safely.
					err = rdb.Incr(ctx, shared.Reservations).Err()
					if err != nil {
						slog.Error("Error incrementing stock",
							"ticketUUID", pm.TicketUUID,
							"error", err,
						)
					} else {
						slog.Info("Successfully deleted reservation and restored stock",
							"ticketUUID", pm.TicketUUID,
							"attempt", i,
						)
					}

					/*
						Wrong snippet:
						nextTicket := rdb.LPop(ctx, "waitlist_queue").Result() if nextTicket != nil { rdb.Set(ctx, nextTicket, "SUCCESSFUL_RESERVATION", 0) }

						The first snippet is **wrong** and won't even compile. Here is why:
						1. `Result()` returns two values: the string (ticket UUID) and the error. Assigning it to just `nextTicket` causes a Go compiler error (`assignment mismatch: 1 variable but 2 values`).
						2. In Go, a `string` can never be `nil`. So checking `if nextTicket != nil` is invalid.

						**Your second snippet is 100% correct and production-ready.**

						It perfectly handles:
						1. Both return values `(nextTicket, err)`.
						2. The specific `redis.Nil` case (which happens when `LPop` tries to pop from an empty list).
						3. Any actual connection errors.
						4. Catching the error on the subsequent `rdb.Set` command and logging it nicely with `slog`.

						Use your second snippet exactly as you wrote it!
					*/
					nextCustomer, err := rdb.LPop(ctx, shared.WaitListQueue).Result()

					if err == redis.Nil {
						// waitlist empty
					} else if err != nil {
						slog.Error("Error popping from waitlist queue", "error", err)
					} else {
						parts := strings.Split(nextCustomer, "|")
						if len(parts) != 3 {
							slog.Error("malformed waitlist entry", "entry", nextCustomer)
						}

						nextTicketUUID := parts[0]
						nextUserUUID := parts[1]
						nextPhoneUUID := parts[2]

						var payload shared.ReservationEvent
						payload.TicketUUID = nextTicketUUID
						payload.UserUUID = nextUserUUID
						payload.PhoneUUID = nextPhoneUUID
						payload.Promoted = true
						b, _ := json.Marshal(payload)
						err = w.WriteMessages(
							ctx,

							kafka.Message{
								Key:   []byte(nextTicketUUID), //convert in bytes
								Value: b,                      //convert in bytes
							},
						)
						if err != nil {
							slog.Error("failed to write message", "error", err)
						}
					}

					success = true
					break // It worked, break out of the retry loop
				}

				slog.Warn("Ticket not found, waiting for insertion or already deleted",
					"attempt", i,
					"ticketUUID", pm.TicketUUID,
				)
			}

			// Sleep before trying again
			if i < maxRetries {
				time.Sleep(500 * time.Millisecond)
			}
		}

		if !success {
			slog.Warn("Finished retries; row was likely already deleted or never inserted",
				"ticketUUID", pm.TicketUUID,
			)
		}

		// Update the final status in Redis so the frontend knows it failed
		err = rdb.Set(ctx, pm.TicketUUID, shared.Failed, 0).Err()
		if err != nil {
			slog.Error("Error updating Redis status",
				"ticketUUID", pm.TicketUUID,
				"error", err,
			)
		}
	}
	return nil
}
