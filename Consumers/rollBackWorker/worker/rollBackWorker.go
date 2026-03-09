package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
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
		Brokers: []string{kafkaBrokerAddress}, //TODO: this is hardcoded, need to fix that
		Topic:   "Payment-Failed",
		GroupID: "Rollback-group",
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
		var pm PaymentEvent
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
					err = rdb.Incr(ctx, "reservation").Err()
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
		err = rdb.Set(ctx, pm.TicketUUID, "FAILED", 0).Err()
		if err != nil {
			slog.Error("Error updating Redis status",
				"ticketUUID", pm.TicketUUID,
				"error", err,
			)
		}
	}
	return nil
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
