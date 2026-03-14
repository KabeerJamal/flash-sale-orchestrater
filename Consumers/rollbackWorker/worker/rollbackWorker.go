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
		Topic:   shared.TopicReservationSuccessful,
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
					//get user form waitlist. set ticket UUID status to RESERVATION_SUCCESSFUL
					script := redis.NewScript(`
							local customer = redis.call('LPOP', KEYS[1])

							if customer then

								local parts = {}
								for part in string.gmatch(customer, '[^|]+') do
									table.insert(parts, part)
								end
								local ticketUUID = parts[1]
								redis.call('SET', ticketUUID, KEYS[2])
								return customer
							else
								redis.call('INCR', KEYS[3])
								return nil
								end
						`)
					//dont call kafka if no one in waitlist
					//return customer so then i can split it

					result, err := script.Run(ctx, rdb, []string{shared.WaitListQueue, shared.SuccessfulReservation, shared.Reservations}).Result()
					if err != nil && err != redis.Nil {
						slog.Error("Some problem with redis database", "error", err)
						return err
					}

					if result != nil {
						parts := strings.Split(result.(string), "|")
						if len(parts) != 3 {
							slog.Error("malformed waitlist entry", "entry", result)
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

			}

			slog.Warn("Ticket not found, waiting for insertion or already deleted",
				"attempt", i,
				"ticketUUID", pm.TicketUUID,
			)

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
