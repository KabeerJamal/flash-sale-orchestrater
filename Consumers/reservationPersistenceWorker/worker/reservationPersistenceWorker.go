package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"myproject/shared"
	"os"
	"strconv"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func ReservationPersistenceWorker(ctx context.Context) error {

	dbUser := os.Getenv("POSTGRES_USER")
	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	dbName := os.Getenv("POSTGRES_DB")
	dbAddress := os.Getenv("DB_HOST_docker")

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable",
		dbUser, dbPassword, dbAddress, dbName,
	)

	db, err := sql.Open("pgx", connStr)

	if err != nil {
		slog.Error("DB connection not established", "error", err)
		return err
	}

	err = db.Ping()
	if err != nil {
		slog.Error("DB Ping failed", "error", err)
		return err
	}

	defer db.Close()

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")

	// 1. Create reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBrokerAddress},
		Topic:          shared.TopicReservationSuccessful,
		GroupID:        shared.TopicReservationSuccessfulGroup,
		CommitInterval: 0, // disables auto-commit
	})
	defer r.Close()

	updateReservationReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokerAddress},
		Topic:   shared.TopicPaymentSuccessful,
		GroupID: shared.TopicPaymentSuccessfulGroup,
	})
	defer updateReservationReader.Close()

	redisAddress := os.Getenv("REDIS_docker")
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "",
		DB:       0,
	})
	defer rdb.Close()

	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokerAddress},
		Balancer: &kafka.Hash{},
		Topic:    shared.TopicDeadLetterQueue,
	})
	defer dlqWriter.Close()

	flashSaleEndedWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokerAddress},
		Balancer: &kafka.Hash{},
		Topic:    shared.TopicFlashSaleEnded,
	})
	defer flashSaleEndedWriter.Close()

	fmt.Println("Consumer B started")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	//consume forever
	go func() {
		for {
			msg, err := r.FetchMessage(ctx)
			if err != nil {
				slog.Error("Error while reading message", "error", err)
				time.Sleep(3 * time.Second)
				continue
			}

			var data shared.ReservationEvent

			err = json.Unmarshal(msg.Value, &data)
			if err != nil {
				slog.Error("Error unmarshaling JSON", "error", err)
				shared.SendToDLQ(ctx, dlqWriter, r, msg)
				//Dont put return cause then one bad message takes down the whole system
				continue //Send to DLQ + commit. What would retrying a malformed JSON message accomplish?
			}

			// 3. Print message
			//fmt.Printf("Received message in insertion Worker: key=%s value=%s\n", string(msg.Key), string(msg.Value))

			//on conflict do nothing is basiaclly idempotency check
			for {
				err = shared.RetryExternal(5, func() error {
					_, err = db.Exec("INSERT INTO RESERVATIONS (ticketID,phoneUUID, userUUID, status) VALUES ($1, $2, $3, $4) ON CONFLICT (ticketID) DO NOTHING", data.TicketUUID, data.PhoneUUID, data.UserUUID, "RESERVED")
					return err
				})

				if err == nil {
					break // Success! Exit the infinite database block.
				}

				// DB is down. Sleep, DONT continue, just loop and try this exact message again!
				slog.Error("failed to insert reservation, retrying", "ticketID", data.TicketUUID, "error", err)
				time.Sleep(2 * time.Second)
			}

			// for {
			// 	err = shared.RetryExternal(5, func() error {
			// 		return rdb.Set(ctx, data.TicketUUID, shared.SuccessfulReservation, 0).Err()
			// 	})

			// 	if err == nil {
			// 		break // Success! Exit the infinite redis block.
			// 	}

			// 	// Redis is down. Sleep, DONT continue, just loop and try this exact message again!
			// 	slog.Error("failed to set redis key, retrying", "ticketID", data.TicketUUID, "error", err)
			// 	time.Sleep(2 * time.Second)
			// }
			// Because the loops above block forever until they succeed, by the time the
			// code reaches here, YOU ARE GUARANTEED that both DB and Redis succeeded.
			// You can now safely run r.CommitMessages(ctx, msg) at the end of your outer loop.
			//If a crash happens mid-way (for example, the database insert worked, but your server caught on fire before the Redis set finished),
			//  your Go worker completely shuts down without ever reaching the r.CommitMessages() line.
			//When the container restarts, it connects to Kafka. Kafka says, "You never finished reading your last bookmark,"
			// and it gives you the exact same message again safely.
			r.CommitMessages(ctx, msg)

		}
	}()

	go func() {
		for {
			msg, err := updateReservationReader.FetchMessage(ctx)

			if err != nil {
				slog.Error("Error while reading message", "error", err)
				time.Sleep(3 * time.Second)
				continue
			}
			//slog.Info("Received message in reservation persistence Worker", "key", string(msg.Key), "value", string(msg.Value))

			var event shared.PaymentEvent
			err = json.Unmarshal(msg.Value, &event)
			if err != nil {
				slog.Error("Error unmarshaling JSON", "error", err)
				shared.SendToDLQ(ctx, dlqWriter, r, msg)
				//Dont put return cause then one bad message takes down the whole system
				continue //Send to DLQ + commit. What would retrying a malformed JSON message accomplish?
			}

			for {
				tx, err := db.BeginTx(ctx, nil)
				if err != nil {
					// handle
					time.Sleep(2 * time.Second)
					continue
				}

				res, err := tx.Exec("UPDATE RESERVATIONS SET status = $1 WHERE ticketID = $2 AND status != 'PAID'", "PAID", event.TicketUUID)
				if err != nil {
					tx.Rollback()
					time.Sleep(2 * time.Second)
					continue
				}

				rowsAffected, err := res.RowsAffected()
				if err != nil {
					tx.Rollback()
					time.Sleep(2 * time.Second)
					continue
				}

				var totalPaid int64
				if rowsAffected > 0 {

					err = tx.QueryRow("UPDATE CONFIG SET value = value + 1 WHERE key = 'total_paid' RETURNING value").Scan(&totalPaid)
					if err != nil {
						tx.Rollback()
						time.Sleep(2 * time.Second)
						continue
					}
					if err = tx.Commit(); err != nil {
						tx.Rollback()
						time.Sleep(2 * time.Second)
						continue
					}
					slog.Info("Success! Ticket updated to PAID", "ticketID", event.TicketUUID)

					for {
						err = rdb.Set(ctx, event.TicketUUID, shared.Paid, 0).Err()
						if err == nil {
							break
						}
						// DB is down, just sleep and loop back instantly!
						slog.Error("Redis down, retrying...", "ticketID", event.TicketUUID, "error", err)
						time.Sleep(2 * time.Second)
					}

					for {
						_, err = rdb.Set(ctx, shared.TotalPaid, totalPaid, 0).Result()

						if err == nil {
							break
						}
						// DB is down, just sleep and loop back instantly!
						slog.Error("Redis down, retrying...", "error", err)
						time.Sleep(2 * time.Second)
					}

					if strconv.FormatInt(totalPaid, 10) == os.Getenv("TOTAL_PRODUCTS") {

						eventBytes := []byte(`{"event": "flash_sale_ended"}`)

						for {
							err := flashSaleEndedWriter.WriteMessages(ctx, kafka.Message{
								Key:   []byte("sold-out-trigger"),
								Value: eventBytes,
							})

							if err == nil {
								break // Success! The trigger was safely sent to Kafka.
							}

							// Kafka is currently unreachable. Don't move on!
							slog.Error("Failed to publish Flash Sale Ended trigger, retrying...", "error", err)
							time.Sleep(2 * time.Second)
						}
					}
					updateReservationReader.CommitMessages(ctx, msg)

				} else {
					if err = tx.Commit(); err != nil {
						tx.Rollback()
						time.Sleep(2 * time.Second) // Add missing sleep
						continue

					}
					// We MUST NOT increment total_paid again. We just read its current value.
					var totalPaid int64
					for {
						err = db.QueryRow("SELECT value FROM CONFIG WHERE key = 'total_paid'").Scan(&totalPaid)

						if err == nil {
							break // Success!
						}

						// DB is down, block infinitely right here without restarting the outer transaction!
						slog.Error("Failed to read total_paid during crash recovery, retrying THIS EXACT query...", "error", err)
						time.Sleep(2 * time.Second)
					}

					//  Force-sync Redis status to PAID (Idempotent, safe to repeat)
					for {
						err = rdb.Set(ctx, event.TicketUUID, shared.Paid, 0).Err()
						if err == nil {
							break
						}
						time.Sleep(2 * time.Second)
					}

					//  Force-sync Redis total_paid counter (Idempotent, because Set overwrites!)
					for {
						_, err = rdb.Set(ctx, shared.TotalPaid, totalPaid, 0).Result()
						if err == nil {
							break
						}
						time.Sleep(2 * time.Second)
					}
					//  Run the Sold-Out trigger check again just in case it crashed before sending!
					if strconv.FormatInt(totalPaid, 10) == os.Getenv("TOTAL_PRODUCTS") {
						eventBytes := []byte(`{"event": "flash_sale_ended"}`)
						for {
							err := flashSaleEndedWriter.WriteMessages(ctx, kafka.Message{
								Key:   []byte("sold-out-trigger"),
								Value: eventBytes,
							})
							if err == nil {
								break
							}
							time.Sleep(2 * time.Second)
						}
					}
					updateReservationReader.CommitMessages(ctx, msg)

					slog.Info("Successfully recovered from previous crash!", "ticketID", event.TicketUUID)

				}
				break
			}

		}
	}()

	<-ctx.Done() //blocked here
	return nil
}
