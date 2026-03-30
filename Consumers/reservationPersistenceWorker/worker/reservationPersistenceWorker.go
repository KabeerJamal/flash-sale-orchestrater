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
	"strings"
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
			err = shared.RetryExternal(5, func() error {
			_, err = db.Exec("INSERT INTO RESERVATIONS (ticketID,phoneUUID, userUUID, status) VALUES ($1, $2, $3, $4) ON CONFLICT (ticketID) DO NOTHING", data.TicketUUID, data.PhoneUUID, data.UserUUID, "RESERVED")
				return err
			})
			if err != nil {
				slog.Error("failed to insert reservation", "ticketID", data.TicketUUID, "error", err)
				shared.SendToDLQ(ctx, dlqWriter, r, msg)
				continue
			}

			err = shared.RetryExternal(5, func() error {
				return rdb.Set(ctx, data.TicketUUID, shared.SuccessfulReservation, 0).Err()
			})
			if err != nil {
				slog.Error("failed to set redis key", "ticketID", data.TicketUUID, "error", err)
				shared.SendToDLQ(ctx, dlqWriter, r, msg)
				continue
			}
			r.CommitMessages(ctx, msg)

		}
	}()

	go func() {
		for {
			msg, err := updateReservationReader.ReadMessage(ctx)

			if err != nil {
				slog.Error("Error while reading message", "error", err)
				return
			}
			slog.Info("Received message in reservation persistence Worker", "key", string(msg.Key), "value", string(msg.Value))

			var event shared.PaymentEvent
			json.Unmarshal(msg.Value, &event)

			// --- Retry Logic for Race Condition ---
			maxRetries := 5
			success := false

			for i := 1; i <= maxRetries; i++ {
				// 1. Attempt the update
				res, err := db.Exec("UPDATE RESERVATIONS SET status = $1 WHERE ticketID = $2 AND status != 'PAID'", "PAID", event.TicketUUID)

				if err != nil {
					slog.Error("DB query error", "attempt", i, "error", err)
				} else {
					// 2. Check if the row actually existed
					rowsAffected, err := res.RowsAffected()
					if err == nil && rowsAffected > 0 {
						slog.Info("Success! Ticket updated to PAID", "ticketID", event.TicketUUID, "attempt", i)
						success = true
						err = rdb.Set(ctx, event.TicketUUID, shared.Paid, 0).Err()
						if err != nil {
							slog.Error("failed to set redis key", "ticketID", event.TicketUUID, "error", err)
							return
						}

						totalPaid, err := rdb.Incr(ctx, shared.TotalPaid).Result()

						if err != nil {
							slog.Error("failed to increment total_paid", "error", err)
							return
						}

						if strconv.FormatInt(totalPaid, 10) == os.Getenv("TOTAL_PRODUCTS") {
					eventBytes := []byte(`{"event": "flash_sale_ended"}`)

					flashSaleEndedWriter.WriteMessages(ctx, kafka.Message{
						Key:   []byte("sold-out-trigger"),
						Value: eventBytes,
					})

				}
		}
	}()

	<-ctx.Done() //blocked here
	return nil
}
