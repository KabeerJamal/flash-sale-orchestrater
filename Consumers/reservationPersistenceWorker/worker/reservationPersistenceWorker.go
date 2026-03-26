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
		Brokers: []string{kafkaBrokerAddress},
		Topic:   shared.TopicReservationSuccessful,
		GroupID: shared.TopicReservationSuccessfulGroup,
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

	fmt.Println("Consumer B started")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	//consume forever
	go func() {
		for {
			msg, err := r.ReadMessage(ctx)

			if err != nil {
				slog.Error("Error while reading message", "error", err)
				return
			}

			var data shared.ReservationEvent

			err = json.Unmarshal(msg.Value, &data)
			if err != nil {
				slog.Error("Error unmarshaling JSON", "error", err)
				continue
			}

			// 3. Print message
			fmt.Printf("Received message in insertion Worker: key=%s value=%s\n", string(msg.Key), string(msg.Value))

			//data["phoneUUID"] and data["userUUID"] insertion into db first(ONLY FOR DEMO MODE)
			//----TEMP CODE---
			_, err = db.Exec("INSERT INTO USERS (userUUID, userName) VALUES ($1, $2) ON CONFLICT (userUUID) DO NOTHING", data.UserUUID, "Messi")
			if err != nil {
				continue
			}
			_, err = db.Exec("INSERT INTO PHONES (phoneUUID, phoneName) VALUES ($1, $2) ON CONFLICT (phoneUUID) DO NOTHING", data.PhoneUUID, "Iphone")
			if err != nil {
				continue
			}
			//---TEMP CODE END---

			//on conflict do nothing is basiaclly idempotency check
			_, err = db.Exec("INSERT INTO RESERVATIONS (ticketID,phoneUUID, userUUID, status) VALUES ($1, $2, $3, $4) ON CONFLICT (ticketID) DO NOTHING", data.TicketUUID, data.PhoneUUID, data.UserUUID, "RESERVED")
			if err != nil {
				continue
			}

			err = shared.RetryExternal(5, func() error {
				return rdb.Set(ctx, data.TicketUUID, shared.SuccessfulReservation, 0).Err()
			})
			if err != nil {
				slog.Error("failed to set redis key", "ticketID", data.TicketUUID, "error", err)
				continue
			}
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
							// Pass a fresh context so the background task doesn't die if the main worker shuts down.
							go soldOut(context.Background(), rdb)
						}

						break //it worked, Break out of the retry loop.
					}

					slog.Warn("Ticket not found yet. Waiting for insertion...", "ticketID", event.TicketUUID, "attempt", i)
				}

				// 3. If we haven't reached max retries yet, sleep before trying again
				if i < maxRetries {
					time.Sleep(500 * time.Millisecond)
				}
			}

			// 4. If we exhausted all 5 attempts and it still failed
			if !success {
				slog.Error("CRITICAL ERROR: Could not update payment for ticket. Dropping message.", "ticketID", event.TicketUUID, "maxRetries", maxRetries)
				continue // Skip to the next Kafka message so the worker doesn't die
			}

			// If success == true, publish the "reservation_confirmed" event for the emailer

		}
	}()

	<-ctx.Done() //blocked here
	return nil
}

func soldOut(ctx context.Context, rdb *redis.Client) {

	for {
		customer, err := rdb.LPop(ctx, shared.WaitListQueue).Result()

		if err == redis.Nil {
			break // The list is completely empty, exit the loop
		}

		if err != nil {
			slog.Error("Redis error during LPop", "error", err)
			continue // Skip if there's a weird network error
		}

		parts := strings.Split(customer, "|")
		if len(parts) != 3 {
			slog.Error("malformed waitlist entry", "entry", customer)
		}

		ticketUUID := parts[0]
		//nextUserUUID := parts[1]
		//nextPhoneUUID := parts[2]

		err = rdb.Set(ctx, ticketUUID, shared.SoldOut, 0).Err()
		if err != nil {
			slog.Error("Failed to set ticket as SOLD_OUT", "ticketID", ticketUUID, "error", err)
		}

	}
}
