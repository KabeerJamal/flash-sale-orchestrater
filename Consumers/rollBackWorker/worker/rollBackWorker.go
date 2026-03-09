package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
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

	fmt.Println("Consumer A started")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Consume forever
	for {
		msg, err := r.ReadMessage(ctx) //this is blocking
		if err != nil {
			log.Println("Error while reading:", err)
			continue
		}

		// Print message
		fmt.Printf("Received message: key=%s value=%s\n", string(msg.Key), string(msg.Value))

		// Unmarshal the message
		var pm PaymentEvent
		err = json.Unmarshal(msg.Value, &pm)
		if err != nil {
			log.Println("Error unmarshaling message:", err)
			continue
		}

		// Retry Logic for Race Condition
		maxRetries := 5
		success := false

		for i := 1; i <= maxRetries; i++ {
			// Attempt to delete the reservation
			res, err := db.Exec("DELETE FROM RESERVATIONS WHERE ticketID = $1", pm.TicketUUID)

			if err != nil {
				log.Printf("Attempt %d: DB query error: %v\n", i, err)
			} else {
				// Check if a row was actually deleted
				rowsAffected, _ := res.RowsAffected()

				if rowsAffected > 0 {
					// 3. We successfully deleted the row! Increment the stock back safely.
					err = rdb.Incr(ctx, "reservation").Err()
					if err != nil {
						log.Printf("Error incrementing stock for %s: %v\n", pm.TicketUUID, err)
					} else {
						fmt.Printf("Success! Deleted %s and restored stock on attempt %d\n", pm.TicketUUID, i)
					}

					nextTicket := rdb.LPop(ctx, "waitlist_queue").Result()
					if nextTicket != nil {
						rdb.Set(ctx, nextTicket, "SUCCESSFUL_RESERVATION", 0)
					}
					success = true
					break // It worked, break out of the retry loop
				}

				log.Printf("Attempt %d: Ticket %s not found. Waiting for insertion (or already deleted)...\n", i, pm.TicketUUID)
			}

			// Sleep before trying again
			if i < maxRetries {
				time.Sleep(500 * time.Millisecond)
			}
		}

		if !success {
			log.Printf("Finished retries for %s. Row was likely already deleted (duplicate message) or never inserted.\n", pm.TicketUUID)
		}

		// Update the final status in Redis so the frontend knows it failed
		err = rdb.Set(ctx, pm.TicketUUID, "FAILED", 0).Err()
		if err != nil {
			log.Printf("Error updating Redis status for %s: %v\n", pm.TicketUUID, err)
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
