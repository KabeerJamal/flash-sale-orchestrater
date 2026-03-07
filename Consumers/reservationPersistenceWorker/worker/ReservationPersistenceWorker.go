package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
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
		log.Print("DB connection not estbalished: ", err)
		return err
	}

	err = db.Ping()
	if err != nil {
		log.Println("DB Ping failed:", err)
		return err
	}

	defer db.Close()

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")

	// 1. Create reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokerAddress},
		Topic:   "Reservation-successful",
		GroupID: "InsertionToSQL-group",
	})
	defer r.Close()

	updateReservationReader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokerAddress},
		Topic:   "Payment-successful",
		GroupID: "UpdateToSQL-group",
	})
	defer updateReservationReader.Close()

	fmt.Println("Consumer B started")

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	//consume forever
	go func() {
		for {
			msg, err := r.ReadMessage(ctx)

			if err != nil {
				log.Println("Error while reading:", err)
				continue
			}

			var data map[string]string

			err = json.Unmarshal(msg.Value, &data)
			if err != nil {
				log.Println(err)
				continue
			}

			// 3. Print message
			fmt.Printf("Received message in insertion Worker: key=%s value=%s\n", string(msg.Key), string(msg.Value))

			//data["phoneUUID"] and data["userUUID"] insertion into db first(ONLY FOR DEMO MODE)
			//----TEMP CODE---
			_, err = db.Exec("INSERT INTO USERS (userUUID, userName) VALUES ($1, $2) ON CONFLICT (userUUID) DO NOTHING", data["userUUID"], "Messi")
			if err != nil {
				continue
			}
			_, err = db.Exec("INSERT INTO PHONES (phoneUUID, phoneName) VALUES ($1, $2) ON CONFLICT (phoneUUID) DO NOTHING", data["phoneUUID"], "Iphone")
			if err != nil {
				continue
			}
			//---TEMP CODE END---

			//on conflict do nothing is basiaclly idempotency check
			_, err = db.Exec("INSERT INTO RESERVATIONS (ticketID,phoneUUID, userUUID, status) VALUES ($1, $2, $3, $4) ON CONFLICT (ticketID) DO NOTHING", data["ticketUUID"], data["phoneUUID"], data["userUUID"], "RESERVED")
			if err != nil {
				continue
			}

			fmt.Print("Things work fine")
		}
	}()

	go func() {
		//race condition + idempotency check
		for {
			msg, err := updateReservationReader.ReadMessage(ctx)

			if err != nil {
				log.Println("Error while reading:", err)
				return
			}
			fmt.Printf("Received message in insertion Worker: key=%s value=%s\n", string(msg.Key), string(msg.Value))

			var event PaymentEvent
			json.Unmarshal(msg.Value, &event)

			// --- Retry Logic for Race Condition ---
			maxRetries := 5
			success := false

			for i := 1; i <= maxRetries; i++ {
				// 1. Attempt the update
				res, err := db.Exec("UPDATE RESERVATIONS SET status = $1 WHERE ticketID = $2", "PAID", event.TicketUUID)

				if err != nil {
					log.Printf("Attempt %d: DB query error: %v\n", i, err)
				} else {
					// 2. Check if the row actually existed
					rowsAffected, err := res.RowsAffected()
					if err == nil && rowsAffected > 0 {
						fmt.Printf("Success! Ticket %s updated to PAID on attempt %d\n", event.TicketUUID, i)
						success = true
						break // It worked! Break out of the retry loop.
					}

					log.Printf("Attempt %d: Ticket %s not found yet. Waiting for insertion...\n", i, event.TicketUUID)
				}

				// 3. If we haven't reached max retries yet, sleep before trying again
				if i < maxRetries {
					time.Sleep(500 * time.Millisecond)
				}
			}

			// 4. If we exhausted all 5 attempts and it still failed
			if !success {
				log.Printf("CRITICAL ERROR: Could not update payment for ticket %s after %d attempts. Dropping message.\n", event.TicketUUID, maxRetries)
				continue // Skip to the next Kafka message so the worker doesn't die
			}

			// If success == true, publish the "reservation_confirmed" event for the emailer

		}
	}()

	<-ctx.Done() //blocked here
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
