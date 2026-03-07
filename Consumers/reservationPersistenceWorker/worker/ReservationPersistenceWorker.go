package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"

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
