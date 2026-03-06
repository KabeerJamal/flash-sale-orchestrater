package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	producer "myproject/Producer"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stripe/stripe-go/v78/webhook"
)

/*
Database/Migrations
Redis Setup
HTTP Router Setup
*/

func RunAPI(ctx context.Context, migrationURL string) error {

	user := os.Getenv("POSTGRES_USER")
	pass := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")
	host := os.Getenv("DB_HOST_local")

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_local")
	redisAddress := os.Getenv("REDIS_local")

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer rdb.Close()

	// prefill it in redis (POTENTIAL ERROR, if main shuts down and runs again, refill happens even when not supposed to happen)
	_, err := rdb.Set(context.Background(), "reservation", 10, 0).Result()
	if err != nil {
		return fmt.Errorf("redis error: %w", err)
	}

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable",
		user, pass, host, dbname,
	)

	//---APPLYING MIGRATION---
	// log.Printf("DB CONN STR: %s", connStr)
	// wd, err := os.Getwd()
	// if err != nil {
	// 	fmt.Println("Error getting working directory:", err)
	// } else {
	// 	fmt.Println("Current working directory:", wd)
	// }
	m, err := migrate.New(migrationURL, connStr)

	if err != nil {
		return fmt.Errorf("migration failed: %w", err)
	}
	err = m.Up() // applies all unapplied migrations automatically
	if err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("migration failed: %w", err)
	}

	//browser sends a request with request body data
	r := gin.Default()

	reservationWriter := createWriter(kafkaBrokerAddress, "Reservations")
	paymentWriter := createWriter(kafkaBrokerAddress, "Payment")

	r.POST("/buy-request", func(c *gin.Context) {
		var body map[string]string
		c.BindJSON(&body)

		//generate a ticketUUID and pass it in body
		ticketUUID := uuid.New().String()
		body["ticketUUID"] = ticketUUID

		b, _ := json.Marshal(body)

		go producer.StartProducer(reservationWriter, ticketUUID, b)

		//put ticketUUID and status in redis
		rdb.Set(context.Background(), ticketUUID, "PENDING", 0).Err()

		c.JSON(200, gin.H{"ticketUUID": ticketUUID, "status": "PENDING"}) //Immediate response with ticket ID and status as pending
	})

	r.GET("/status/:ticketId", func(c *gin.Context) {
		//get ticket uuid from param
		ticketUUID := c.Param("ticketId")

		//respond with whats in redis
		resp, err := rdb.Get(context.Background(), ticketUUID).Result()

		if err == redis.Nil {
			// Key not found — expected case
			log.Printf("ticket not found: %s", ticketUUID)
			c.JSON(http.StatusNotFound, gin.H{
				"error": "ticket not found",
			})
			return // or return custom error
		}
		if err != nil {
			log.Printf("redis GET failed for ticket %s: %v", ticketUUID, err)
			return
		}
		c.JSON(200, gin.H{"ticketUUID": ticketUUID, "status": resp})

		//its frontend duty to keep in polling until it recieves a certain response
	})

	//get stripe webhook api request
	//write to kafka
	//payment reads from kafka
	//whsec_ff38d757499e475d599e089e9cf8100d9c46fe5b6b94f29917a8c8db7a24bdf3
	r.POST("/webhook", func(c *gin.Context) {
		endpointSecret := os.Getenv("STRIPE_WEBHOOK_SECRET")

		//give me raw bytes of this request
		payload, err := io.ReadAll(c.Request.Body)
		if err != nil {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}

		sigHeader := c.GetHeader("Stripe-Signature")
		if sigHeader == "" {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}

		//event contains everything Stripe sent about that event.
		event, err := webhook.ConstructEventWithOptions(
			payload,
			sigHeader,
			endpointSecret,
			webhook.ConstructEventOptions{
				IgnoreAPIVersionMismatch: true,
			},
		)
		if err != nil {
			c.AbortWithStatus(http.StatusBadRequest) // signature check failed
			return
		}
		b, err := json.Marshal(event.Data.Object)
		if err != nil {
			c.AbortWithStatus(http.StatusBadRequest)
			return
		}

		go producer.StartProducer(paymentWriter, "userIdBroski", b)
		//sends HTTP 200 OK back to Stripe.
		c.Status(http.StatusOK)

	})

	/*r.Run() is an infinite loop. It completely ignores the ctx context.
	Context you passed in, and it will block your test forever.
	The return nil below it will literally never execute.*/
	// r.Run() //starts server on localhost:8080

	srv := &http.Server{
		Addr:    ":8080",
		Handler: r,
	}

	// Start the server in the background so it doesn't block!
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("listen: %s\n", err)
		}
	}()

	// Block here until the test says "Stop!" by cancelling the context
	<-ctx.Done()
	log.Println("Shutting down API...")
	return srv.Shutdown(context.Background())
}

func createWriter(kafkaBrokerAddress string, topic string) *kafka.Writer {
	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBrokerAddress},
		Topic:   topic,
		//Balancer: &kafka.LeastBytes{},
		Balancer: &kafka.Hash{},
	})
	return w
}

type PaymentEvent struct {
	TicketUUID      string
	PhoneUUID       string
	UserUUID        string
	PaymentIntentID string
	Amount          int64
	Currency        string
	Status          string
}

func filterStripeData(data map[string]interface{}) (PaymentEvent, error) {
	var filteredData PaymentEvent

	filteredData.TicketUUID = data["metadata"].(map[string]interface{})["ticketUUID"].(string)
	filteredData.PhoneUUID = data["metadata"].(map[string]interface{})["phoneUUID"].(string)
	filteredData.UserUUID = data["metadata"].(map[string]interface{})["userUUID"].(string)
	filteredData.PaymentIntentID = data["id"].(string)
	filteredData.Amount = int64(data["amount_total"].(float64))
	filteredData.Currency = data["currency"].(string)
	filteredData.Status = data["payment_status"].(string)

	return filteredData, nil
}

func convertToBytes(data any) ([]byte, error) {
	return json.Marshal(data)
}

