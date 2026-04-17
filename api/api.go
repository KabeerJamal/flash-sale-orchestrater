package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	producer "myproject/Producer"
	"myproject/shared"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stripe/stripe-go/v75"
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

	stripe.Key = os.Getenv("STRIPE_SECRET_KEY")

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_local")
	redisAddress := os.Getenv("REDIS_local")

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer rdb.Close()

	paymentCancelledWriter := createWriter(kafkaBrokerAddress, shared.TopicPaymentFailure)
	defer paymentCancelledWriter.Close()
	go pollExpiredTimers(ctx, rdb, paymentCancelledWriter)

	// prefill it in redis (POTENTIAL ERROR, if main shuts down and runs again, refill happens even when not supposed to happen)
	//TODO: need to fix this problem
	totalProducts, _ := strconv.Atoi(os.Getenv("TOTAL_PRODUCTS"))
	_, err := rdb.Set(context.Background(), shared.Reservations, totalProducts, 0).Result()
	if err != nil {
		return fmt.Errorf("redis error: %w", err)
	}

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable",
		user, pass, host, dbname,
	)

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

	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"http://localhost:3000"},
		AllowMethods:     []string{"GET", "POST", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept"},
		AllowCredentials: true,
	}))

	//reservationWriter := createWriter(kafkaBrokerAddress, shared.TopicReservation)
	paymentWriter := createWriter(kafkaBrokerAddress, shared.TopicPayment)
	//defer reservationWriter.Close()
	defer paymentWriter.Close()

	db, err := sql.Open("pgx", connStr)

	if err != nil {
		slog.Error("DB connection not established", "error", err)
	}

	err = db.Ping()
	if err != nil {
		slog.Error("DB Ping failed", "error", err)
	}

	defer db.Close()

	//r.POST("/buy-request", buyRequest(rdb, reservationWriter))
	r.POST("/buy-request", buyRequest(rdb))

	r.GET("/status/:ticketId", getTicketStatus(rdb))

	//in this function you create a stripe checkout session and attach to it metadata which will be needed when
	//stripe sends webhook upon successful payment
	r.POST("/pay", createPayment(rdb, ctx))

	//get stripe webhook api request
	//write to kafka
	//payment reads from kafka
	r.POST("/webhook", handleWebhook(rdb, ctx, paymentWriter))

	//Fetch Users
	r.GET("/users", getUsers())

	r.GET("/test-users", getTestUsers())

	//Fetch Phones
	r.GET("/phones", getPhones())

	r.GET("/phones/:phoneUUID/status", getPhoneStatus(rdb))

	r.POST("/reset", loadTestingTearDown(rdb, db))

	r.POST("/stripeWebhookTest", callStripeWebhook())

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
	TicketUUID      string `json:"ticketUUID"`
	PhoneUUID       string `json:"phoneUUID"`
	UserUUID        string `json:"userUUID"`
	PaymentIntentID string `json:"paymentIntentID"`
	Amount          int64  `json:"amount"`
	Currency        string `json:"currency"`
	Status          string `json:"status"`
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

func StartTimer(ctx context.Context, rdb *redis.Client, memberData string, ttl time.Duration) error {
	expireTime := float64(time.Now().Add(ttl).Unix())
	err := rdb.ZAdd(ctx, shared.FlashSaleTimers, redis.Z{
		Score:  expireTime,
		Member: memberData, // The data you want to retrieve later
	}).Err()
	if err != nil {
		return err
	}
	return nil
}

func pollExpiredTimers(ctx context.Context, rdb *redis.Client, paymentCancelledWriter *kafka.Writer) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			time.Sleep(500 * time.Millisecond) //added timer here so CPU is not busy 100% of the time
			currentTime := float64(time.Now().Unix())
			list, err := rdb.ZRangeArgs(ctx, redis.ZRangeArgs{
				Key:     shared.FlashSaleTimers,
				Start:   "-inf",
				Stop:    fmt.Sprintf("%f", currentTime),
				ByScore: true,
			}).Result()

			if err != nil {
				log.Println("Error: ", err)
				time.Sleep(time.Second)
				continue
			}
			if len(list) != 0 {

				//get the list
				for _, memberString := range list {
					// .Val() returns the number of items deleted (1 if success, 0 if another worker beat us to it)
					removed := rdb.ZRem(ctx, shared.FlashSaleTimers, memberString).Val()

					//create a payment ,message with everything null, but ticket id phone uuid, status and user uuid filled

					//to ensure race condition is avodied
					if removed > 0 {
						var paymentMessage PaymentEvent
						parts := strings.Split(memberString, "|")
						if len(parts) != 3 {
							continue // safe fallback
						}

						ticketUUID := parts[0]
						phoneUUID := parts[1]
						userUUID := parts[2]

						paymentMessage = PaymentEvent{
							TicketUUID: ticketUUID,
							PhoneUUID:  phoneUUID,
							UserUUID:   userUUID,
							Status:     "unpaid", // because the timer expired
						}
						paymentMessageByte, err := convertToBytes(paymentMessage)
						if err != nil {
							continue
						}

						go producer.StartProducer(paymentCancelledWriter, ticketUUID, paymentMessageByte)

					}

				}
			}
		}
	}

}
