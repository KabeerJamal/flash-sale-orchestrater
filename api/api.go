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
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stripe/stripe-go/v75"
	"github.com/stripe/stripe-go/v75/checkout/session"
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

	stripe.Key = os.Getenv("STRIPE_SECRET_KEY")

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_local")
	redisAddress := os.Getenv("REDIS_local")

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer rdb.Close()
	paymentCancelledWriter := createWriter(kafkaBrokerAddress, "Payment-Failed")
	defer paymentCancelledWriter.Close()
	go pollExpiredTimers(ctx, rdb, paymentCancelledWriter)

	// prefill it in redis (POTENTIAL ERROR, if main shuts down and runs again, refill happens even when not supposed to happen)
	_, err := rdb.Set(context.Background(), "reservation", 10, 0).Result()
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

	reservationWriter := createWriter(kafkaBrokerAddress, "Reservations")
	paymentWriter := createWriter(kafkaBrokerAddress, "Payment")
	defer reservationWriter.Close()
	defer paymentWriter.Close()

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

	//in this function you create a stripe checkout session and attach to it metadata which will be needed when
	//stripe sends webhook upon successful payment
	r.POST("/pay", func(c *gin.Context) {
		//body has ticketUUID, userUUID, phoneUUID
		var body map[string]string
		if err := c.BindJSON(&body); err != nil {
			log.Fatal(err)
			return // Handle error
		}

		//get url from checkoutsession and return it as response
		params := &stripe.CheckoutSessionParams{
			// ... (Add LineItems, Mode, SuccessURL, CancelURL) ...
			Mode:       stripe.String(string(stripe.CheckoutSessionModePayment)),
			SuccessURL: stripe.String("https://example.com/success"), //TODO:update this
			CancelURL:  stripe.String("https://example.com/cancel"),  //TODO:update this
			LineItems: []*stripe.CheckoutSessionLineItemParams{
				{
					PriceData: &stripe.CheckoutSessionLineItemPriceDataParams{
						Currency:   stripe.String("usd"),
						UnitAmount: stripe.Int64(5000),
						ProductData: &stripe.CheckoutSessionLineItemPriceDataProductDataParams{
							Name: stripe.String(body["phoneUUID"]), // Or a real product name
						},
					},
					Quantity: stripe.Int64(1),
				},
			},
			Metadata: map[string]string{
				"ticketUUID": body["ticketUUID"],
				"userUUID":   body["userUUID"],
				"phoneUUID":  body["phoneUUID"],
			},
			ExpiresAt: stripe.Int64(time.Now().Add(30 * time.Minute).Unix()),
		}
		s, err := session.New(params) //stripe api call
		if err != nil {
			log.Fatal(err)
			return
		}

		//call a function which adds ticket uuid to the ZSET
		memberData := body["ticketUUID"] + "|" + body["userUUID"] + "|" + body["phoneUUID"]
		StartTimer(ctx, rdb, memberData, 5*time.Minute)

		//create a function which polls, and if it finds something, it creeates a topic which writes to kafka worker

		c.JSON(200, gin.H{"url": s.URL})

	})

	//get stripe webhook api request
	//write to kafka
	//payment reads from kafka
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

		//stripe sends multiple api calls of differnet even types. only the event type
		//"checkout.session.completed contains the metadata"
		switch event.Type {
		case "checkout.session.completed", "checkout.session.async_payment_completed":
			stripeData, err := filterStripeData(event.Data.Object)
			if err != nil {
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
			stripeDataBytes, err := convertToBytes(stripeData)
			if err != nil {
				c.AbortWithStatus(http.StatusInternalServerError)
				return
			}
			//Idempotency Check
			val, err := rdb.Get(ctx, stripeData.TicketUUID).Result()
			if err == nil && val == "PAID" {
				//already processed this exact payment
				c.Status(http.StatusOK) // Tell Stripe "thanks, we got it"
				return
			}

			if _, ok := event.Data.Object["payment_status"].(string); ok {
				go producer.StartProducer(paymentWriter, stripeData.TicketUUID, stripeDataBytes)
			}
			c.Status(http.StatusOK)

		case "checkout.session.expired":
			//send same request but payment status is not paid now(need to check). might need to filter stripe data
			stripeData, err := filterStripeData(event.Data.Object)
			if err != nil {
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
			stripeDataBytes, err := convertToBytes(stripeData)
			if err != nil {
				c.AbortWithStatus(http.StatusInternalServerError)
				return
			}
			//idempotency check, doesnt cover all cases
			val, err := rdb.Get(ctx, stripeData.TicketUUID).Result()
			if err == nil && val == "FAILED" {
				//already processed this exact payment
				c.Status(http.StatusOK) // Tell Stripe "thanks, we got it"
				return
			}
			if _, ok := event.Data.Object["payment_status"].(string); ok {
				go producer.StartProducer(paymentWriter, stripeData.TicketUUID, stripeDataBytes)
				c.Status(http.StatusOK)
			}
			c.Status(http.StatusOK)

		case "checkout.session.async_payment_failed":
			//send same request but payment status is not paid now. might need to filter stripe data
			stripeData, err := filterStripeData(event.Data.Object)
			if err != nil {
				c.AbortWithStatus(http.StatusBadRequest)
				return
			}
			stripeDataBytes, err := convertToBytes(stripeData)
			if err != nil {
				c.AbortWithStatus(http.StatusInternalServerError)
				return
			}
			//idempotency check, doesnt cover all cases
			//idempotency check, doesnt cover all cases
			val, err := rdb.Get(ctx, stripeData.TicketUUID).Result()
			if err == nil && val == "FAILED" {
				//already processed this exact payment
				c.Status(http.StatusOK) // Tell Stripe "thanks, we got it"
				return
			}
			go producer.StartProducer(paymentWriter, stripeData.TicketUUID, stripeDataBytes)
			c.Status(http.StatusOK)
		default:
			// Ignore other events safely
			c.Status(http.StatusOK)
			return
		}

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
	err := rdb.ZAdd(ctx, "flash_sale_timers", redis.Z{
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
				Key:     "flash_sale_timers",
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
					removed := rdb.ZRem(ctx, "flash_sale_timers", memberString).Val()

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
