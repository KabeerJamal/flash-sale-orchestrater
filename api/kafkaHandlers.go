package api

import (
	"context"
	"encoding/json"
	"io"
	"log"
	producer "myproject/Producer"
	"net/http"
	"os"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stripe/stripe-go/v75"
	"github.com/stripe/stripe-go/v75/checkout/session"
	"github.com/stripe/stripe-go/v78/webhook"
)

func buyRequest(rdb *redis.Client, reservationWriter *kafka.Writer) gin.HandlerFunc {
	return func(c *gin.Context) {
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
	}

}

func getTicketStatus(rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
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
	}

}

func createPayment(rdb *redis.Client, ctx context.Context) gin.HandlerFunc {
	return func(c *gin.Context) {
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
	}
}

func handleWebhook(rdb *redis.Client, ctx context.Context, paymentWriter *kafka.Writer) gin.HandlerFunc {
	return func(c *gin.Context) {
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
	}
}
