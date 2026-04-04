package api

import (
	"bytes"
	"fmt"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/stripe/stripe-go/v78/webhook"
)

type stripeWebhookTestRequest struct {
	UserUUID      string `json:"userUUID"`
	PhoneUUID     string `json:"phoneUUID"`
	TicketUUID    string `json:"ticketUUID"`
	PaymentStatus string `json:"paymentStatus"`
}

func callStripeWebhook() gin.HandlerFunc {
	return func(c *gin.Context) {
		var req stripeWebhookTestRequest
		if err := c.ShouldBindJSON(&req); err != nil {
			c.JSON(400, gin.H{"error": err.Error()})
			return
		}

		payload := fmt.Sprintf(`{
            "type": "checkout.session.completed",
            "data": {
                "object": {
                    "id": "pi_test_123",
                    "amount_total": 1000,
                    "currency": "usd",
                    "payment_status": "%s",
                    "metadata": {
                        "ticketUUID": "%s",
                        "phoneUUID": "%s",
                        "userUUID": "%s"
                    }
                }
            }
        }`, req.PaymentStatus, req.TicketUUID, req.PhoneUUID, req.UserUUID)

		testSecret := os.Getenv("STRIPE_WEBHOOK_SECRET")
		signedPayload := webhook.GenerateTestSignedPayload(&webhook.UnsignedPayload{
			Payload: []byte(payload),
			Secret:  testSecret,
		})

		httpReq, err := http.NewRequest("POST", "http://localhost:8080/webhook", bytes.NewBuffer(signedPayload.Payload))
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		httpReq.Header.Set("Stripe-Signature", signedPayload.Header)

		client := &http.Client{}
		resp, err := client.Do(httpReq)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		defer resp.Body.Close()

		c.JSON(resp.StatusCode, gin.H{"status": "ok"})
	}
}
