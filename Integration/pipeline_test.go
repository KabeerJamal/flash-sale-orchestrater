package test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"

	"log/slog"
	paymentWorker "myproject/Consumers/paymentWorker/worker"
	reservationPersistenceWorker "myproject/Consumers/reservationPersistenceWorker/worker"
	reservationWorker "myproject/Consumers/reservationWorker/worker"
	rollbackWorker "myproject/Consumers/rollbackWorker/worker"
	"myproject/api"
	"net/http"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/stripe/stripe-go/v78/webhook"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"
)

type TestEnv struct {
	redisC            *testcontainers.DockerContainer
	redpandaContainer *redpanda.Container
	postgresContainer *postgres.PostgresContainer
	redisEndpoint     string
	brokerAddress     string
	dbEndpoint        string
	db                *sql.DB
}

func init() {
	opts := &slog.HandlerOptions{
		AddSource: true,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	slog.SetDefault(logger)
}

func TestIntegrationPipeline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	env := setUpTestEnv(t, ctx)

	redisC := env.redisC
	db := env.db
	redpandaContainer := env.redpandaContainer
	postgresContainer := env.postgresContainer
	testcontainers.CleanupContainer(t, redisC)

	defer func() {
		if err := testcontainers.TerminateContainer(redpandaContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	defer func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	defer db.Close()

	startWorkers(t, ctx)

	waitForApi(t)

	//Run possible test cases
	//Running this test case first, matters
	t.Run("Fake ticker ID status check", func(t *testing.T) {
		resp, err := http.Get("http://localhost:8080/status/2345")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	//post request followed by get request
	t.Run("successful reservation flow", func(t *testing.T) {
		body := `{"phoneUUID": "3f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a90" , "userUUID": "b6a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e41"}`
		phoneUUID := "3f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a90"
		userUUID := "b6a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e41"

		//test redis check
		host, err := redisC.Host(ctx)
		require.NoError(t, err)

		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		initialValueRedisReservation, err := rdb.Get(ctx, "reservation").Result()
		require.NoError(t, err)
		initialValueRedisReservationInt, err := strconv.Atoi(initialValueRedisReservation)

		ticketUUID := doReservation(t, body, "SUCCESSFUL_RESERVATION")

		//----TEST IN REDIS---

		val, err := rdb.Get(ctx, "reservation").Result()
		valInt, err := strconv.Atoi(val)
		require.NoError(t, err)

		require.Equal(t, initialValueRedisReservationInt, valInt+1)

		//--TEST IF INSERTION HAPPENS
		// 1. Declare variables to hold the data we read from the database
		var dbPhoneUUID, dbUserUUID, dbStatus string

		// 2. Query the database using the ticketUUID, and scan the results into our variables
		err = db.QueryRow(
			"SELECT phoneUUID, userUUID, status FROM RESERVATIONS WHERE ticketID = $1",
			ticketUUID,
		).Scan(&dbPhoneUUID, &dbUserUUID, &dbStatus)
		require.NoError(t, err) // Fails the test if the row doesn't exist or query fails

		// 3. Verify the database values match the values from our HTTP request
		require.Equal(t, phoneUUID, dbPhoneUUID)
		require.Equal(t, userUUID, dbUserUUID)
		require.Equal(t, "RESERVED", dbStatus)

	})

	t.Run("successful payment flow", func(t *testing.T) {
		body := `{"phoneUUID": "3f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a91" , "userUUID": "b6a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e42"}`
		phoneUUID := "3f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a91"
		userUUID := "b6a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e42"

		ticketUUID := doReservation(t, body, "SUCCESSFUL_RESERVATION")

		//mock ann send webhook,test if response 200
		//mock json payload
		payload := fmt.Sprintf(`{
			"type": "checkout.session.completed",
			"data": {
				"object": {
					"id": "pi_test_123",
					"amount_total": 1000,
					"currency": "usd",
					"payment_status": "paid",
					"metadata": {
						"ticketUUID": "%s",
						"phoneUUID": "%s",
						"userUUID": "%s"
					}
				}
			}
		}`, ticketUUID, phoneUUID, userUUID)

		testSecret := os.Getenv("STRIPE_WEBHOOK_SECRET")

		signedPayload := webhook.GenerateTestSignedPayload(&webhook.UnsignedPayload{
			Payload: []byte(payload),
			Secret:  testSecret,
		})

		//create a post request http, with webhook endpoint, attaching releavant stuff asheaders
		req, err := http.NewRequest("POST", "http://localhost:8080/webhook", bytes.NewBuffer(signedPayload.Payload))
		require.NoError(t, err)
		req.Header.Set("Stripe-Signature", signedPayload.Header)

		client := &http.Client{}
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		//test redis check 	err = rdb.Set(ctx, event.TicketUUID, "PAID", 0).Err(), check total paid value totalPaid, err := rdb.Incr(ctx, "total_paid").Result() to be 1
		host, err := redisC.Host(ctx)
		require.NoError(t, err)

		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})

		require.Eventually(t, func() bool {
			resp, err := http.Get(fmt.Sprintf("http://localhost:8080/status/%s", ticketUUID))
			if err != nil {
				return false // Network error, try again on the next tick
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return false // Not 200 OK yet, try again
			}

			statusRespBody, err := io.ReadAll(resp.Body)
			if err != nil {
				return false // Error reading body, try again
			}

			var statusResponse map[string]string
			if err := json.Unmarshal(statusRespBody, &statusResponse); err != nil {
				return false // Error parsing JSON, try again
			}

			// If it matches, this returns true and the test immediately passes!
			return statusResponse["status"] == "PAID"

		}, 15*time.Second, 500*time.Millisecond, "Expected status to become PAID within 15 seconds")
		require.Eventually(t, func() bool {
			val, err := rdb.Get(ctx, "total_paid").Result()
			if err != nil {
				return false
			}
			return val == "1"
		}, 15*time.Second, 500*time.Millisecond, "Expected total_paid to be 1 in Redis")

		//check db , status should be reserved for that tikcet uuid
		require.Eventually(t, func() bool {
			var dbPhoneUUID, dbUserUUID, dbStatus string
			err := db.QueryRow(
				"SELECT phoneUUID, userUUID, status FROM RESERVATIONS WHERE ticketID = $1",
				ticketUUID,
			).Scan(&dbPhoneUUID, &dbUserUUID, &dbStatus)
			if err != nil {
				return false
			}
			return dbPhoneUUID == phoneUUID && dbUserUUID == userUUID && dbStatus == "PAID"
		}, 15*time.Second, 500*time.Millisecond, "Expected DB status to be PAID")
	})

	t.Run("Unsuccessful payment flow", func(t *testing.T) {

		body := `{"phoneUUID": "3f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a93" , "userUUID": "b6a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e44"}`
		phoneUUID := "3f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a93"
		userUUID := "b6a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e44"

		ticketUUID := doReservation(t, body, "SUCCESSFUL_RESERVATION")

		//test redis check
		host, err := redisC.Host(ctx)
		require.NoError(t, err)

		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		initialValueRedisReservation, err := rdb.Get(ctx, "reservation").Result()
		require.NoError(t, err)
		initialValueRedisReservationInt, err := strconv.Atoi(initialValueRedisReservation)

		//mock ann send webhook,test if response 200
		//mock json payload
		payload := fmt.Sprintf(`{
			"type": "checkout.session.expired",
			"data": {
				"object": {
					"id": "pi_test_123",
					"amount_total": 1000,
					"currency": "usd",
					"payment_status": "unpaid",
					"metadata": {
						"ticketUUID": "%s",
						"phoneUUID": "%s",
						"userUUID": "%s"
					}
				}
			}
		}`, ticketUUID, phoneUUID, userUUID)

		testSecret := os.Getenv("STRIPE_WEBHOOK_SECRET")

		signedPayload := webhook.GenerateTestSignedPayload(&webhook.UnsignedPayload{
			Payload: []byte(payload),
			Secret:  testSecret,
		})

		//create a post request http, with webhook endpoint, attaching releavant stuff asheaders
		req, err := http.NewRequest("POST", "http://localhost:8080/webhook", bytes.NewBuffer(signedPayload.Payload))
		require.NoError(t, err)
		req.Header.Set("Stripe-Signature", signedPayload.Header)

		client := &http.Client{}
		resp, err := client.Do(req)
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		require.Eventually(t, func() bool {
			resp, err := http.Get(fmt.Sprintf("http://localhost:8080/status/%s", ticketUUID))
			if err != nil {
				return false // Network error, try again on the next tick
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return false // Not 200 OK yet, try again
			}

			statusRespBody, err := io.ReadAll(resp.Body)
			if err != nil {
				return false // Error reading body, try again
			}

			var statusResponse map[string]string
			if err := json.Unmarshal(statusRespBody, &statusResponse); err != nil {
				return false // Error parsing JSON, try again
			}

			// If it matches, this returns true and the test immediately passes!
			return statusResponse["status"] == "FAILED"

		}, 15*time.Second, 500*time.Millisecond, "Expected status to become FAILED within 15 seconds")
		require.Eventually(t, func() bool {
			val, err := rdb.Get(ctx, "reservation").Result()
			valInt, err := strconv.Atoi(val)
			if err != nil {
				return false
			}
			return valInt == initialValueRedisReservationInt+1
		}, 15*time.Second, 500*time.Millisecond, "Expected Resevation to be 10 in Redis")

		//check db , status should be reserved for that tikcet uuid
		require.Eventually(t, func() bool {
			var dbPhoneUUID, dbUserUUID, dbStatus string
			err := db.QueryRow(
				"SELECT phoneUUID, userUUID, status FROM RESERVATIONS WHERE ticketID = $1",
				ticketUUID,
			).Scan(&dbPhoneUUID, &dbUserUUID, &dbStatus)
			if errors.Is(err, sql.ErrNoRows) {
				return true
			}
			return false
		}, 15*time.Second, 500*time.Millisecond, "Expected DB status to be deleted")

	})

	t.Run("Unsuccessful payment flow(Expired timer)", func(t *testing.T) {
		//create user id and phone id
		body := `{"phoneUUID": "3f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a95" , "userUUID": "b6a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e46"}`
		phoneUUID := "3f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a95"
		userUUID := "b6a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e46"
		//do reservation (get ticket uuid)
		ticketUUID := doReservation(t, body, "SUCCESSFUL_RESERVATION")

		//test redis check
		host, err := redisC.Host(ctx)
		require.NoError(t, err)

		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		initialValueRedisReservation, err := rdb.Get(ctx, "reservation").Result()
		require.NoError(t, err)
		initialValueRedisReservationInt, err := strconv.Atoi(initialValueRedisReservation)

		//call start timer and pass it all member data
		memberData := ticketUUID + "|" + userUUID + "|" + phoneUUID

		//this will call pollexpired timer
		api.StartTimer(ctx, rdb, memberData, 1*time.Second)

		//then same assertions as above code, also check if its removed from Zset
		require.Eventually(t, func() bool {
			resp, err := http.Get(fmt.Sprintf("http://localhost:8080/status/%s", ticketUUID))
			if err != nil {
				return false // Network error, try again on the next tick
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return false // Not 200 OK yet, try again
			}

			statusRespBody, err := io.ReadAll(resp.Body)
			if err != nil {
				return false // Error reading body, try again
			}

			var statusResponse map[string]string
			if err := json.Unmarshal(statusRespBody, &statusResponse); err != nil {
				return false // Error parsing JSON, try again
			}

			// If it matches, this returns true and the test immediately passes!
			return statusResponse["status"] == "FAILED"

		}, 15*time.Second, 500*time.Millisecond, "Expected status to become FAILED within 15 seconds")
		require.Eventually(t, func() bool {
			val, err := rdb.Get(ctx, "reservation").Result()
			valInt, err := strconv.Atoi(val)
			if err != nil {
				return false
			}
			return valInt == initialValueRedisReservationInt+1
		}, 15*time.Second, 500*time.Millisecond, "Expected Resevation to be 10 in Redis")

		//check db , status should be reserved for that tikcet uuid
		require.Eventually(t, func() bool {
			var dbPhoneUUID, dbUserUUID, dbStatus string
			err := db.QueryRow(
				"SELECT phoneUUID, userUUID, status FROM RESERVATIONS WHERE ticketID = $1",
				ticketUUID,
			).Scan(&dbPhoneUUID, &dbUserUUID, &dbStatus)
			if errors.Is(err, sql.ErrNoRows) {
				return true
			}
			return false
		}, 15*time.Second, 500*time.Millisecond, "Expected DB status to be deleted")

	})

	//2 more test cases left
	t.Run("Assigned to wait list", func(t *testing.T) {
		body := `{"phoneUUID": "3f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a90" , "userUUID": "b6a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e41"}`
		// phoneUUID := "3f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a90"
		// userUUID := "b6a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e41"

		//test redis check
		host, err := redisC.Host(ctx)
		require.NoError(t, err)

		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		_, err = rdb.Set(context.Background(), "reservation", 0, 0).Result()
		require.NoError(t, err)

		doReservation(t, body, "WAITING_LIST")

		val, err := rdb.Get(ctx, "reservation").Result()
		require.Equal(t, "0", val)

	})

	t.Run("Promoted to wait list", func(t *testing.T) {
		/*If final assertions pass it must be the case that redis waiting queue is working fine*/
		//test redis check
		host, err := redisC.Host(ctx)
		require.NoError(t, err)

		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		rdb.Del(ctx, "waitlist_queue")
		_, err = rdb.Set(context.Background(), "reservation", 1, 0).Result()
		require.NoError(t, err)

		//need 2 users
		body1 := `{"phoneUUID": "1f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a97" , "userUUID": "16a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e47"}`
		phoneUUID1 := "3f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a97"
		userUUID1 := "b6a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e47"

		body2 := `{"phoneUUID": "1f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a98" , "userUUID": "26a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e48"}`
		phoneUUID2 := "1f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a98"
		userUUID2 := "26a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e48"

		ticketUUID1 := doReservation(t, body1, "SUCCESSFUL_RESERVATION")
		ticketUUID2 := doReservation(t, body2, "WAITING_LIST")

		memberData := ticketUUID1 + "|" + userUUID1 + "|" + phoneUUID1

		//this will call pollexpired timer
		api.StartTimer(ctx, rdb, memberData, 1*time.Second)

		//then same assertions as above code, also check if its removed from Zset
		require.Eventually(t, func() bool {
			resp, err := http.Get(fmt.Sprintf("http://localhost:8080/status/%s", ticketUUID1))
			if err != nil {
				return false // Network error, try again on the next tick
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return false // Not 200 OK yet, try again
			}

			statusRespBody, err := io.ReadAll(resp.Body)
			if err != nil {
				return false // Error reading body, try again
			}

			var statusResponse map[string]string
			if err := json.Unmarshal(statusRespBody, &statusResponse); err != nil {
				return false // Error parsing JSON, try again
			}

			// If it matches, this returns true and the test immediately passes!
			return statusResponse["status"] == "FAILED"

		}, 15*time.Second, 500*time.Millisecond, "Expected status to become FAILED within 15 seconds")

		//check db , status should be reserved for that tikcet uuid
		require.Eventually(t, func() bool {
			var dbPhoneUUID, dbUserUUID, dbStatus string
			err := db.QueryRow(
				"SELECT phoneUUID, userUUID, status FROM RESERVATIONS WHERE ticketID = $1",
				ticketUUID1,
			).Scan(&dbPhoneUUID, &dbUserUUID, &dbStatus)
			if errors.Is(err, sql.ErrNoRows) {
				return true
			}
			return false
		}, 15*time.Second, 500*time.Millisecond, "Expected DB status to be deleted")

		require.Eventually(t, func() bool {
			resp, err := http.Get(fmt.Sprintf("http://localhost:8080/status/%s", ticketUUID2))
			if err != nil {
				return false // Network error, try again on the next tick
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				return false // Not 200 OK yet, try again
			}

			statusRespBody, err := io.ReadAll(resp.Body)
			if err != nil {
				return false // Error reading body, try again
			}

			var statusResponse map[string]string
			if err := json.Unmarshal(statusRespBody, &statusResponse); err != nil {
				return false // Error parsing JSON, try again
			}

			// If it matches, this returns true and the test immediately passes!
			return statusResponse["status"] == "SUCCESSFUL_RESERVATION"

		}, 15*time.Second, 500*time.Millisecond, "Expected status for user B to become SUCCESSFUL_RESERVATION within 15 seconds")
		require.Eventually(t, func() bool {
			val, err := rdb.Get(ctx, "reservation").Result()
			valInt, err := strconv.Atoi(val)
			if err != nil {
				return false
			}
			return valInt == 0
		}, 15*time.Second, 500*time.Millisecond, "Expected Resevation to be 0 in Redis")
		//poll and check db
		require.Eventually(t, func() bool {
			var dbPhoneUUID, dbUserUUID, dbStatus string
			err := db.QueryRow(
				"SELECT phoneUUID, userUUID, status FROM RESERVATIONS WHERE ticketID = $1",
				ticketUUID2,
			).Scan(&dbPhoneUUID, &dbUserUUID, &dbStatus)
			if err != nil {
				return false
			}
			return dbPhoneUUID == phoneUUID2 && dbUserUUID == userUUID2 && dbStatus == "RESERVED"
		}, 15*time.Second, 500*time.Millisecond, "Expected DB status to be RESERVED")

	})

}

func doReservation(t *testing.T, body string, status string) string {
	// var requestData map[string]string
	// err := json.Unmarshal([]byte(body), &requestData)
	// require.NoError(t, err)

	resp, err := http.Post("http://localhost:8080/buy-request", "application/json", bytes.NewBuffer([]byte(body)))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	buyRespBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var buyResponse map[string]string
	err = json.Unmarshal(buyRespBody, &buyResponse)
	require.NoError(t, err)

	// {"ticketUUID": x, "status":y}
	require.Equal(t, "PENDING", buyResponse["status"])
	require.NotEmpty(t, buyResponse["ticketUUID"])

	/*
		Because your workers are running in the background via Kafka,
		if you instantly call GET /status, it might still say "PENDING".
		You have to write a small polling loop (or a time.Sleep for now)
		to give the worker a few milliseconds to process the Kafka message.
	*/
	ticketUUID := buyResponse["ticketUUID"]

	// require.Eventually will run this function every 500ms for up to 15 seconds.
	// As soon as it returns true, the test passes and moves on instantly!
	require.Eventually(t, func() bool {
		resp, err := http.Get(fmt.Sprintf("http://localhost:8080/status/%s", ticketUUID))
		if err != nil {
			return false // Network error, try again on the next tick
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return false // Not 200 OK yet, try again
		}

		statusRespBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return false // Error reading body, try again
		}

		var statusResponse map[string]string
		if err := json.Unmarshal(statusRespBody, &statusResponse); err != nil {
			return false // Error parsing JSON, try again
		}

		// If it matches, this returns true and the test immediately passes!
		return statusResponse["status"] == status

	}, 15*time.Second, 500*time.Millisecond, "Expected status to become SUCCESSFUL_RESERVATION within 15 seconds")

	return ticketUUID

}

func waitForApi(t *testing.T) {
	// Give the API time to boot dynamically (waits up to 10 seconds, checks every 100ms)
	require.Eventually(t, func() bool {
		// We just make a dummy request to see if the port is open
		resp, err := http.Get("http://localhost:8080/")
		if err != nil {
			return false // "connection refused" -> server not up yet
		}
		resp.Body.Close()
		return true // Connection succeeded, Gin is running!
	}, 10*time.Second, 100*time.Millisecond, "API server did not start in time")
}

func startWorkers(t *testing.T, ctx context.Context) {
	go func() {
		err := api.RunAPI(ctx, "file://../migrations")
		// We can't use require.NoError inside a goroutine easily, so we just log it
		if err != nil {
			t.Logf("API stopped: %v", err)
		}
	}()

	go func() {
		err := reservationPersistenceWorker.ReservationPersistenceWorker(ctx)
		if err != nil {
			t.Logf("InsertionWorker stopped: %v", err)
		}
	}()

	go func() {
		err := reservationWorker.ReservationWorker(ctx)
		if err != nil {
			t.Logf("InsertionWorker stopped: %v", err)
		}
	}()

	go func() {
		err := paymentWorker.PaymentWorker(ctx)
		if err != nil {
			t.Logf("PaymentWorker stopped: %v", err)
		}
	}()

	go func() {
		err := rollbackWorker.RollbackWorker(ctx)
		if err != nil {
			t.Logf("Rollback stopped: %v", err)
		}
	}()
}

func setUpTestEnv(t *testing.T, ctx context.Context) TestEnv {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true") //Debug test was not working but now it works. see detials in Notion
	redisC, err := testcontainers.Run(
		ctx, "redis:8-alpine",
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("6379/tcp"),
			wait.ForLog("Ready to accept connections"),
		),
	)
	require.NoError(t, err)

	//container port defined internally, 9092 → Kafka API (container port)
	redpandaContainer, err := redpanda.Run(ctx,
		"docker.redpanda.com/redpandadata/redpanda:v23.3.3",
		redpanda.WithAutoCreateTopics(),
	)
	if err != nil {
		log.Printf("failed to start container: %s", err)
		return TestEnv{}
	}

	//could use the .env file here.
	dbName := "users"
	dbUser := "user"
	dbPassword := "password"

	//in api.go it uses the env variables in the env file instead, thats why did this.
	os.Setenv("POSTGRES_USER", "user")
	os.Setenv("POSTGRES_PASSWORD", "password")
	os.Setenv("POSTGRES_DB", "users")

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		log.Printf("failed to start container: %s", err)
		return TestEnv{}
	}

	redisEndpoint, err := redisC.Endpoint(ctx, "")
	require.NoError(t, err)

	dbEndpoint, err := postgresContainer.Endpoint(ctx, "")
	require.NoError(t, err)

	// Dial the broker from your Go code (Host machine)
	brokerAddress, err := redpandaContainer.KafkaSeedBroker(ctx)
	conn, err := kafka.Dial("tcp", brokerAddress)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	//database connection string
	connString := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable",
		dbUser, dbPassword, dbEndpoint, dbName)

	// 2. Open the connection pool
	db, err := sql.Open("pgx", connString)
	require.NoError(t, err)

	// 3. Ping to ensure the connection is actually established
	err = db.Ping()
	require.NoError(t, err)

	err = createKafkaTopics(conn)
	if err != nil {
		panic(err)
	}

	brokerAddress = forceIPv4(brokerAddress)
	os.Setenv("KAFKA_BROKER_HOST_local", brokerAddress)
	os.Setenv("KAFKA_BROKER_HOST_docker", brokerAddress)

	dbEndpoint = forceIPv4(dbEndpoint)
	os.Setenv("DB_HOST_local", dbEndpoint)
	os.Setenv("DB_HOST_docker", dbEndpoint)

	redisEndpoint = forceIPv4(redisEndpoint)
	os.Setenv("REDIS_local", redisEndpoint)
	os.Setenv("REDIS_docker", redisEndpoint)
	//fmt.Printf("Redis: %s | Redpanda: %s | DB: %s\n", redisEndpoint, brokerAddress, dbEndpoint)

	os.Setenv("STRIPE_WEBHOOK_SECRET", "test_secret")

	var testEnv TestEnv
	testEnv.redisC = redisC
	testEnv.postgresContainer = postgresContainer
	testEnv.redpandaContainer = redpandaContainer
	testEnv.redisEndpoint = redisEndpoint
	testEnv.dbEndpoint = dbEndpoint
	testEnv.brokerAddress = brokerAddress
	testEnv.db = db

	return testEnv

}

func createKafkaTopics(conn *kafka.Conn) error {
	//  Create the topic cleanly
	err := conn.CreateTopics(kafka.TopicConfig{
		Topic:             "Reservations",
		NumPartitions:     1,
		ReplicationFactor: 1,
	},
		kafka.TopicConfig{
			Topic:             "Reservation-successful",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		kafka.TopicConfig{
			Topic:             "Payment",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		kafka.TopicConfig{
			Topic:             "Payment-Successful",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		kafka.TopicConfig{
			Topic:             "Payment-Failed",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	)
	return err
}

// Safely forces local connections to use IPv4 to avoid Docker Desktop / Windows bugs
func forceIPv4(endpoint string) string {
	host, port, err := net.SplitHostPort(endpoint)
	if err != nil {
		return endpoint // Return as-is if it's not a standard host:port format
	}

	// Catch localhost AND explicit IPv6 loopbacks
	if host == "localhost" || host == "::1" || host == "[::1]" {
		return net.JoinHostPort("127.0.0.1", port)
	}
	return endpoint
}
