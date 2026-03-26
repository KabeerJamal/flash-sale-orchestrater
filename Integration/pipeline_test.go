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
	"math/rand"
	"myproject/shared"
	"strconv"

	"log/slog"
	"myproject/api"
	"net/http"
	"os"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
	"github.com/stripe/stripe-go/v78/webhook"
	"github.com/testcontainers/testcontainers-go"
)

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

	//TODO: if you change anything here, you have to change it in 3 different places. Bad practice.
	luaScript := redis.NewScript(`
    local keys = redis.call('KEYS', '*')
    for _, key in ipairs(keys) do
        if key ~= ARGV[1] then
            redis.call('DEL', key)
        end
    end
    redis.call('SET', ARGV[1], ARGV[2])
    return 1
	`)

	startWorkers(t, ctx)

	users, phones := pollUsersAndPhones()

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
		userUUID := users[rand.Intn(len(users))].UserUUID
		phoneUUID := phones[0].PhoneUUID
		body := fmt.Sprintf(`{"phoneUUID": "%s", "userUUID": "%s"}`, phoneUUID, userUUID)

		//test redis check
		host, err := redisC.Host(ctx)
		require.NoError(t, err)

		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		initialValueRedisReservation, err := rdb.Get(ctx, shared.Reservations).Result()
		require.NoError(t, err)
		initialValueRedisReservationInt, err := strconv.Atoi(initialValueRedisReservation)

		ticketUUID := doReservation(t, body, shared.SuccessfulReservation)

		//----TEST IN REDIS---

		val, err := rdb.Get(ctx, shared.Reservations).Result()
		valInt, err := strconv.Atoi(val)
		require.NoError(t, err)

		require.Equal(t, initialValueRedisReservationInt, valInt+1)

		//--TEST IF INSERTION HAPPENS
		// // 1. Declare variables to hold the data we read from the database
		// var dbPhoneUUID, dbUserUUID, dbStatus string

		// // 2. Query the database using the ticketUUID, and scan the results into our variables
		// err = db.QueryRow(
		// 	"SELECT phoneUUID, userUUID, status FROM RESERVATIONS WHERE ticketID = $1",
		// 	ticketUUID,
		// ).Scan(&dbPhoneUUID, &dbUserUUID, &dbStatus)
		// require.NoError(t, err) // Fails the test if the row doesn't exist or query fails

		// // 3. Verify the database values match the values from our HTTP request
		// require.Equal(t, phoneUUID, dbPhoneUUID)
		// require.Equal(t, userUUID, dbUserUUID)
		// require.Equal(t, "RESERVED", dbStatus)
		require.Eventually(t, func() bool {
			var dbPhoneUUID, dbUserUUID, dbStatus string
			err := db.QueryRow(
				"SELECT phoneUUID, userUUID, status FROM RESERVATIONS WHERE ticketID = $1",
				ticketUUID,
			).Scan(&dbPhoneUUID, &dbUserUUID, &dbStatus)
			if err != nil {
				return false
			}
			return dbPhoneUUID == phoneUUID && dbUserUUID == userUUID && dbStatus == "RESERVED"
		}, 15*time.Second, 500*time.Millisecond, "Expected DB row to exist with correct values")

		t.Cleanup(func() {
			_ = luaScript.Run(context.Background(), rdb, []string{}, shared.Reservations, 10).Err()
			db.Exec("DELETE FROM RESERVATIONS")
			db.Exec("UPDATE CONFIG SET value = 0 WHERE key = 'total_paid'")
		})

	})

	t.Run("successful payment flow", func(t *testing.T) {
		userUUID := users[rand.Intn(len(users))].UserUUID
		phoneUUID := phones[0].PhoneUUID
		body := fmt.Sprintf(`{"phoneUUID": "%s", "userUUID": "%s"}`, phoneUUID, userUUID)

		ticketUUID := doReservation(t, body, shared.SuccessfulReservation)

		//mock ann send webhook,test if response 200
		//mock json payload
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
		}`, shared.StripePaid, ticketUUID, phoneUUID, userUUID)

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
			return statusResponse["status"] == shared.Paid

		}, 15*time.Second, 500*time.Millisecond, "Expected status to become PAID within 15 seconds")
		require.Eventually(t, func() bool {
			val, err := rdb.Get(ctx, shared.TotalPaid).Result()
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

		t.Cleanup(func() {
			_ = luaScript.Run(context.Background(), rdb, []string{}, shared.Reservations, 10).Err()
			db.Exec("DELETE FROM RESERVATIONS")
			db.Exec("UPDATE CONFIG SET value = 0 WHERE key = 'total_paid'")
		})
	})

	t.Run("Unsuccessful payment flow", func(t *testing.T) {

		userUUID := users[rand.Intn(len(users))].UserUUID
		phoneUUID := phones[0].PhoneUUID
		body := fmt.Sprintf(`{"phoneUUID": "%s", "userUUID": "%s"}`, phoneUUID, userUUID)

		ticketUUID := doReservation(t, body, shared.SuccessfulReservation)

		//test redis check
		host, err := redisC.Host(ctx)
		require.NoError(t, err)

		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		initialValueRedisReservation, err := rdb.Get(ctx, shared.Reservations).Result()
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
			return statusResponse["status"] == shared.Failed

		}, 15*time.Second, 500*time.Millisecond, "Expected status to become FAILED within 15 seconds")
		require.Eventually(t, func() bool {
			val, err := rdb.Get(ctx, shared.Reservations).Result()
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

		t.Cleanup(func() {
			_ = luaScript.Run(context.Background(), rdb, []string{}, shared.Reservations, 10).Err()
			db.Exec("DELETE FROM RESERVATIONS")
			db.Exec("UPDATE CONFIG SET value = 0 WHERE key = 'total_paid'")
		})

	})

	t.Run("Unsuccessful payment flow(Expired timer)", func(t *testing.T) {
		//create user id and phone id
		userUUID := users[rand.Intn(len(users))].UserUUID
		phoneUUID := phones[0].PhoneUUID
		body := fmt.Sprintf(`{"phoneUUID": "%s", "userUUID": "%s"}`, phoneUUID, userUUID)
		//do reservation (get ticket uuid)
		ticketUUID := doReservation(t, body, shared.SuccessfulReservation)

		//test redis check
		host, err := redisC.Host(ctx)
		require.NoError(t, err)

		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		initialValueRedisReservation, err := rdb.Get(ctx, shared.Reservations).Result()
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
			return statusResponse["status"] == shared.Failed

		}, 15*time.Second, 500*time.Millisecond, "Expected status to become FAILED within 15 seconds")
		require.Eventually(t, func() bool {
			val, err := rdb.Get(ctx, shared.Reservations).Result()
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

		t.Cleanup(func() {
			_ = luaScript.Run(context.Background(), rdb, []string{}, shared.Reservations, 10).Err()
			db.Exec("DELETE FROM RESERVATIONS")
			db.Exec("UPDATE CONFIG SET value = 0 WHERE key = 'total_paid'")
		})

	})

	//2 more test cases left
	t.Run("Assigned to wait list", func(t *testing.T) {
		userUUID := users[rand.Intn(len(users))].UserUUID
		phoneUUID := phones[0].PhoneUUID
		body := fmt.Sprintf(`{"phoneUUID": "%s", "userUUID": "%s"}`, phoneUUID, userUUID)

		//test redis check
		host, err := redisC.Host(ctx)
		require.NoError(t, err)

		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		_, err = rdb.Set(context.Background(), shared.Reservations, 0, 0).Result()
		oldValue, existed := os.LookupEnv("TOTAL_PRODUCTS")
		os.Setenv("TOTAL_PRODUCTS", "0")
		defer func() {
			if existed {
				os.Setenv("TOTAL_PRODUCTS", oldValue) // restore previous value
			} else {
				os.Unsetenv("TOTAL_PRODUCTS") // remove if it wasn’t set before
			}
		}()
		require.NoError(t, err)

		doReservation(t, body, shared.WaitingList)

		val, err := rdb.Get(ctx, shared.Reservations).Result()
		require.Equal(t, "0", val)

		t.Cleanup(func() {
			_ = luaScript.Run(context.Background(), rdb, []string{}, shared.Reservations, 10).Err()
			db.Exec("DELETE FROM RESERVATIONS")
			db.Exec("UPDATE CONFIG SET value = 0 WHERE key = 'total_paid'")
		})

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
		rdb.Del(ctx, shared.WaitListQueue)
		_, err = rdb.Set(context.Background(), shared.Reservations, 1, 0).Result()
		oldValue, existed := os.LookupEnv("TOTAL_PRODUCTS")
		os.Setenv("TOTAL_PRODUCTS", "1")
		defer func() {
			if existed {
				os.Setenv("TOTAL_PRODUCTS", oldValue) // restore previous value
			} else {
				os.Unsetenv("TOTAL_PRODUCTS") // remove if it wasn’t set before
			}
		}()
		require.NoError(t, err)

		//need 2 users
		userUUID1 := users[rand.Intn(len(users))].UserUUID
		phoneUUID1 := phones[0].PhoneUUID
		body1 := fmt.Sprintf(`{"phoneUUID": "%s", "userUUID": "%s"}`, phoneUUID1, userUUID1)

		userUUID2 := users[rand.Intn(len(users))].UserUUID
		phoneUUID2 := phones[0].PhoneUUID
		body2 := fmt.Sprintf(`{"phoneUUID": "%s", "userUUID": "%s"}`, phoneUUID2, userUUID2)

		ticketUUID1 := doReservation(t, body1, shared.SuccessfulReservation)
		ticketUUID2 := doReservation(t, body2, shared.WaitingList)

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
			return statusResponse["status"] == shared.Failed

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
			return statusResponse["status"] == shared.SuccessfulReservation

		}, 15*time.Second, 500*time.Millisecond, "Expected status for user B to become SUCCESSFUL_RESERVATION within 15 seconds")
		require.Eventually(t, func() bool {
			val, err := rdb.Get(ctx, shared.Reservations).Result()
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

		t.Cleanup(func() {
			_ = luaScript.Run(context.Background(), rdb, []string{}, shared.Reservations, 10).Err()
			db.Exec("DELETE FROM RESERVATIONS")
			db.Exec("UPDATE CONFIG SET value = 0 WHERE key = 'total_paid'")
		})
	})

	t.Run("Product Sold out", func(t *testing.T) {
		host, err := redisC.Host(ctx)
		require.NoError(t, err)

		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})

		_, err = rdb.Set(context.Background(), shared.Reservations, 1, 0).Result()
		oldValue, existed := os.LookupEnv("TOTAL_PRODUCTS")
		os.Setenv("TOTAL_PRODUCTS", "1")
		defer func() {
			if existed {
				os.Setenv("TOTAL_PRODUCTS", oldValue) // restore previous value
			} else {
				os.Unsetenv("TOTAL_PRODUCTS") // remove if it wasn’t set before
			}
		}()
		require.NoError(t, err)

		//set reservation in redis to 1.
		userUUID1 := users[rand.Intn(len(users))].UserUUID
		phoneUUID1 := phones[0].PhoneUUID
		body1 := fmt.Sprintf(`{"phoneUUID": "%s", "userUUID": "%s"}`, phoneUUID1, userUUID1)

		userUUID2 := users[rand.Intn(len(users))].UserUUID
		phoneUUID2 := phones[0].PhoneUUID
		body2 := fmt.Sprintf(`{"phoneUUID": "%s", "userUUID": "%s"}`, phoneUUID2, userUUID2)

		//user A makes successful reservation
		//user B gets waiting list.
		ticketUUID1 := doReservation(t, body1, shared.SuccessfulReservation)
		ticketUUID2 := doReservation(t, body2, shared.WaitingList)

		//user A does successful payment
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
		}`, shared.StripePaid, ticketUUID1, phoneUUID1, userUUID1)

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

		//user B polling should get "SOLD_OUT"
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
			return statusResponse["status"] == shared.SoldOut

		}, 15*time.Second, 500*time.Millisecond, "Expected status to become SOLD_OUT within 15 seconds")

		t.Cleanup(func() {
			_ = luaScript.Run(context.Background(), rdb, []string{}, shared.Reservations, 10).Err()
			db.Exec("DELETE FROM RESERVATIONS")
			db.Exec("UPDATE CONFIG SET value = 0 WHERE key = 'total_paid'")
		})

	})

	t.Run("Duplicate Buy Now requests", func(t *testing.T) {

		host, err := redisC.Host(ctx)
		require.NoError(t, err)

		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})

		userUUID := users[rand.Intn(len(users))].UserUUID
		phoneUUID := phones[0].PhoneUUID
		body := fmt.Sprintf(`{"phoneUUID": "%s", "userUUID": "%s"}`, phoneUUID, userUUID)

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
		require.Equal(t, shared.Pending, buyResponse["status"])
		require.NotEmpty(t, buyResponse["ticketUUID"])

		resp2, err := http.Post("http://localhost:8080/buy-request", "application/json", bytes.NewBuffer([]byte(body)))
		require.NoError(t, err)
		defer resp2.Body.Close()
		require.Equal(t, http.StatusConflict, resp2.StatusCode)

		t.Cleanup(func() {
			_ = luaScript.Run(context.Background(), rdb, []string{}, shared.Reservations, 10).Err()
			db.Exec("DELETE FROM RESERVATIONS")
			db.Exec("UPDATE CONFIG SET value = 0 WHERE key = 'total_paid'")
		})

	})

	//same user buying 2 different phones
	// t.Run("Same User buy 2 different phones", func(t *testing.T) {
	// 	host, err := redisC.Host(ctx)
	// 	require.NoError(t, err)

	// 	port, err := redisC.MappedPort(ctx, "6379")
	// 	require.NoError(t, err)
	// 	rdb := redis.NewClient(&redis.Options{
	// 		Addr: host + ":" + port.Port(),
	// 	})

	// 	userUUID := uuid.New().String()
	// 	phoneUUID := uuid.New().String()
	// 	phoneUUID2 := uuid.New().String()
	// 	body := fmt.Sprintf(`{"phoneUUID": "%s", "userUUID": "%s"}`, phoneUUID, userUUID)
	// 	body2 := fmt.Sprintf(`{"phoneUUID": "%s", "userUUID": "%s"}`, phoneUUID2, userUUID)

	// 	resp, err := http.Post("http://localhost:8080/buy-request", "application/json", bytes.NewBuffer([]byte(body)))
	// 	require.NoError(t, err)
	// 	defer resp.Body.Close()
	// 	require.Equal(t, http.StatusOK, resp.StatusCode)

	// 	buyRespBody, err := io.ReadAll(resp.Body)
	// 	require.NoError(t, err)

	// 	var buyResponse map[string]string
	// 	err = json.Unmarshal(buyRespBody, &buyResponse)
	// 	require.NoError(t, err)

	// 	// {"ticketUUID": x, "status":y}
	// 	require.Equal(t, shared.Pending, buyResponse["status"])
	// 	require.NotEmpty(t, buyResponse["ticketUUID"])

	// 	resp2, err := http.Post("http://localhost:8080/buy-request", "application/json", bytes.NewBuffer([]byte(body2)))
	// 	require.NoError(t, err)
	// 	defer resp2.Body.Close()
	// 	require.Equal(t, http.StatusOK, resp2.StatusCode)

	// 	buyRespBody2, err := io.ReadAll(resp2.Body)
	// 	require.NoError(t, err)

	// 	var buyResponse2 map[string]string
	// 	err = json.Unmarshal(buyRespBody2, &buyResponse2)
	// 	require.NoError(t, err)

	// 	// {"ticketUUID": x, "status":y}
	// 	require.Equal(t, shared.Pending, buyResponse2["status"])
	// 	require.NotEmpty(t, buyResponse2["ticketUUID"])

	// 	require.NotEqual(t, buyResponse["ticketUUID"], buyResponse2["ticketUUID"])
	// 	t.Cleanup(func() {
	// 		_ = luaScript.Run(context.Background(), rdb, []string{}, shared.Reservations, 10).Err()
	// 		db.Exec("DELETE FROM RESERVATIONS")
	// 		db.Exec("UPDATE CONFIG SET value = 0 WHERE key = 'total_paid'")
	// 	})
	// })

}
