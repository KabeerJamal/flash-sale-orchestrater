package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"myproject/api"
	"myproject/shared"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"

	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
)

func init() {
	opts := &slog.HandlerOptions{
		AddSource: true,
	}
	logger := slog.New(slog.NewJSONHandler(os.Stdout, opts))
	slog.SetDefault(logger)
}

func TestHandleFailurePipeline(t *testing.T) {
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

	users, phones := pollUsersAndPhones()

	waitForApi(t)

	/*All test cases for reservation worker */
	//Idempotency check
	//Outbox crash in the middle (TODO)
	//DLQ - 4 tests (check notion DLQ page) (1 TODO)
	//Manul commits (TODO)
	//Atomicity

	t.Run("Idempotency Check for reservationWorker", func(t *testing.T) {

		var event shared.ReservationEvent

		event.TicketUUID = uuid.New().String()
		event.PhoneUUID = phones[0].PhoneUUID
		event.UserUUID = users[rand.Intn(len(users))].UserUUID

		eventBytes, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}

		host, err := redisC.Host(ctx)
		require.NoError(t, err)
		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		initialStockStr, _ := rdb.Get(ctx, shared.Reservations).Result()
		initialStock, _ := strconv.Atoi(initialStockStr)
		// Normally the API sets ticketUUID to PENDING before publishing to Kafka.
		// Here we bypass the API and write directly to Kafka, so we must set PENDING manually.
		rdb.Set(ctx, event.TicketUUID, shared.Pending, 0)

		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicReservation,
			//Balancer: &kafka.LeastBytes{},
			Balancer: &kafka.Hash{},
		})

		w.WriteMessages(
			ctx,
			kafka.Message{
				Key:   []byte(event.TicketUUID),
				Value: eventBytes,
			},
		)
		w.WriteMessages(
			ctx,
			kafka.Message{
				Key:   []byte(event.TicketUUID),
				Value: eventBytes,
			},
		)

		require.Eventually(t, func() bool {
			stockStr, _ := rdb.Get(ctx, "reservation").Result()
			stock, _ := strconv.Atoi(stockStr)
			return stock == initialStock-1
		}, 10*time.Second, 500*time.Millisecond)
		require.Eventually(t, func() bool {
			var dbPhoneUUID, dbUserUUID, dbStatus string
			var count int
			db.QueryRow("SELECT COUNT(*) FROM RESERVATIONS WHERE ticketID = $1", event.TicketUUID).Scan(&count)
			err := db.QueryRow(
				"SELECT phoneUUID, userUUID, status FROM RESERVATIONS WHERE ticketID = $1",
				event.TicketUUID,
			).Scan(&dbPhoneUUID, &dbUserUUID, &dbStatus)
			if err != nil {
				return false
			}
			return dbPhoneUUID == event.PhoneUUID && dbUserUUID == event.UserUUID && dbStatus == "RESERVED" && count == 1
		}, 15*time.Second, 500*time.Millisecond, "Expected DB row to exist with correct values")

		t.Cleanup(func() {
			api.CleanUpFunction(rdb, db)
		})

	})

	t.Run("DLQ Reservation Worker, json.Unmarhsal fails: invalid JSON syntax", func(t *testing.T) {

		host, err := redisC.Host(ctx)
		require.NoError(t, err)
		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		initialStockStr, _ := rdb.Get(ctx, shared.Reservations).Result()
		initialStock, _ := strconv.Atoi(initialStockStr)
		/*
			Invalid JSON syntax — {broken json
			Wrong type — field expects bool but gets "hello"
		*/
		//write to Reservation topic. with either invalid json syntax or wrong type
		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicReservation,
			//Balancer: &kafka.LeastBytes{},
			Balancer: &kafka.Hash{},
		})
		defer w.Close()
		w.WriteMessages(
			ctx,
			// Invalid JSON syntax
			kafka.Message{
				Key:   []byte("test-key"),
				Value: []byte(`{broken json`),
			},
			// Wrong type for bool field
			// kafka.Message{
			//     Key:   []byte("test-key"),
			//     Value: []byte(`{"ticketUUID":"123","phoneUUID":"456","userUUID":"789","promoted":"notabool"}`),
			// }
		)

		//check if DLQ read that message
		dlqReader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicDeadLetterQueue,
			GroupID: "test-dlq-group",
		})
		defer dlqReader.Close()

		dlqMsg, err := dlqReader.ReadMessage(ctx)
		require.NoError(t, err)
		require.Equal(t, []byte(`{broken json`), dlqMsg.Value)

		require.Eventually(t, func() bool {
			stockStr, _ := rdb.Get(ctx, "reservation").Result()
			stock, _ := strconv.Atoi(stockStr)
			return stock == initialStock
		}, 10*time.Second, 500*time.Millisecond)

		successReader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicReservationSuccessful,
			GroupID: "test-success-group",
		})
		defer successReader.Close()
		timeoutCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		defer cancel()
		_, err = successReader.ReadMessage(timeoutCtx)
		// We EXPECT a timeout here, because no success message should exist
		require.ErrorIs(t, err, context.DeadlineExceeded, "Should not have published to success topic")

		verifyCommitReader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicReservation,
			GroupID: shared.TopicReservationGroup, // Must match the worker's consumer group!
		})
		defer verifyCommitReader.Close()
		verifyCtx, cancelVerify := context.WithTimeout(ctx, 2*time.Second)
		defer cancelVerify()
		_, err = verifyCommitReader.ReadMessage(verifyCtx)
		// We EXPECT a timeout here. If the worker committed the message after sending it
		// to the DLQ, there will be no uncommitted messages left for this group to read.
		require.ErrorIs(t, err, context.DeadlineExceeded, "Worker failed to commit the original message")

		t.Cleanup(func() {
			api.CleanUpFunction(rdb, db)
		})

	})

	// TODO: Potential problem, both DLQ test cases share the same topic. Second test could read
	// first test's message. Fix: use StartOffset: kafka.LastOffset with unique GroupID per test.
	t.Run("DLQ Reservation Worker, json.Unmarhsal fails: Wrong type", func(t *testing.T) {

		host, err := redisC.Host(ctx)
		require.NoError(t, err)
		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		initialStockStr, _ := rdb.Get(ctx, shared.Reservations).Result()
		initialStock, _ := strconv.Atoi(initialStockStr)
		/*
			Invalid JSON syntax — {broken json
			Wrong type — field expects bool but gets "hello"
		*/
		//write to Reservation topic. with either invalid json syntax or wrong type
		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicReservation,
			//Balancer: &kafka.LeastBytes{},
			Balancer: &kafka.Hash{},
		})
		defer w.Close()
		w.WriteMessages(
			ctx,
			// Invalid JSON syntax
			// kafka.Message{
			// 	Key:   []byte("test-key"),
			// 	Value: []byte(`{broken json`),
			// },
			// Wrong type for bool field
			kafka.Message{
				Key:   []byte("test-key"),
				Value: []byte(`{"ticketUUID":"123","phoneUUID":"456","userUUID":"789","promoted":"notabool"}`),
			},
		)

		//check if DLQ read that message
		dlqReader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicDeadLetterQueue,
			GroupID: "test-dlq-group",
		})
		defer dlqReader.Close()

		dlqMsg, err := dlqReader.ReadMessage(ctx)
		require.NoError(t, err)
		require.Equal(t, []byte(`{"ticketUUID":"123","phoneUUID":"456","userUUID":"789","promoted":"notabool"}`), dlqMsg.Value)

		require.Eventually(t, func() bool {
			stockStr, _ := rdb.Get(ctx, "reservation").Result()
			stock, _ := strconv.Atoi(stockStr)
			return stock == initialStock
		}, 10*time.Second, 500*time.Millisecond)

		t.Cleanup(func() {
			api.CleanUpFunction(rdb, db)
		})
	})

	t.Run("Redis gatekeeper: exactly 10 reservations under concurrent load", func(t *testing.T) {
		host, err := redisC.Host(ctx)
		require.NoError(t, err)
		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		fmt.Printf("Redis Client Address: %s\n", rdb.Options().Addr)

		var wg sync.WaitGroup
		var mu sync.Mutex
		errors := []string{}
		var reserved atomic.Int32
		var waitingList atomic.Int32
		totalUsers := 50
		//use atomic.int32 smth
		for i := 0; i < totalUsers; i++ {
			userUUID := users[i].UserUUID
			phoneUUID := phones[0].PhoneUUID
			body := fmt.Sprintf(`{"phoneUUID": "%s", "userUUID": "%s"}`, phoneUUID, userUUID)

			wg.Add(1)
			go func() {
				defer wg.Done()
				resp, err := http.Post("http://localhost:8080/buy-request", "application/json", bytes.NewBuffer([]byte(body)))
				require.NoError(t, err)
				defer resp.Body.Close()
				require.Equal(t, http.StatusOK, resp.StatusCode)

				buyRespBody, err := io.ReadAll(resp.Body)
				require.NoError(t, err)

				var buyResponse map[string]string
				err = json.Unmarshal(buyRespBody, &buyResponse)
				require.NoError(t, err)

				require.Equal(t, shared.Pending, buyResponse["status"])
				require.NotEmpty(t, buyResponse["ticketUUID"])
				ticketUUID := buyResponse["ticketUUID"]

				timeout := time.After(30 * time.Second)

				for {
					select {
					case <-timeout:
						mu.Lock()
						errors = append(errors, "some error")
						mu.Unlock()
					default:
						resp, err := http.Get(fmt.Sprintf("http://localhost:8080/status/%s", ticketUUID))
						require.NoError(t, err)
						require.Equal(t, http.StatusOK, resp.StatusCode)

						statusRespBody, err := io.ReadAll(resp.Body)
						require.NoError(t, err)
						resp.Body.Close()

						var statusResponse map[string]string
						err = json.Unmarshal(statusRespBody, &statusResponse)
						require.NoError(t, err)

						if statusResponse["status"] == shared.SuccessfulReservation {
							reserved.Add(1)
							return
						}

						if statusResponse["status"] == shared.WaitingList {
							waitingList.Add(1)
							return
						}
						time.Sleep(1 * time.Second)

					}
				}

			}()

		}
		wg.Wait()
		require.Empty(t, errors)
		//conver total products to atomic int 32, and int
		totalProducts, _ := strconv.Atoi(os.Getenv("TOTAL_PRODUCTS"))
		require.Equal(t, int32(totalProducts), reserved.Load())
		require.Equal(t, int32(totalUsers-totalProducts), waitingList.Load())

		//check if db has 10 insertions too
		require.Eventually(t, func() bool {
			var length int
			err := db.QueryRow("SELECT COUNT(*) FROM RESERVATIONS").Scan(&length)
			if err != nil {
				return false
			}
			return length == totalProducts
		}, 15*time.Second, 500*time.Millisecond, "Expected Reservations to be same as total products. No overselling")

		t.Cleanup(func() {
			api.CleanUpFunction(rdb, db)
		})
	})

	/*All test cases for reservation persistence worker, insertion */
	//Atomicity
	//Idempotency
	//DLQ + MANUAL COMMIT + ALL OR NOTHING (3 test cases)

	t.Run("Idempotency Check for reservationPersistence Worker, insertion function", func(t *testing.T) {
		var event shared.ReservationEvent

		event.TicketUUID = uuid.New().String()
		event.PhoneUUID = phones[0].PhoneUUID
		event.UserUUID = users[rand.Intn(len(users))].UserUUID

		eventBytes, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}

		host, err := redisC.Host(ctx)
		require.NoError(t, err)
		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})

		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicReservationSuccessful,
			//Balancer: &kafka.LeastBytes{},
			Balancer: &kafka.Hash{},
		})

		w.WriteMessages(
			ctx,
			kafka.Message{
				Key:   []byte(event.TicketUUID),
				Value: eventBytes,
			},
		)
		w.WriteMessages(
			ctx,
			kafka.Message{
				Key:   []byte(event.TicketUUID),
				Value: eventBytes,
			},
		)

		require.Eventually(t, func() bool {
			status, _ := rdb.Get(ctx, event.TicketUUID).Result()
			return status == shared.SuccessfulReservation
		}, 10*time.Second, 500*time.Millisecond)
		require.Eventually(t, func() bool {
			var dbPhoneUUID, dbUserUUID, dbStatus string
			var count int
			db.QueryRow("SELECT COUNT(*) FROM RESERVATIONS WHERE ticketID = $1", event.TicketUUID).Scan(&count)
			err := db.QueryRow(
				"SELECT phoneUUID, userUUID, status FROM RESERVATIONS WHERE ticketID = $1",
				event.TicketUUID,
			).Scan(&dbPhoneUUID, &dbUserUUID, &dbStatus)
			if err != nil {
				return false
			}
			return dbPhoneUUID == event.PhoneUUID && dbUserUUID == event.UserUUID && dbStatus == "RESERVED" && count == 1
		}, 15*time.Second, 500*time.Millisecond, "Expected DB row to exist with correct values")

		t.Cleanup(func() {
			api.CleanUpFunction(rdb, db)
		})
	})

	t.Run("DLQ reservationPersistenceWorker, insertion function, json.unmarhsal fails: invalid syntax", func(t *testing.T) {

		host, err := redisC.Host(ctx)
		require.NoError(t, err)
		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		initialStockStr, _ := rdb.Get(ctx, shared.Reservations).Result()
		initialStock, _ := strconv.Atoi(initialStockStr)
		/*
			Invalid JSON syntax — {broken json
			Wrong type — field expects bool but gets "hello"
		*/
		//write to Reservation topic. with either invalid json syntax or wrong type
		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicReservationSuccessful,
			//Balancer: &kafka.LeastBytes{},
			Balancer: &kafka.Hash{},
		})
		defer w.Close()
		w.WriteMessages(
			ctx,
			// Invalid JSON syntax
			kafka.Message{
				Key:   []byte("test-key"),
				Value: []byte(`{broken json`),
			},
			// Wrong type for bool field
			// kafka.Message{
			//     Key:   []byte("test-key"),
			//     Value: []byte(`{"ticketUUID":"123","phoneUUID":"456","userUUID":"789","promoted":"notabool"}`),
			// }
		)

		//check if DLQ read that message
		dlqReader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicDeadLetterQueue,
			GroupID: "test-dlq-group",
		})
		defer dlqReader.Close()

		dlqMsg, err := dlqReader.ReadMessage(ctx)
		require.NoError(t, err)
		require.Equal(t, []byte(`{broken json`), dlqMsg.Value)

		require.Eventually(t, func() bool {
			stockStr, _ := rdb.Get(ctx, "reservation").Result()
			stock, _ := strconv.Atoi(stockStr)
			return stock == initialStock
		}, 10*time.Second, 500*time.Millisecond)

		//test if db is empty

		t.Cleanup(func() {
			api.CleanUpFunction(rdb, db)
		})
	})

	t.Run("DLQ reservationPersistenceWorker, insertion function, json.unmarhsal fails: wrong type", func(t *testing.T) {

		host, err := redisC.Host(ctx)
		require.NoError(t, err)
		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})

		/*
			Invalid JSON syntax — {broken json
			Wrong type — field expects bool but gets "hello"
		*/
		//write to Reservation topic. with either invalid json syntax or wrong type
		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicReservationSuccessful,
			//Balancer: &kafka.LeastBytes{},
			Balancer: &kafka.Hash{},
		})
		defer w.Close()
		w.WriteMessages(
			ctx,
			// Invalid JSON syntax
			// kafka.Message{
			// 	Key:   []byte("test-key"),
			// 	Value: []byte(`{broken json`),
			// },
			//Wrong type for bool field
			kafka.Message{
				Key:   []byte("test-key"),
				Value: []byte(`{"ticketUUID":"123","phoneUUID":"456","userUUID":"789","promoted":"notabool"}`),
			},
		)

		//check if DLQ read that message
		dlqReader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicDeadLetterQueue,
			GroupID: "test-dlq-group",
		})
		defer dlqReader.Close()

		dlqMsg, err := dlqReader.ReadMessage(ctx)
		require.NoError(t, err)
		require.Equal(t, []byte(`{"ticketUUID":"123","phoneUUID":"456","userUUID":"789","promoted":"notabool"}`), dlqMsg.Value)

		//test if db is empty

		t.Cleanup(func() {
			api.CleanUpFunction(rdb, db)
		})
	})

	t.Run("Idempotency Check for reservationPersistence Worker, update function", func(t *testing.T) {

		//from the database get total paid value
		var initialTotalPaid int
		err := db.QueryRow("SELECT value FROM CONFIG WHERE key = 'total_paid'").Scan(&initialTotalPaid)

		var event shared.PaymentEvent

		event.TicketUUID = uuid.New().String()
		event.PhoneUUID = phones[0].PhoneUUID
		event.UserUUID = users[rand.Intn(len(users))].UserUUID
		event.PaymentIntentID = "doesnt matter"
		event.Amount = 32
		event.Currency = "pkr"
		event.Status = shared.StripePaid

		_, err = db.Exec(
			"INSERT INTO RESERVATIONS (ticketID, phoneUUID, userUUID, status) VALUES ($1, $2, $3, 'RESERVED')",
			event.TicketUUID, event.PhoneUUID, event.UserUUID,
		)
		require.NoError(t, err)

		eventBytes, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}

		host, err := redisC.Host(ctx)
		require.NoError(t, err)
		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})

		w := kafka.NewWriter(kafka.WriterConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicPaymentSuccessful,
			//Balancer: &kafka.LeastBytes{},
			Balancer: &kafka.Hash{},
		})

		w.WriteMessages(
			ctx,
			kafka.Message{
				Key:   []byte(event.TicketUUID),
				Value: eventBytes,
			},
		)
		w.WriteMessages(
			ctx,
			kafka.Message{
				Key:   []byte(event.TicketUUID),
				Value: eventBytes,
			},
		)

		require.Eventually(t, func() bool {
			var dbPhoneUUID, dbUserUUID, dbStatus string
			var count int
			db.QueryRow("SELECT COUNT(*) FROM RESERVATIONS WHERE ticketID = $1", event.TicketUUID).Scan(&count)
			err := db.QueryRow(
				"SELECT phoneUUID, userUUID, status FROM RESERVATIONS WHERE ticketID = $1",
				event.TicketUUID,
			).Scan(&dbPhoneUUID, &dbUserUUID, &dbStatus)
			if err != nil {
				return false
			}
			return dbPhoneUUID == event.PhoneUUID && dbUserUUID == event.UserUUID && dbStatus == "PAID" && count == 1
		}, 15*time.Second, 500*time.Millisecond, "Expected DB row to exist with correct values")

		require.Eventually(t, func() bool {
			status, _ := rdb.Get(ctx, event.TicketUUID).Result()
			return status == shared.Paid
		}, 10*time.Second, 500*time.Millisecond, "Expected redis to have status paid for the ticketUUID")

		require.Eventually(t, func() bool {
			var totalPaid int
			err = db.QueryRow("SELECT value FROM CONFIG WHERE key = 'total_paid'").Scan(&totalPaid)
			if err != nil {
				return false
			}
			return totalPaid == initialTotalPaid+1
		}, 10*time.Second, 500*time.Millisecond, "Exepct db value in total Paid to be udpated")
		require.Eventually(t, func() bool {
			totalPaidRedis, _ := rdb.Get(ctx, shared.TotalPaid).Result()
			totalPaidInt, _ := strconv.Atoi(totalPaidRedis)
			return totalPaidInt == initialTotalPaid+1
		}, 10*time.Second, 500*time.Millisecond, "Expected redis to have status paid for the ticketUUID")

		t.Cleanup(func() {
			api.CleanUpFunction(rdb, db)
		})

	})

	//TODO:Idempotency and DLQ test  rollback wokrer (TODO)
	//TODO: Test external service down
	//TODO:Test crash halfway(worker)
}
