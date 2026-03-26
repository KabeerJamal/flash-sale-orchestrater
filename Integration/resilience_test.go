package test

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"myproject/shared"
	"strconv"

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
	//DLQ - 4 tests (check notion DLQ page) (TODO)
	//Manul commits

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
			err := db.QueryRow(
			var count int
			db.QueryRow("SELECT COUNT(*) FROM RESERVATIONS WHERE ticketID = $1", event.TicketUUID).Scan(&count)
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

		t.Cleanup(func() {
			api.CleanUpFunction(rdb, db)
		})

	})

	// TODO: Potential problem, both DLQ test cases share the same topic. Second test could read
	// first test's message. Fix: use StartOffset: kafka.LastOffset with unique GroupID per test.
	t.Run("DLQ Reservation Worker, json.Unmarhsal fails, Wrong type", func(t *testing.T) {

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

		t.Cleanup(func() {
			api.CleanUpFunction(rdb, db)
		})
	})

	t.Run("DLQ reservation worker, Redis service is down", func(t *testing.T) {
		host, err := redisC.Host(ctx)
		require.NoError(t, err)
		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})
		err = redisC.Stop(ctx, nil)
		require.NoError(t, err)

		defer func() {
			redisC.Start(ctx)
			// reconnect rdb and reset state
			host, _ := redisC.Host(ctx)
			port, _ := redisC.MappedPort(ctx, "6379")
			rdb := redis.NewClient(&redis.Options{
				Addr: host + ":" + port.Port(),
			})
			rdb.Set(ctx, shared.Reservations, 10, 0)
		}()

		var event shared.ReservationEvent

		event.TicketUUID = uuid.New().String()
		event.PhoneUUID = phones[0].PhoneUUID
		event.UserUUID = users[rand.Intn(len(users))].UserUUID

		eventBytes, err := json.Marshal(event)
		if err != nil {
			t.Fatal(err)
		}

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
		w.Close()

		dlqReader := kafka.NewReader(kafka.ReaderConfig{
			Brokers: []string{env.brokerAddress},
			Topic:   shared.TopicDeadLetterQueue,
			GroupID: "test-dlq-group",
		})
		defer dlqReader.Close()

		dlqMsg, err := dlqReader.ReadMessage(ctx)
		require.NoError(t, err)
		require.Equal(t, eventBytes, dlqMsg.Value)

		t.Cleanup(func() {
			_ = luaScript.Run(context.Background(), rdb, []string{}, shared.Reservations, 10).Err()
			db.Exec("DELETE FROM RESERVATIONS")
			db.Exec("UPDATE CONFIG SET value = 0 WHERE key = 'total_paid'")
		})
	})
}
