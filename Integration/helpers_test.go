package test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	outboxWorker "myproject/Consumers/outboxWorker/worker"
	paymentWorker "myproject/Consumers/paymentWorker/worker"
	reservationPersistenceWorker "myproject/Consumers/reservationPersistenceWorker/worker"
	rollbackWorker "myproject/Consumers/rollbackWorker/worker"
	soldoutWorker "myproject/Consumers/soldoutWorker/worker"
	"myproject/api"
	"myproject/db"
	"myproject/shared"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
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
	//require.Equal(t, shared.Inserting, buyResponse["status"])
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

	// go func() {
	// 	err := reservationWorker.ReservationWorker(ctx)
	// 	if err != nil {
	// 		t.Logf("InsertionWorker stopped: %v", err)
	// 	}
	// }()

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
	go func() {
		err := outboxWorker.OutboxWorker(ctx)
		if err != nil {
			t.Logf("Rollback stopped: %v", err)
		}
	}()

	go func() {
		err := soldoutWorker.SoldOutWorker(ctx)
		if err != nil {
			t.Logf("Rollback stopped: %v", err)
		}
	}()
}

func pollUsersAndPhones() ([]db.User, []db.Phone) {
	for {
		users, phones, err := getUsersAndPhones()
		if err == nil && len(users) > 0 && len(phones) > 0 {
			return users, phones
		}
		time.Sleep(3 * time.Second)
	}
}

func getUsersAndPhones() ([]db.User, []db.Phone, error) {
	usersResp, err := http.Get("http://localhost:8080/users")
	if err != nil {
		return nil, nil, err
	}
	defer usersResp.Body.Close()

	var users []db.User
	if err = json.NewDecoder(usersResp.Body).Decode(&users); err != nil {
		return nil, nil, err
	}

	phonesResp, err := http.Get("http://localhost:8080/phones")
	if err != nil {
		return nil, nil, err
	}
	defer phonesResp.Body.Close()

	var phones []db.Phone
	if err = json.NewDecoder(phonesResp.Body).Decode(&phones); err != nil {
		return nil, nil, err
	}
	return users, phones, nil
}

func setUpTestEnv(t *testing.T, ctx context.Context) TestEnv {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true") //Debug test was not working but now it works. see detials in Notion
	redisC, err := testcontainers.Run(
		ctx, "redis:8-alpine",
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{
				"6379/tcp": []nat.PortBinding{{HostPort: "6379"}},
			}
		}),
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

	os.Setenv("TOTAL_PRODUCTS", "10")

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase(dbName),
		postgres.WithUsername(dbUser),
		postgres.WithPassword(dbPassword),
		postgres.BasicWaitStrategies(),
		testcontainers.WithHostConfigModifier(func(hc *container.HostConfig) {
			hc.PortBindings = nat.PortMap{
				"5432/tcp": []nat.PortBinding{{HostPort: "5432"}},
			}
		}),
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
		Topic:             shared.TopicReservation,
		NumPartitions:     1,
		ReplicationFactor: 1,
	},
		kafka.TopicConfig{
			Topic:             shared.TopicReservationSuccessful,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		kafka.TopicConfig{
			Topic:             shared.TopicPayment,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		kafka.TopicConfig{
			Topic:             shared.TopicPaymentSuccessful,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		kafka.TopicConfig{
			Topic:             shared.TopicPaymentFailure,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		kafka.TopicConfig{
			Topic:             shared.TopicDeadLetterQueue,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		kafka.TopicConfig{
			Topic:             shared.TopicFlashSaleEnded,
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
