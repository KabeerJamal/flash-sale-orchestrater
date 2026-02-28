package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"

	reservationWorker "myproject/Consumers/reservationWorker/worker"
	"myproject/api"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestIntegrationPipeline(t *testing.T) {
	os.Setenv("TESTCONTAINERS_RYUK_DISABLED", "true") //Debug test was not working but now it works. see detials in Notion
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	redisC, err := testcontainers.Run(
		ctx, "redis:8-alpine",
		testcontainers.WithExposedPorts("6379/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForListeningPort("6379/tcp"),
			wait.ForLog("Ready to accept connections"),
		),
	)
	testcontainers.CleanupContainer(t, redisC)
	require.NoError(t, err)

	//container port defined internally, 9092 → Kafka API (container port)
	redpandaContainer, err := redpanda.Run(ctx,
		"docker.redpanda.com/redpandadata/redpanda:v23.3.3",
		redpanda.WithAutoCreateTopics(),
	)
	defer func() {
		if err := testcontainers.TerminateContainer(redpandaContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		log.Printf("failed to start container: %s", err)
		return
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
	defer func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			log.Printf("failed to terminate container: %s", err)
		}
	}()
	if err != nil {
		log.Printf("failed to start container: %s", err)
		return
	}

	redisEndpoint, err := redisC.Endpoint(ctx, "")
	require.NoError(t, err)

	// brokers, err := redpandaContainer.Endpoint(ctx, "") //returns a []string, just do brokers[0]
	// require.NoError(t, err)

	dbEndpoint, err := postgresContainer.Endpoint(ctx, "")
	require.NoError(t, err)

	// Dial the broker from your Go code (Host machine)
	brokerAddress, err := redpandaContainer.KafkaSeedBroker(ctx)
	conn, err := kafka.Dial("tcp", brokerAddress)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	//  Create the topic cleanly
	err = conn.CreateTopics(kafka.TopicConfig{
		Topic:             "Reservations",
		NumPartitions:     1,
		ReplicationFactor: 1,
	},
		kafka.TopicConfig{
			Topic:             "Reservation-successful",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	)
	if err != nil {
		panic(err)
	}

	//brokers = strings.ReplaceAll(brokers, "localhost", "127.0.0.1")
	// os.Setenv("KAFKA_BROKER_HOST_local", brokers)
	// os.Setenv("KAFKA_BROKER_HOST_docker", brokers)

	// os.Setenv("KAFKA_BROKER_HOST_local", brokerAddress)
	// os.Setenv("KAFKA_BROKER_HOST_docker", brokerAddress)

	// // dbEndpoint = strings.ReplaceAll(dbEndpoint, "localhost", "127.0.0.1")
	// os.Setenv("DB_HOST_local", dbEndpoint)
	// os.Setenv("DB_HOST_docker", dbEndpoint)

	// redisEndpoint = strings.ReplaceAll(redisEndpoint, "localhost", "127.0.0.1") // fixed dial tcp: lookup localhost: i/o timeout error
	// os.Setenv("REDIS_local", redisEndpoint)
	// os.Setenv("REDIS_docker", redisEndpoint)
	// fmt.Printf("Redis: %s | Redpanda: %s | DB: %s\n", redisEndpoint, brokerAddress, dbEndpoint)

	brokerAddress = forceIPv4(brokerAddress)
	os.Setenv("KAFKA_BROKER_HOST_local", brokerAddress)
	os.Setenv("KAFKA_BROKER_HOST_docker", brokerAddress)

	dbEndpoint = forceIPv4(dbEndpoint)
	os.Setenv("DB_HOST_local", dbEndpoint)
	os.Setenv("DB_HOST_docker", dbEndpoint)

	redisEndpoint = forceIPv4(redisEndpoint)
	os.Setenv("REDIS_local", redisEndpoint)
	os.Setenv("REDIS_docker", redisEndpoint)
	fmt.Printf("Redis: %s | Redpanda: %s | DB: %s\n", redisEndpoint, brokerAddress, dbEndpoint)

	go func() {
		//print the current working directory here
		// wd, err := os.Getwd()
		// if err != nil {
		// 	fmt.Println("Error getting working directory:", err)
		// } else {
		// 	fmt.Println("Current working directory:", wd)
		// }
		err = api.RunAPI(ctx, "file://../migrations")
		// We can't use require.NoError inside a goroutine easily, so we just log it
		if err != nil {
			t.Logf("API stopped: %v", err)
		}
	}()

	// go func() {
	// 	err = insertionworker.InsertionWorker(ctx)
	// 	if err != nil {
	// 		t.Logf("InsertionWorker stopped: %v", err)
	// 	}
	// }()

	go func() {
		err = reservationWorker.ReservationWorker(ctx)
		if err != nil {
			t.Logf("InsertionWorker stopped: %v", err)
		}
	}()

	// Give the API time to boot
	time.Sleep(4 * time.Second)

	//Run possible test cases
	//Running this test case first, matters
	t.Run("Fake ticker ID status check", func(t *testing.T) {
		resp, err := http.Get("http://localhost:8080/status/2345")
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusNotFound, resp.StatusCode)
	})

	//post request followed by get request
	t.Run("POST request and GET status request", func(t *testing.T) {
		body := `{"phoneUUID": "3f9c2b7e-8d41-4f6a-9a2e-5c1b7d8e4a90" , "userUUID": "b6a1e3d4-2c7f-4b98-8e15-9d3f6a2c7e41"}`
		resp, err := http.Post("http://localhost:8080/buy-request", "application/json", bytes.NewBuffer([]byte(body)))
		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)

		bodyBytes, err := io.ReadAll(resp.Body)
		require.NoError(t, err)

		var result map[string]string
		err = json.Unmarshal(bodyBytes, &result)
		require.NoError(t, err)

		// {"ticketUUID": x, "status":y}
		require.Equal(t, "PENDING", result["status"])
		require.NotEmpty(t, result["ticketUUID"])

		/*
			Because your workers are running in the background via Kafka,
			if you instantly call GET /status, it might still say "PENDING".
			You have to write a small polling loop (or a time.Sleep for now)
			to give the worker a few milliseconds to process the Kafka message.
		*/
		time.Sleep(12 * time.Second)

		resp, err = http.Get(fmt.Sprintf("http://localhost:8080/status/%s", result["ticketUUID"]))

		var result2 map[string]string
		bodyBytes2, err := io.ReadAll(resp.Body)
		require.NoError(t, err)
		err = json.Unmarshal(bodyBytes2, &result2)
		require.NoError(t, err)

		require.NoError(t, err)
		defer resp.Body.Close()
		require.Equal(t, http.StatusOK, resp.StatusCode)
		require.Equal(t, "SUCCESSFUL_RESERVATION", result2["status"])

		//----TEST IN REDIS---
		host, err := redisC.Host(ctx)
		require.NoError(t, err)

		port, err := redisC.MappedPort(ctx, "6379")
		require.NoError(t, err)
		rdb := redis.NewClient(&redis.Options{
			Addr: host + ":" + port.Port(),
		})

		val, err := rdb.Get(ctx, "reservation").Result()
		require.NoError(t, err)

		require.Equal(t, "9", val)

	})

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
