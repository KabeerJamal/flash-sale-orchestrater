package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func ReservationWorker(ctx context.Context) error {

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")
	redisAddress := os.Getenv("REDIS_docker")

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer rdb.Close()

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBrokerAddress},
		Topic:   "Reservation-successful",
		//Balancer: &kafka.LeastBytes{},
		Balancer: &kafka.Hash{},
	})

	// 1. Create reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokerAddress}, //TODO: this is hardcoded, need to fix that
		Topic:   "Reservations",
		GroupID: "Reservation-group",
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 2. Consume forever
	for {
		msg, err := r.ReadMessage(ctx) //this is blocking
		if err != nil {
			log.Println("Error while reading:", err)
			break
		}

		// 3. Print message..
		fmt.Printf("Received message: key=%s value=%s\n", string(msg.Key), string(msg.Value))

		//You send message to two places, frontend(via redis) and backend(using redpanda)
		// if reservation in redis is less than 10, do reservation in redis 10 - 1. Send message to kafka for new topic
		script := redis.NewScript(`
			local stock = tonumber(redis.call("GET", KEYS[1]))
			if stock == nil then
				return -2
			end

			if stock <= 0 then
				return -1
			end

			return redis.call("DECR", KEYS[1])
		`) // script is basically -2,-1 or remaining stock after decrement

		//TODO
		res, err := script.Run(ctx, rdb, []string{"reservation"}).Result()
		if err != nil {
			log.Printf("Some problem with redis database: %v", err)
			return err
		}

		// Redis returns numbers as int64 through go-redis
		n := res.(int64)
		var data map[string]string

		err = json.Unmarshal(msg.Value, &data)
		if err != nil {
			log.Println(err)
			return err
		}

		ticketUUID := data["ticketUUID"]
		if n == -2 {
			//error handling
			log.Printf("Some problem with redis database")
			return err
		} else if n == -1 {
			//polling code implement
			rdb.Set(ctx, ticketUUID, "WAITING_LIST", 0).Err()
		} else {
			//TODO: send a message topic reservation successful, with key and value.
			err = w.WriteMessages(
				context.Background(),

				kafka.Message{
					Key:   msg.Key,
					Value: msg.Value,
				},
			)
			if err != nil {
				log.Printf("failed to write message: %v", err)
			}

			//update redis too
			rdb.Set(ctx, ticketUUID, "SUCCESSFUL_RESERVATION", 0).Err()
		}

	}
	return nil
}
