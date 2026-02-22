package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func main() {

	rdb := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer rdb.Close()

	_ = godotenv.Load("../.env")
	// if err != nil {
	// 	panic(err)
	// }

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")

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

		// 3. Print message
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
		if script == -2 {
			//error handling
		} else if script = -1 {
			//polling code implement
		} else {
			//send a message topic reservation successful, with key and value.
			//update redis too
		}


	}
}
