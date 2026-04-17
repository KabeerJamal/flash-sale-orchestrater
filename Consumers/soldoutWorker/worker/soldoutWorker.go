package worker

import (
	"context"
	"log/slog"
	"myproject/shared"
	"os"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func SoldOutWorker(ctx context.Context) error {

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")
	redisAddress := os.Getenv("REDIS_docker")

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer rdb.Close()

	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBrokerAddress},
		Topic:          shared.TopicFlashSaleEnded,
		GroupID:        shared.TopicFlashSaleEndedGroup,
		CommitInterval: 0, // disables auto-commit
	})
	defer r.Close()
	//use a redis script. because if pop is successful, and we fail to set ticket UUID to sold out, we lose the ticketUUID as we have already popped
	script := redis.NewScript(`
			local customer = redis.call('LPOP', KEYS[1])
			if not customer then 
				return nil 
			end

			-- Parse into 3 parts
			local ticketUUID, userUUID, phoneUUID = string.match(customer, "([^|]+)|([^|]+)|([^|]+)")

			-- Validate structure (same as your Go check)
			if not ticketUUID or not userUUID or not phoneUUID then
				return {err = "malformed waitlist entry: " .. customer}
			end

			-- Set ticket as SOLD_OUT
			redis.call('SET', ticketUUID, 'SOLD_OUT')

			return customer
		`)

	for {
		_, err := r.FetchMessage(ctx)
		//TODO: this needs to be called only noce, not every time
		rdb.Set(ctx, shared.ProductSoldOut, 1, 0)
		if err != nil {
			slog.Error("Error while reading message", "error", err)
			time.Sleep(3 * time.Second)
		}

		for {
			_, err := script.Run(ctx, rdb, []string{shared.WaitListQueue}).Result()
			if err == redis.Nil {
				time.Sleep(3 * time.Second)
				continue
				//break //The list is empty, done
			}
			//problem with redis, it is probably down.
			if err != nil {
				//Dont dlq for infrastructure issues
				time.Sleep(3 * time.Second)
				continue
			}
		}

		//r.CommitMessages(ctx, msg)
	}

	return nil
}
