package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"myproject/shared"
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

	// 1. Create reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokerAddress}, //TODO: this is hardcoded, need to fix that
		Topic:   shared.TopicReservation,
		GroupID: shared.TopicReservationGroup,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// consume forever
	for {
		msg, err := r.ReadMessage(ctx) //this is blocking

		if err != nil {
			slog.Error("Error while reading message", "error", err)

			return err
		}

		// print message..
		slog.Info("Received message", "key", string(msg.Key), "value", string(msg.Value))

		var data shared.ReservationEvent

		err = json.Unmarshal(msg.Value, &data)
		if err != nil {
			slog.Error("Error unmarshaling JSON", "error", err)
			return err
		}
		ticketUUID := data.TicketUUID

		//You send message to two places, frontend(via redis) and backend(using redpanda)
		// if reservation in redis is less than 10, do reservation in redis 10 - 1. Send message to kafka for new topic
		//[Idempotency] Previous script was not idempotent
		script := redis.NewScript(`
			local stock = tonumber(redis.call("GET", KEYS[1]))
			if stock == nil then
				return -2
			end

			-- [Idempotency] SET NX ensures only first execution sets this
			if redis.call("GET", KEYS[2]) ~= "PENDING" then
				return -3
			end
			redis.call("SET", KEYS[2], ARGV[1])

			if stock <= 0 then
				return -1
			end

			-- [AllOrNothing] Outbox pattern - store Kafka message atomically before publishing
			redis.call("HSET", KEYS[3], "topic", ARGV[2], "key", ARGV[3], "value", ARGV[4])

			return redis.call("DECR", KEYS[1])
		`) // script is basically -2,-1 or remaining stock after decrement

		//[Idempotency][AllOrNothing]
		res, err := script.Run(ctx, rdb, []string{shared.Reservations, ticketUUID, "outbox:" + ticketUUID},
			shared.SuccessfulReservation, shared.TopicReservationSuccessful,
			string(msg.Key),
			string(msg.Value)).Result()

		if err != nil {
			slog.Error("Some problem with redis database", "error", err)
			return err
		}

		// Redis returns numbers as int64 through go-redis
		//TODO: I think this code will break if we have bad input data
		n := res.(int64)

		if n == -2 {
			//error handling
			slog.Error("Some problem with redis database")
			return err
		} else if n == -1 {
			//TODO:
			/*
				Because Set and RPush are two separate network calls, if your worker crashes exactly after the Set but before the RPush:
				The frontend will see "WAITING_LIST" and keep polling forever.
				The user will never actually be in the waitlist_queue, so they will never get a ticket if one opens up.
				For a practice project: It is fine to leave it as is.
				For enterprise: You would wrap both commands inside a Redis Pipeline or a Lua script to make them execute atomically (all-or-nothing).
			*/
			userUUID := data.UserUUID
			phoneUUID := data.PhoneUUID
			rdb.Set(ctx, ticketUUID, shared.WaitingList, 0).Err()
			memberData := ticketUUID + "|" + userUUID + "|" + phoneUUID
			rdb.RPush(ctx, "waitlist_queue", memberData)
		} else if n == -3 { //[Idempotency]
			slog.Info("Duplicate message, idempotency check blocked", "ticketUUID", ticketUUID)
			continue
		} else {
			// [AllOrNothing] Outbox worker handles Kafka publish
			// Nothing to do here - outbox entry written atomically in Lua script
		}

	}
	return nil
}
