package worker

import (
	"context"
	"encoding/json"
	"fmt"
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

	// Create reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBrokerAddress},
		Topic:          shared.TopicReservation,
		GroupID:        shared.TopicReservationGroup,
		CommitInterval: 0, // disables auto-commit
	})
	defer r.Close()

	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokerAddress},
		Balancer: &kafka.Hash{},
		Topic:    shared.TopicDeadLetterQueue,
	})
	defer dlqWriter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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

	//[AllOrNothing]
	waitlistScript := redis.NewScript(`
				redis.call("SET", KEYS[1], ARGV[1])
				redis.call("RPUSH", KEYS[2], ARGV[2])
				return 1
			`)

	// consume forever
	for {
		msg, err := r.FetchMessage(ctx) //this is blocking

		//DLQ is for messages that can't be processed. If Kafka is down, you have no message, you just wait/retry until Kafka comes back.
		if err != nil {
			slog.Error("Error while reading message", "error", err)
			return err
		}

		// print message..
		slog.Info("Received message", "key", string(msg.Key), "value", string(msg.Value))

		var data shared.ReservationEvent

		err = json.Unmarshal(msg.Value, &data)
		//[DLQ][MANUAL COMMITS]
		if err != nil {
			slog.Error("Error unmarshaling JSON", "error", err)
			shared.SendToDLQ(ctx, dlqWriter, r, msg)
			//Dont put return cause then one bad message takes down the whole system
			continue //Send to DLQ + commit. What would retrying a malformed JSON message accomplish?
		}
		ticketUUID := data.TicketUUID

		//[Idempotency][AllOrNothing]
		var res interface{}
		err = shared.RetryExternal(5, func() error {
			res, err = script.Run(ctx, rdb, []string{shared.Reservations, ticketUUID, "outbox:" + ticketUUID},
				shared.Inserting, shared.TopicReservationSuccessful,
				string(msg.Key),
				string(msg.Value)).Result()
			return err
		})
		if err != nil {
			slog.Error("Some problem with redis database", "error", err)
			//[DLQ][MANUAL COMMITS]
			shared.SendToDLQ(ctx, dlqWriter, r, msg)
			continue // outer message loop

		}

		// Redis returns numbers as int64 through go-redis
		//[DLQ][MANUAL COMMITS]
		n, ok := res.(int64)
		if !ok {
			slog.Error("Unexpected return type from Lua script — TODO: send to DLQ", "result", res)
			shared.SendToDLQ(ctx, dlqWriter, r, msg)
			continue // TODO: replace with DLQ publish + commit
		}

		if n == -2 {
			//error handling
			slog.Error("FATAL: Stock key missing in Redis — system misconfiguration, halting worker",
				"key", shared.Reservations)
			return fmt.Errorf("fatal: stock key %s not found in Redis, worker cannot continue", shared.Reservations)
		} else if n == -1 {
			//TODO:
			/*
				Because Set and RPush are two separate network calls, if your worker crashes exactly after the Set but before the RPush:
				The frontend will see "WAITING_LIST" and keep polling forever.
				The user will never actually be in the waitlist_queue, so they will never get a ticket if one opens up.
				For a practice project: It is fine to leave it as is.
				For enterprise: You would wrap both commands inside a Redis Pipeline or a Lua script to make them execute atomically (all-or-nothing).
			*/
			//Bad message here is impossible, you already validated data during json.Unmarshal.
			userUUID := data.UserUUID
			phoneUUID := data.PhoneUUID
			// rdb.Set(ctx, ticketUUID, shared.WaitingList, 0).Err()
			memberData := ticketUUID + "|" + userUUID + "|" + phoneUUID
			// rdb.RPush(ctx, "waitlist_queue", memberData)

			err = shared.RetryExternal(5, func() error {
				_, err = waitlistScript.Run(ctx, rdb, []string{ticketUUID, "waitlist_queue"},
					shared.WaitingList, memberData).Result()
				return err
			})
			if err != nil {
				slog.Error("Waitlist script failed after 5 retries", "error", err)
				//[DLQ][MANUAL COMMITS]
				shared.SendToDLQ(ctx, dlqWriter, r, msg)
				continue
			}
			//[MANUAL COMMITS]
			r.CommitMessages(ctx, msg)
		} else if n == -3 { //[Idempotency]
			slog.Info("Duplicate message, idempotency check blocked", "ticketUUID", ticketUUID)
			//[MANUAL COMMITS]
			r.CommitMessages(ctx, msg)
			continue
		} else {
			// [AllOrNothing] Outbox worker handles Kafka publish
			// Nothing to do here - outbox entry written atomically in Lua script

			//[MANUAL COMMITS]
			r.CommitMessages(ctx, msg)
		}

	}
	return nil
}
