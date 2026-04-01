package worker

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"myproject/shared"
	"os"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/segmentio/kafka-go"
)

func RollbackWorker(ctx context.Context) error {
	dbUser := os.Getenv("POSTGRES_USER")
	dbPassword := os.Getenv("POSTGRES_PASSWORD")
	dbName := os.Getenv("POSTGRES_DB")
	dbAddress := os.Getenv("DB_HOST_docker")
	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", dbUser, dbPassword, dbAddress, dbName)

	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return err
	}
	defer db.Close()

	redisAddress := os.Getenv("REDIS_docker")
	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddress,
		Password: "",
		DB:       0,
	})
	defer rdb.Close()

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_docker")

	// 1. Create reader config
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBrokerAddress},
		Topic:   shared.TopicPaymentFailure,
		GroupID: shared.TopicPaymentFailureGroup,
	})
	defer r.Close()

	w := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBrokerAddress},
		Topic:   shared.TopicReservationSuccessful,
		//Balancer: &kafka.LeastBytes{},
		Balancer: &kafka.Hash{},
	})
	defer w.Close()

	dlqWriter := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  []string{kafkaBrokerAddress},
		Balancer: &kafka.Hash{},
		Topic:    shared.TopicDeadLetterQueue,
	})
	defer dlqWriter.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	script := redis.NewScript(
		`local customer = redis.call('LPOP', KEYS[1])
		-- Mark the original (current) user's ticket as failed atomically!
		redis.call('SET', KEYS[3], 'FAILED') 

		if customer then
			redis.call('SET', ARGV[1] .. KEYS[3], customer) 
			return customer
		else
			redis.call('INCR', KEYS[2]) 
			return nil
		end`)

	// Consume forever
	for {
		msg, err := r.FetchMessage(ctx) //this is blocking
		if err != nil {
			slog.Error("Error while reading message", "error", err)
			time.Sleep(3 * time.Second)
			continue
		}

		// Print message
		//slog.Info("Received message", "key", string(msg.Key), "value", string(msg.Value))

		// Unmarshal the message
		var pm shared.PaymentEvent
		err = json.Unmarshal(msg.Value, &pm)
		if err != nil {
			slog.Error("Error unmarshaling JSON", "error", err)
			shared.SendToDLQ(ctx, dlqWriter, r, msg)
			//Dont put return cause then one bad message takes down the whole system
			continue //Send to DLQ + commit. What would retrying a malformed JSON message accomplish?
		}

		for {
			res, err := db.Exec("DELETE FROM RESERVATIONS WHERE ticketID = $1", pm.TicketUUID)
			if err != nil {
				time.Sleep(2 * time.Second)
				continue
			}

			rowsAffected, err := res.RowsAffected()
			if err != nil {
				time.Sleep(2 * time.Second)
				continue
			}

			if rowsAffected > 0 {
				customerStr := executeLuaPromoScript(ctx, rdb, script, pm.TicketUUID)
				if customerStr != "" {
					err = processPromotedCustomer(ctx, rdb, w, pm.TicketUUID, customerStr)
					if err != nil {
						shared.SendToDLQ(ctx, dlqWriter, r, msg)
						break
					}
				}
			} else {
				// Crash Recovery Path
				var currentStatus string
				for {
					currentStatus, err = rdb.Get(ctx, pm.TicketUUID).Result()
					if err == nil || err == redis.Nil {
						break
					}
					time.Sleep(2 * time.Second)
				}
				if currentStatus != "FAILED" {
					// SCENARIO 1: Lua never ran
					customerStr := executeLuaPromoScript(ctx, rdb, script, pm.TicketUUID)
					if customerStr != "" {
						err = processPromotedCustomer(ctx, rdb, w, pm.TicketUUID, customerStr)
						if err != nil {
							shared.SendToDLQ(ctx, dlqWriter, r, msg)
							break
						}
					}
				} else {
					// SCENARIO 2 & 3: Lua ran, check outbox!
					var outboxCustomer string
					for {
						outboxCustomer, err = rdb.Get(ctx, "pending_promo:"+pm.TicketUUID).Result()
						if err == nil || err == redis.Nil {
							break
						}
						time.Sleep(2 * time.Second)
					}
					if outboxCustomer != "" {
						// SCENARIO 2: Outbox exists, just resend to Kafka!
						err = processPromotedCustomer(ctx, rdb, w, pm.TicketUUID, outboxCustomer)
						if err != nil {
							shared.SendToDLQ(ctx, dlqWriter, r, msg)
							break
						}
					}
				}

			}
			break
		}
		r.CommitMessages(ctx, msg)

	}
}

// 1. Helper to run the Lua script infinitely
func executeLuaPromoScript(ctx context.Context, rdb *redis.Client, script *redis.Script, failedTicketUUID string) string {
	var result interface{}
	var err error
	for {
		result, err = script.Run(ctx, rdb, []string{shared.WaitListQueue, shared.Reservations, failedTicketUUID}, shared.PromotionPending).Result()
		if err == nil || err == redis.Nil {
			break
		}
		slog.Error("Redis Lua script failed, retrying...", "error", err)
		time.Sleep(2 * time.Second)
	}

	if result != nil {
		return result.(string)
	}
	return ""
}

// 2. Helper to write to Kafka and delete the outbox infinitely
func processPromotedCustomer(ctx context.Context, rdb *redis.Client, w *kafka.Writer, failedTicketUUID string, customerData string) error {
	parts := strings.Split(customerData, "|")
	if len(parts) != 3 {
		slog.Error("malformed waitlist entry", "entry", customerData)
		return fmt.Errorf("malformed waitlist entry: %s", customerData)
	}

	payload := shared.ReservationEvent{
		TicketUUID: parts[0],
		UserUUID:   parts[1],
		PhoneUUID:  parts[2],
		Promoted:   true,
	}
	b, err := json.Marshal(payload)
	if err != nil {
		slog.Error("failed to marshal payload", "error", err)
		return err // ← Return error
	}

	// Infinite loop for Kafka
	for {
		err := w.WriteMessages(ctx, kafka.Message{
			Key:   []byte(payload.TicketUUID),
			Value: b,
		})
		if err == nil {
			break
		}
		slog.Error("Failed to write Kafka promotion, retrying...", "error", err)
		time.Sleep(2 * time.Second)
	}

	// Infinite loop to clean the outbox
	for {
		err := rdb.Del(ctx, "pending_promo:"+failedTicketUUID).Err()
		if err == nil {
			break
		}
		slog.Error("Failed to delete pending user outbox, retrying...", "error", err)
		time.Sleep(2 * time.Second)
	}
	return nil
}
