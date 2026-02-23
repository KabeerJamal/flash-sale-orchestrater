package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/redis/go-redis/v9"
)

func main() {

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	defer rdb.Close()

	// prefill it in redis
	_, err := rdb.Set(context.Background(), "reservation", 10, 0).Result()
	if err != nil {
		log.Fatalf("redis error: %v", err)
	}

	//---FETCHING ENVIORMENT VARIABLES---
	err = godotenv.Load("../.env")
	if err != nil {
		panic(err)
	}

	user := os.Getenv("POSTGRES_USER")
	pass := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")
	host := os.Getenv("DB_HOST_local")

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_local")

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable",
		user, pass, host, dbname,
	)

	//---APPLYING MIGRATION---
	m, err := migrate.New("file://../migrations", connStr)

	if err != nil {
		panic(err)
	}
	err = m.Up() // applies all unapplied migrations automatically
	if err != nil && err != migrate.ErrNoChange {
		panic(err)
	}

	//browser sends a request with request body data
	r := gin.Default()

	r.POST("/buy-request", func(c *gin.Context) {
		var body map[string]string
		c.BindJSON(&body)

		//generate a ticketUUID and pass it in body
		ticketUUID := uuid.New().String()
		body["ticketUUID"] = ticketUUID

		go StartProducer(kafkaBrokerAddress, "Reservations", body)

		//put ticketUUID and status in redis
		rdb.Set(context.Background(), ticketUUID, "PENDING", 0).Err()

		c.JSON(200, gin.H{"ticketUUID": ticketUUID, "status": "PENDING"}) //Immediate response with ticket ID and status as pending
	})

	r.GET("/status/:ticketId", func(c *gin.Context) {
		//get ticket uuid from param
		ticketUUID := c.Param("ticketId")

		//respond with whats in redis
		resp, err := rdb.Get(context.Background(), ticketUUID).Result()

		if err == redis.Nil {
			// Key not found — expected case
			log.Printf("ticket not found: %s", ticketUUID)
			c.JSON(http.StatusNotFound, gin.H{
				"error": "ticket not found",
			})
			return // or return custom error
		}
		if err != nil {
			log.Printf("redis GET failed for ticket %s: %v", ticketUUID, err)
			return
		}
		c.JSON(200, gin.H{"ticketUUID": ticketUUID, "status": resp})

		//its frontend duty to keep in polling until it recieves a certain response
	})

	r.Run() //starts server on localhost:8080
}
