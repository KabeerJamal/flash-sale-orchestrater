package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
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
	ok, err := rdb.SetNX(context.Background(), "reservation", 10, 0).Result()
	if err != nil {
		log.Fatalf("redis error: %v", err)
	}

	if !ok {
		log.Println("key already exists")
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
	// r.GET("/", func(c *gin.Context) {
	// 	c.JSON(200, gin.H{"message": "Hello World"})
	// })
	r.POST("/buy-request", func(c *gin.Context) {
		var body map[string]string
		c.BindJSON(&body)

		//generate a ticketUUID and pass it in body
		go StartProducer(kafkaBrokerAddress, "Reservations", body)
		//put ticketUUID and status in redis
		c.JSON(200, gin.H{"status": "ok"}) //Immediate response with ticket ID and status as pending
	})

	r.GET("/status/:ticketId", func(c *gin.Context) {
		//respon with whats in redis
		//its frontend duty to keep in polling until it recieves a certain response
	})

	r.Run() //starts server on localhost:8080
}
