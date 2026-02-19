package main

import (
	"fmt"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/joho/godotenv"
)

func main() { // main function — the entry point
	err := godotenv.Load("../.env")
	if err != nil {
		panic(err)
	}

	user := os.Getenv("POSTGRES_USER")
	pass := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")
	host := os.Getenv("DB_HOST_local") //TODO: hardcoded, need to fix this

	kafkaBrokerAddress := os.Getenv("KAFKA_BROKER_HOST_local")

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable",
		user, pass, host, dbname,
	)

	//applying migration
	m, err := migrate.New("file://../migrations", connStr)

	if err != nil {
		panic(err)
	}
	err = m.Up() // applies all unapplied migrations automatically
	if err != nil && err != migrate.ErrNoChange {
		panic(err)
	}

	go StartProducer(kafkaBrokerAddress, "flashsale-events", "order-id-a", "sale started AAAA")
	go StartProducer(kafkaBrokerAddress, "flashsale-events", "order-id-b", "sale started BBBB")
	go StartProducer(kafkaBrokerAddress, "flashsale-events", "order-id-c", "sale started CCCC")
	go StartProducer(kafkaBrokerAddress, "flashsale-events", "order-id-d", "sale started DDDD")

	//sending request to certain server
	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "Hello World"})
	})

	r.Run() //starts server on localhost:8080
}
