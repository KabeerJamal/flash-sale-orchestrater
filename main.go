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
	err := godotenv.Load("env")
	if err != nil {
		panic(err)
	}

	user := os.Getenv("POSTGRES_USER")
	pass := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")
	host := "localhost"

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s:5432/%s?sslmode=disable",
		user, pass, host, dbname,
	)

	//applying migration
	m, err := migrate.New("file://./migrations", connStr)

	if err != nil {
		fmt.Print("yo")
		panic(err)
	}
	err = m.Up() // applies all unapplied migrations automatically
	if err != nil && err != migrate.ErrNoChange {
		fmt.Print("yo2")
		panic(err)
	}

	StartProducer()

	//sending request to certain server
	r := gin.Default()
	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "Hello World"})
	})

	r.Run() //starts server on localhost:8080
}
