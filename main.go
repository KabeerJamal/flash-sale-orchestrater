package main

import (
	"context"
	"log"

	"myproject/api"

	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/joho/godotenv"
)

// TODO: manual work that needs to be done, and set up topics for kafka with partitions
// this should be automated
func main() {
	//---FETCHING ENVIORMENT VARIABLES---
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}

	// 2. Run API
	if err := api.RunAPI(context.Background(), "file://migrations"); err != nil {
		log.Fatal(err)
	}
}
