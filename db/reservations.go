package db

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
)

func GetPhoneStatus(phoneUUID string) (string, error) {
	user := os.Getenv("POSTGRES_USER")
	pass := os.Getenv("POSTGRES_PASSWORD")
	dbname := os.Getenv("POSTGRES_DB")
	host := os.Getenv("DB_HOST_local")

	connStr := fmt.Sprintf(
		"postgres://%s:%s@%s/%s?sslmode=disable",
		user, pass, host, dbname,
	)

	db, err := sql.Open("pgx", connStr)

	if err != nil {
		slog.Error("DB connection not established", "error", err)
		return "", err
	}

	err = db.Ping()
	if err != nil {
		slog.Error("DB Ping failed", "error", err)
		return "", err
	}

	defer db.Close()

	var status string
	err = db.QueryRow("SELECT status FROM RESERVATIONS WHERE phoneUUID = $1", phoneUUID).Scan(&status)
	if err == sql.ErrNoRows {
		return "AVAILABLE", nil
	}
	if err != nil {
		return "", err
	}
	return status, nil

}
