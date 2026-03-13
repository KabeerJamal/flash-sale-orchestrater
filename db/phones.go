package db

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type Phone struct {
	PhoneUUID string `json:"phoneUUID"`
	PhoneName string `json:"phoneName"`
}

func GetPhones() ([]Phone, error) {
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
		return []Phone{}, err
	}

	err = db.Ping()
	if err != nil {
		slog.Error("DB Ping failed", "error", err)
		return []Phone{}, err
	}

	defer db.Close()

	res, err := db.Query("SELECT * FROM PHONES LIMIT 10;")
	if err != nil {
		slog.Error("DB query error", "error", err)
	}

	var phones []Phone

	for res.Next() {
		var p Phone
		res.Scan(&p.PhoneUUID, &p.PhoneName)
		phones = append(phones, p)
	}

	return phones, nil

}
