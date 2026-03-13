package db

import (
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
)

type User struct {
	UserUUID string `json:"userUUID"`
	UserName string `json:"userName"`
}

func GetUsers() ([]User, error) {
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
		return []User{}, err
	}

	err = db.Ping()
	if err != nil {
		slog.Error("DB Ping failed", "error", err)
		return []User{}, err
	}

	defer db.Close()

	res, err := db.Query("SELECT * FROM USERS LIMIT 10;")
	if err != nil {
		slog.Error("DB query error", "error", err)
	}

	var users []User

	for res.Next() {
		var p User
		res.Scan(&p.UserUUID, &p.UserName)
		users = append(users, p)
	}

	return users, nil

}
