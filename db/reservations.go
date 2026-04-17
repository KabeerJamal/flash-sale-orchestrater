package db

import (
	"context"

	"myproject/shared"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

func GetPhoneStatus(phoneUUID string, rdb *redis.Client) (string, error) {
	status, err := rdb.Get(context.Background(), shared.ProductSoldOut).Result()

	if err == redis.Nil {
		return "AVAILABLE", nil
	}
	if err != nil {
		return "", err
	}

	if status != "1" {
		return "AVAILABLE", nil
	}
	return "SOLD_OUT", nil
}
