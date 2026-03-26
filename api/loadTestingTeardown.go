package api

import (
	"context"
	"database/sql"
	"myproject/shared"
	"os"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func CleanUpFunction(rdb *redis.Client, db *sql.DB) {
	luaScript := redis.NewScript(`
    local keys = redis.call('KEYS', '*')
    for _, key in ipairs(keys) do
        if key ~= ARGV[1] then
            redis.call('DEL', key)
        end
    end
    redis.call('SET', ARGV[1], ARGV[2])
    return 1
	`)

	_ = luaScript.Run(context.Background(), rdb, []string{}, shared.Reservations, os.Getenv("TOTAL_PRODUCTS")).Err()
	db.Exec("DELETE FROM RESERVATIONS")
	db.Exec("UPDATE CONFIG SET value = 0 WHERE key = 'total_paid'")
}

func loadTestingTearDown(rdb *redis.Client, db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		CleanUpFunction(rdb, db)
	}
}
