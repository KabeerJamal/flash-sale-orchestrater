package api

import (
	"context"
	"database/sql"
	"myproject/shared"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func loadTestingTearDown(rdb *redis.Client, db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {

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

		_ = luaScript.Run(context.Background(), rdb, []string{}, shared.Reservations, 10).Err()
		db.Exec("DELETE FROM RESERVATIONS")
		c.JSON(200, gin.H{"status": "reset"})
	}
}
