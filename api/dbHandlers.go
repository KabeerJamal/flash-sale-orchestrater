package api

import (
	"myproject/db"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func getUsers() gin.HandlerFunc {
	return func(c *gin.Context) {
		res, err := db.GetUsers()
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, res)
	}
}
func getTestUsers() gin.HandlerFunc {
	return func(c *gin.Context) {
		res, err := db.GetTestUsers()
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, res)
	}
}

func getPhones() gin.HandlerFunc {
	return func(c *gin.Context) {
		res, err := db.GetPhones()
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, res)
	}
}

func getPhoneStatus(rdb *redis.Client) gin.HandlerFunc {
	return func(c *gin.Context) {
		phoneUUID := c.Param("phoneUUID")
		res, err := db.GetPhoneStatus(phoneUUID, rdb)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"status": res})
	}
}
