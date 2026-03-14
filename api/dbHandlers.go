package api

import (
	"myproject/db"

	"github.com/gin-gonic/gin"
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

func getPhoneStatus() gin.HandlerFunc {
	return func(c *gin.Context) {
		phoneUUID := c.Param("phoneUUID")
		res, err := db.GetPhoneStatus(phoneUUID)
		if err != nil {
			c.JSON(500, gin.H{"error": err.Error()})
			return
		}
		c.JSON(200, gin.H{"status": res})
	}
}
