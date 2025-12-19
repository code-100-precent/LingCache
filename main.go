package main

import (
	"github.com/code-100-precent/LingCache/structure"
	"github.com/gin-gonic/gin"
)

var Storage = make(map[string]interface{})

func main() {
	server := gin.New()

	server.GET("/:type/:key/:value", func(context *gin.Context) {
		paramType := context.Param("type")
		paramKey := context.Param("key")
		paramValue := context.Param("value")
		switch paramType {
		case "string":
			Storage[paramKey] = structure.NewSDS(paramValue)
		}
		context.JSON(200, (Storage[paramKey].(structure.SDS)))
	})

	server.Run(":8080")
}
