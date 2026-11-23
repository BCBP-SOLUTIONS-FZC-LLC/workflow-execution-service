package handler

import (
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

func RejectNulInRequestStrings() gin.HandlerFunc {
	return func(c *gin.Context) {
		if requestStringsHaveNul(c) {
			writeProblem(c, http.StatusBadRequest, CodeBadRequest, "request has a NUL character", nil)
			c.Abort()
			return
		}
		c.Next()
	}
}

func requestStringsHaveNul(c *gin.Context) bool {
	if strings.ContainsRune(c.Request.URL.Path, 0) {
		return true
	}
	for _, p := range c.Params {
		if strings.ContainsRune(p.Value, 0) {
			return true
		}
	}
	for name, values := range c.Request.URL.Query() {
		if strings.ContainsRune(name, 0) {
			return true
		}
		for _, v := range values {
			if strings.ContainsRune(v, 0) {
				return true
			}
		}
	}
	return false
}
