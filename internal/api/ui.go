package api

import (
	_ "embed"
	"net/http"

	"github.com/gin-gonic/gin"
)

//go:embed ui/index.html
var embeddedStatusHTML []byte

func (s *Server) handleUI(c *gin.Context) {
	c.Data(http.StatusOK, "text/html; charset=utf-8", embeddedStatusHTML)
}
