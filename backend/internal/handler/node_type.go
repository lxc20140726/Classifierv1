package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/liqiye/classifier/internal/service"
)

type NodeTypeReader interface {
	ListNodeSchemas() []service.NodeSchema
}

type NodeTypeHandler struct {
	runner NodeTypeReader
}

func NewNodeTypeHandler(runner NodeTypeReader) *NodeTypeHandler {
	return &NodeTypeHandler{runner: runner}
}

func (h *NodeTypeHandler) List(c *gin.Context) {
	if h.runner == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "workflow runner not configured"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": h.runner.ListNodeSchemas()})
}
