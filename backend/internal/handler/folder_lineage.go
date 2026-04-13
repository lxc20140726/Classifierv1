package handler

import (
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/liqiye/classifier/internal/repository"
)

func (h *FolderHandler) GetLineage(c *gin.Context) {
	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "folder id is required"})
		return
	}

	if h.lineageReader == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "folder lineage reader not configured"})
		return
	}

	resp, err := h.lineageReader.GetFolderLineage(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "folder not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get folder lineage"})
		return
	}

	c.JSON(http.StatusOK, resp)
}
