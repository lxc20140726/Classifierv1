package handler

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/liqiye/classifier/internal/repository"
)

type AuditHandler struct {
	audit repository.AuditRepository
}

func NewAuditHandler(auditRepo repository.AuditRepository) *AuditHandler {
	return &AuditHandler{audit: auditRepo}
}

func (h *AuditHandler) List(c *gin.Context) {
	page := 1
	if rawPage := c.Query("page"); rawPage != "" {
		parsedPage, err := strconv.Atoi(rawPage)
		if err != nil || parsedPage <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid page"})
			return
		}
		page = parsedPage
	}

	limit := 50
	if rawLimit := c.Query("limit"); rawLimit != "" {
		parsedLimit, err := strconv.Atoi(rawLimit)
		if err != nil || parsedLimit <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
			return
		}
		limit = parsedLimit
	}

	from, err := parseAuditTimeQueryValue(c.Query("from"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid from"})
		return
	}

	to, err := parseAuditTimeQueryValue(c.Query("to"))
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid to"})
		return
	}

	if !from.IsZero() && !to.IsZero() && from.After(to) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid time range"})
		return
	}

	folderPathKeyword := strings.TrimSpace(c.Query("folder_path"))
	if folderPathKeyword == "" {
		folderPathKeyword = strings.TrimSpace(c.Query("folder_path_keyword"))
	}

	items, total, err := h.audit.List(c.Request.Context(), repository.AuditListFilter{
		JobID:             c.Query("job_id"),
		WorkflowRunID:     c.Query("workflow_run_id"),
		NodeRunID:         c.Query("node_run_id"),
		NodeID:            c.Query("node_id"),
		NodeType:          c.Query("node_type"),
		Action:            c.Query("action"),
		Result:            c.Query("result"),
		FolderID:          c.Query("folder_id"),
		FolderPathKeyword: folderPathKeyword,
		From:              from,
		To:                to,
		Page:              page,
		Limit:             limit,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list audit logs"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": items, "total": total, "page": page, "limit": limit})
}

func parseAuditTimeQueryValue(raw string) (time.Time, error) {
	value := strings.TrimSpace(raw)
	if value == "" {
		return time.Time{}, nil
	}

	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}

	for _, layout := range layouts {
		parsed, err := time.Parse(layout, value)
		if err == nil {
			return parsed.UTC(), nil
		}
	}

	return time.Time{}, fmt.Errorf("invalid time format")
}
