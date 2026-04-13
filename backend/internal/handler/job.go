package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/service"
)

type WorkflowJobStarter interface {
	StartJob(ctx context.Context, input service.StartWorkflowJobInput) (string, error)
}

type JobHandler struct {
	jobs             repository.JobRepository
	workflowStarter  WorkflowJobStarter
	config           repository.ConfigRepository
	defaultSourceDir string
}

func NewJobHandler(jobRepo repository.JobRepository) *JobHandler {
	return &JobHandler{jobs: jobRepo}
}

func NewJobHandlerWithWorkflow(
	jobRepo repository.JobRepository,
	workflowStarter WorkflowJobStarter,
	config repository.ConfigRepository,
	defaultSourceDir string,
) *JobHandler {
	return &JobHandler{
		jobs:             jobRepo,
		workflowStarter:  workflowStarter,
		config:           config,
		defaultSourceDir: strings.TrimSpace(defaultSourceDir),
	}
}

func (h *JobHandler) List(c *gin.Context) {
	page := 1
	if rawPage := c.Query("page"); rawPage != "" {
		parsedPage, err := strconv.Atoi(rawPage)
		if err != nil || parsedPage <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid page"})
			return
		}
		page = parsedPage
	}

	limit := 20
	if rawLimit := c.Query("limit"); rawLimit != "" {
		parsedLimit, err := strconv.Atoi(rawLimit)
		if err != nil || parsedLimit <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
			return
		}
		limit = parsedLimit
	}

	items, total, err := h.jobs.List(c.Request.Context(), repository.JobListFilter{
		Status: c.Query("status"),
		Page:   page,
		Limit:  limit,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list jobs"})
		return
	}

	resp := make([]gin.H, 0, len(items))
	for _, item := range items {
		resp = append(resp, serializeJob(item))
	}

	c.JSON(http.StatusOK, gin.H{"data": resp, "total": total, "page": page, "limit": limit})
}

func (h *JobHandler) Get(c *gin.Context) {
	job, err := h.jobs.GetByID(c.Request.Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get job"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": serializeJob(job)})
}

func (h *JobHandler) Progress(c *gin.Context) {
	job, err := h.jobs.GetByID(c.Request.Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get job progress"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"job_id":     job.ID,
		"status":     job.Status,
		"done":       job.Done,
		"total":      job.Total,
		"failed":     job.Failed,
		"updated_at": job.UpdatedAt,
	})
}

func (h *JobHandler) StartWorkflow(c *gin.Context) {
	if h.workflowStarter == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "workflow runner not configured"})
		return
	}

	var req struct {
		WorkflowDefID string `json:"workflow_def_id"`
		SourceDir     string `json:"source_dir"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	if req.WorkflowDefID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "workflow_def_id is required"})
		return
	}

	sourceDir := strings.TrimSpace(req.SourceDir)
	if sourceDir == "" {
		sourceDir = h.resolveWorkflowSourceDir(c.Request.Context())
	}

	jobID, err := h.workflowStarter.StartJob(c.Request.Context(), service.StartWorkflowJobInput{
		WorkflowDefID: req.WorkflowDefID,
		SourceDir:     sourceDir,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start workflow job"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"job_id": jobID})
}

func (h *JobHandler) resolveWorkflowSourceDir(ctx context.Context) string {
	if h.config != nil {
		if raw, err := h.config.Get(ctx, "source_dir"); err == nil {
			if trimmed := strings.TrimSpace(raw); trimmed != "" {
				return trimmed
			}
		}
	}

	return strings.TrimSpace(h.defaultSourceDir)
}

func serializeJob(job *repository.Job) gin.H {
	folderIDs := make([]string, 0)
	if job.FolderIDs != "" {
		parsed := make([]string, 0)
		if err := json.Unmarshal([]byte(job.FolderIDs), &parsed); err == nil && parsed != nil {
			folderIDs = parsed
		}
	}

	return gin.H{
		"id":              job.ID,
		"type":            job.Type,
		"workflow_def_id": job.WorkflowDefID,
		"status":          job.Status,
		"folder_ids":      folderIDs,
		"total":           job.Total,
		"done":            job.Done,
		"failed":          job.Failed,
		"error":           job.Error,
		"started_at":      job.StartedAt,
		"finished_at":     job.FinishedAt,
		"created_at":      job.CreatedAt,
		"updated_at":      job.UpdatedAt,
	}
}
