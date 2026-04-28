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

type WorkflowJobCanceller interface {
	CancelJob(ctx context.Context, jobID string) error
}

type JobHandler struct {
	jobs              repository.JobRepository
	folders           repository.FolderRepository
	workflowStarter   WorkflowJobStarter
	workflowCanceller WorkflowJobCanceller
	config            repository.ConfigRepository
	defaultSourceDir  string
}

func NewJobHandler(jobRepo repository.JobRepository) *JobHandler {
	return &JobHandler{jobs: jobRepo}
}

func NewJobHandlerWithWorkflow(
	jobRepo repository.JobRepository,
	folderRepo repository.FolderRepository,
	workflowStarter WorkflowJobStarter,
	config repository.ConfigRepository,
	defaultSourceDir string,
) *JobHandler {
	return &JobHandler{
		jobs:              jobRepo,
		folders:           folderRepo,
		workflowStarter:   workflowStarter,
		workflowCanceller: workflowCancellerFromStarter(workflowStarter),
		config:            config,
		defaultSourceDir:  strings.TrimSpace(defaultSourceDir),
	}
}

func workflowCancellerFromStarter(starter WorkflowJobStarter) WorkflowJobCanceller {
	canceller, ok := starter.(WorkflowJobCanceller)
	if !ok {
		return nil
	}
	return canceller
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
		resp = append(resp, h.serializeJob(c.Request.Context(), item))
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

	c.JSON(http.StatusOK, gin.H{"data": h.serializeJob(c.Request.Context(), job)})
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
		"job_id":      job.ID,
		"status":      job.Status,
		"done":        job.Done,
		"total":       job.Total,
		"failed":      job.Failed,
		"started_at":  job.StartedAt,
		"finished_at": job.FinishedAt,
		"updated_at":  job.UpdatedAt,
	})
}

func (h *JobHandler) StartWorkflow(c *gin.Context) {
	if h.workflowStarter == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "workflow runner not configured"})
		return
	}

	var req struct {
		WorkflowDefID string   `json:"workflow_def_id"`
		SourceDir     string   `json:"source_dir"`
		FolderIDs     []string `json:"folder_ids"`
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
		FolderIDs:     req.FolderIDs,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start workflow job"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"job_id": jobID})
}

func (h *JobHandler) Cancel(c *gin.Context) {
	if h.workflowCanceller == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "workflow canceller not configured"})
		return
	}

	if err := h.workflowCanceller.CancelJob(c.Request.Context(), c.Param("id")); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to cancel job"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"cancelled": true})
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

func (h *JobHandler) serializeJob(ctx context.Context, job *repository.Job) gin.H {
	folderIDs := make([]string, 0)
	if job.FolderIDs != "" {
		parsed := make([]string, 0)
		if err := json.Unmarshal([]byte(job.FolderIDs), &parsed); err == nil && parsed != nil {
			folderIDs = parsed
		}
	}
	folderTargets := h.resolveFolderTargets(ctx, folderIDs)

	return gin.H{
		"id":              job.ID,
		"type":            job.Type,
		"workflow_def_id": job.WorkflowDefID,
		"status":          job.Status,
		"folder_ids":      folderIDs,
		"folder_targets":  folderTargets,
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

func (h *JobHandler) resolveFolderTargets(ctx context.Context, folderIDs []string) []repository.FolderTarget {
	if len(folderIDs) == 0 {
		return []repository.FolderTarget{}
	}

	targets := make([]repository.FolderTarget, 0, len(folderIDs))
	for _, rawFolderID := range folderIDs {
		folderID := strings.TrimSpace(rawFolderID)
		if folderID == "" {
			continue
		}

		target := repository.FolderTarget{ID: folderID}
		if h.folders != nil {
			folder, err := h.folders.GetByID(ctx, folderID)
			if err == nil && folder != nil {
				target.Name = strings.TrimSpace(folder.Name)
				target.Path = strings.TrimSpace(folder.Path)
			}
		}
		if target.Name == "" {
			target.Name = folderID
		}
		targets = append(targets, target)
	}

	return targets
}
