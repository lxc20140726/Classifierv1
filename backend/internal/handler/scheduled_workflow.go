package handler

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/robfig/cron/v3"

	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/service"
)

type ScheduledWorkflowSyncer interface {
	Sync(ctx context.Context) error
}

type ScheduledWorkflowHandler struct {
	repo    repository.ScheduledWorkflowRepository
	service *service.ScheduledWorkflowService
	syncer  ScheduledWorkflowSyncer
}

func NewScheduledWorkflowHandler(repo repository.ScheduledWorkflowRepository, service *service.ScheduledWorkflowService, syncer ScheduledWorkflowSyncer) *ScheduledWorkflowHandler {
	return &ScheduledWorkflowHandler{repo: repo, service: service, syncer: syncer}
}

func (h *ScheduledWorkflowHandler) List(c *gin.Context) {
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

	items, total, err := h.repo.List(c.Request.Context(), repository.ScheduledWorkflowListFilter{Page: page, Limit: limit})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list scheduled workflows"})
		return
	}

	resp := make([]gin.H, 0, len(items))
	for _, item := range items {
		resp = append(resp, serializeScheduledWorkflow(item))
	}

	c.JSON(http.StatusOK, gin.H{"data": resp, "total": total, "page": page, "limit": limit})
}

func (h *ScheduledWorkflowHandler) Get(c *gin.Context) {
	item, err := h.repo.GetByID(c.Request.Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "scheduled workflow not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get scheduled workflow"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": serializeScheduledWorkflow(item)})
}

func (h *ScheduledWorkflowHandler) Create(c *gin.Context) {
	item, err := h.bindAndCreate(c)
	if err != nil {
		return
	}

	c.JSON(http.StatusCreated, gin.H{"data": serializeScheduledWorkflow(item)})
}

func (h *ScheduledWorkflowHandler) Update(c *gin.Context) {
	var req scheduledWorkflowUpsertRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	if validationErr := validateScheduledWorkflowRequest(req); validationErr != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": validationErr.Error()})
		return
	}

	item, err := h.service.Update(c.Request.Context(), c.Param("id"), req.JobType, req.Name, req.WorkflowDefID, req.CronSpec, req.Enabled, req.FolderIDs, req.SourceDirs)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "scheduled workflow not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if err := h.sync(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to sync scheduled workflows"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": serializeScheduledWorkflow(item)})
}

func (h *ScheduledWorkflowHandler) Delete(c *gin.Context) {
	if err := h.repo.Delete(c.Request.Context(), c.Param("id")); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "scheduled workflow not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete scheduled workflow"})
		return
	}
	if err := h.sync(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to sync scheduled workflows"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"deleted": true})
}

func (h *ScheduledWorkflowHandler) RunNow(c *gin.Context) {
	jobID, err := h.service.RunNow(c.Request.Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "scheduled workflow not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to start workflow job"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"job_id": jobID})
}

type scheduledWorkflowUpsertRequest struct {
	JobType       string   `json:"job_type"`
	Name          string   `json:"name"`
	WorkflowDefID string   `json:"workflow_def_id"`
	FolderIDs     []string `json:"folder_ids"`
	SourceDirs    []string `json:"source_dirs"`
	CronSpec      string   `json:"cron_spec"`
	Enabled       bool     `json:"enabled"`
}

func (h *ScheduledWorkflowHandler) bindAndCreate(c *gin.Context) (*repository.ScheduledWorkflow, error) {
	var req scheduledWorkflowUpsertRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return nil, err
	}
	if validationErr := validateScheduledWorkflowRequest(req); validationErr != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": validationErr.Error()})
		return nil, validationErr
	}

	item, err := h.service.Create(c.Request.Context(), req.JobType, req.Name, req.WorkflowDefID, req.CronSpec, req.Enabled, req.FolderIDs, req.SourceDirs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return nil, err
	}
	if err := h.sync(); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to sync scheduled workflows"})
		return nil, err
	}

	return item, nil
}

func validateScheduledWorkflowRequest(req scheduledWorkflowUpsertRequest) error {
	if strings.TrimSpace(req.Name) == "" {
		return errors.New("name is required")
	}
	jobType := strings.TrimSpace(req.JobType)
	if jobType == "" {
		jobType = "workflow"
	}
	if jobType != "workflow" && jobType != "scan" {
		return errors.New("invalid job_type")
	}
	if jobType == "workflow" {
		if strings.TrimSpace(req.WorkflowDefID) == "" {
			return errors.New("workflow_def_id is required")
		}
	}
	if jobType == "scan" && len(req.SourceDirs) == 0 {
		return errors.New("source_dirs is required")
	}
	if strings.TrimSpace(req.CronSpec) == "" {
		return errors.New("cron_spec is required")
	}
	if _, err := cron.ParseStandard(strings.TrimSpace(req.CronSpec)); err != nil {
		return errors.New("invalid cron_spec")
	}

	return nil
}

func serializeScheduledWorkflow(item *repository.ScheduledWorkflow) gin.H {
	folderIDs, _ := service.ScheduledWorkflowFolderIDsForAPI(item)
	sourceDirs, _ := service.ScheduledWorkflowSourceDirsForAPI(item)
	return gin.H{
		"id":              item.ID,
		"name":            item.Name,
		"job_type":        item.JobType,
		"workflow_def_id": item.WorkflowDefID,
		"folder_ids":      folderIDs,
		"source_dirs":     sourceDirs,
		"cron_spec":       item.CronSpec,
		"enabled":         item.Enabled,
		"last_run_at":     item.LastRunAt,
		"created_at":      item.CreatedAt,
		"updated_at":      item.UpdatedAt,
	}
}

func (h *ScheduledWorkflowHandler) sync() error {
	if h.syncer == nil {
		return nil
	}

	return h.syncer.Sync(context.Background())
}
