package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	internalfs "github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/service"
)

var validCategories = map[string]struct{}{
	"photo": {},
	"video": {},
	"mixed": {},
	"manga": {},
	"other": {},
}

var validStatuses = map[string]struct{}{
	"pending": {},
	"done":    {},
	"skip":    {},
}

type FolderScanStarter interface {
	StartJob(ctx context.Context, sourceDirs []string) (string, error)
}

type FolderLineageReader interface {
	GetFolderLineage(ctx context.Context, folderID string) (*service.FolderLineageResponse, error)
}

type FolderSourceManifestReader interface {
	ListLatestByFolderID(ctx context.Context, folderID string) ([]*repository.FolderSourceManifest, error)
}

type FolderHandler struct {
	folders          repository.FolderRepository
	config           repository.ConfigRepository
	scheduledJobs    repository.ScheduledWorkflowRepository
	scanStarter      FolderScanStarter
	outputChecker    service.OutputValidator
	lineageReader    FolderLineageReader
	manifestReader   FolderSourceManifestReader
	fs               internalfs.FSAdapter
	defaultSourceDir string
	deleteStagingDir string
}

func NewFolderHandler(folderRepo repository.FolderRepository, configRepo repository.ConfigRepository, scheduledJobRepo repository.ScheduledWorkflowRepository, scanStarter FolderScanStarter, fsAdapter internalfs.FSAdapter, sourceDir, deleteStagingDir string) *FolderHandler {
	return &FolderHandler{
		folders:          folderRepo,
		config:           configRepo,
		scheduledJobs:    scheduledJobRepo,
		scanStarter:      scanStarter,
		fs:               fsAdapter,
		defaultSourceDir: sourceDir,
		deleteStagingDir: deleteStagingDir,
	}
}

func (h *FolderHandler) SetOutputChecker(outputChecker service.OutputValidator) {
	h.outputChecker = outputChecker
}

func (h *FolderHandler) SetLineageReader(lineageReader FolderLineageReader) {
	h.lineageReader = lineageReader
}

func (h *FolderHandler) SetSourceManifestReader(manifestReader FolderSourceManifestReader) {
	h.manifestReader = manifestReader
}

func (h *FolderHandler) List(c *gin.Context) {
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

	filter := repository.FolderListFilter{
		Status:         c.Query("status"),
		Category:       c.Query("category"),
		Q:              c.Query("q"),
		Page:           page,
		Limit:          limit,
		IncludeDeleted: c.Query("include_deleted") == "true",
		OnlyDeleted:    c.Query("only_deleted") == "true",
		TopLevelOnly:   c.Query("top_level_only") == "true",
		SortBy:         c.Query("sort_by"),
		SortOrder:      c.Query("sort_order"),
	}

	items, total, err := h.folders.List(c.Request.Context(), filter)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list folders"})
		return
	}
	h.attachWorkflowSummaries(c.Request.Context(), items)

	c.JSON(http.StatusOK, gin.H{
		"data":  items,
		"total": total,
		"page":  page,
		"limit": limit,
	})
}

func (h *FolderHandler) Get(c *gin.Context) {
	id := c.Param("id")

	folder, err := h.folders.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "folder not found"})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get folder"})
		return
	}
	h.attachWorkflowSummary(c.Request.Context(), folder)

	c.JSON(http.StatusOK, gin.H{"data": folder})
}

func (h *FolderHandler) Scan(c *gin.Context) {
	sourceDirs, err := h.resolveScanSourceDirs(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if len(sourceDirs) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no scan directories configured"})
		return
	}

	if h.scanStarter == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "scan starter not configured"})
		return
	}

	jobID, err := h.scanStarter.StartJob(c.Request.Context(), sourceDirs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create scan job"})
		return
	}

	c.JSON(http.StatusAccepted, gin.H{"started": true, "job_id": jobID, "source_dirs": sourceDirs})
}

func (h *FolderHandler) resolveScanSourceDirs(ctx context.Context) ([]string, error) {
	if h.config == nil {
		return nil, errors.New("config repository is required")
	}

	sourceDirs := make([]string, 0, 4)
	seen := make(map[string]struct{}, 4)
	appendUnique := func(items []string) {
		for _, dir := range items {
			trimmed := strings.TrimSpace(dir)
			if trimmed == "" {
				continue
			}
			if _, ok := seen[trimmed]; ok {
				continue
			}
			seen[trimmed] = struct{}{}
			sourceDirs = append(sourceDirs, trimmed)
		}
	}

	if h.scheduledJobs != nil {
		enabledItems, err := h.scheduledJobs.ListEnabled(ctx)
		if err != nil {
			return nil, err
		}
		for _, item := range enabledItems {
			if item == nil || strings.TrimSpace(item.JobType) != "scan" {
				continue
			}
			if strings.TrimSpace(item.SourceDirs) == "" {
				continue
			}
			var scheduledSourceDirs []string
			if err := json.Unmarshal([]byte(item.SourceDirs), &scheduledSourceDirs); err != nil {
				return nil, fmt.Errorf("parse scheduled scan source_dirs: %w", err)
			}
			appendUnique(scheduledSourceDirs)
		}
		if len(sourceDirs) > 0 {
			return sourceDirs, nil
		}
	}

	appConfig, err := h.config.GetAppConfig(ctx)
	if err != nil {
		return nil, err
	}

	appendUnique(appConfig.ScanInputDirs)
	if len(sourceDirs) > 0 {
		return sourceDirs, nil
	}

	if trimmedDefaultSourceDir := strings.TrimSpace(h.defaultSourceDir); trimmedDefaultSourceDir != "" {
		return []string{trimmedDefaultSourceDir}, nil
	}

	return sourceDirs, nil
}

func (h *FolderHandler) UpdateCategory(c *gin.Context) {
	id := c.Param("id")

	var req struct {
		Category string `json:"category"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}

	if _, ok := validCategories[req.Category]; !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid category"})
		return
	}

	err := h.folders.UpdateCategory(c.Request.Context(), id, req.Category, "manual")
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "folder not found"})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update category"})
		return
	}

	folder, err := h.folders.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "folder not found"})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get folder"})
		return
	}
	h.attachWorkflowSummary(c.Request.Context(), folder)

	c.JSON(http.StatusOK, gin.H{"data": folder})
}

func (h *FolderHandler) UpdateStatus(c *gin.Context) {
	id := c.Param("id")

	var req struct {
		Status string `json:"status"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}

	if _, ok := validStatuses[req.Status]; !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid status"})
		return
	}
	if req.Status == "done" && h.outputChecker != nil {
		canMarkDone, err := h.outputChecker.CanMarkDone(c.Request.Context(), id)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to verify output check status"})
			return
		}
		if !canMarkDone {
			c.JSON(http.StatusBadRequest, gin.H{"error": "latest output check is not passed"})
			return
		}
	}

	err := h.folders.UpdateStatus(c.Request.Context(), id, req.Status)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "folder not found"})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update status"})
		return
	}

	folder, err := h.folders.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "folder not found"})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get folder"})
		return
	}
	h.attachWorkflowSummary(c.Request.Context(), folder)

	c.JSON(http.StatusOK, gin.H{"data": folder})
}

func (h *FolderHandler) GetOutputCheck(c *gin.Context) {
	if h.outputChecker == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "output checker not configured"})
		return
	}

	detail, err := h.outputChecker.GetLatestDetail(c.Request.Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "folder not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get output check"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": detail})
}

func (h *FolderHandler) RunOutputCheck(c *gin.Context) {
	if h.outputChecker == nil {
		c.JSON(http.StatusNotImplemented, gin.H{"error": "output checker not configured"})
		return
	}

	if _, err := h.outputChecker.ValidateFolder(c.Request.Context(), c.Param("id")); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "folder output mapping not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to run output check"})
		return
	}

	detail, err := h.outputChecker.GetLatestDetail(c.Request.Context(), c.Param("id"))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get output check"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": detail})
}

func (h *FolderHandler) Delete(c *gin.Context) {
	id := c.Param("id")
	folder, err := h.folders.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "folder not found"})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get folder"})
		return
	}

	if folder.DeletedAt != nil {
		c.JSON(http.StatusOK, gin.H{"data": gin.H{"suppressed": true}})
		return
	}

	err = h.folders.Suppress(c.Request.Context(), id, "", "")
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "folder not found"})
			return
		}

		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to suppress folder record"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": gin.H{"suppressed": true}})
}

func (h *FolderHandler) Restore(c *gin.Context) {
	id := c.Param("id")
	folder, err := h.folders.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "folder not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get folder"})
		return
	}
	if folder.DeletedAt == nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "folder is not suppressed"})
		return
	}
	if err := h.folders.Unsuppress(c.Request.Context(), id); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to restore suppressed folder"})
		return
	}
	restored, err := h.folders.GetByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get folder"})
		return
	}
	h.attachWorkflowSummary(c.Request.Context(), restored)
	c.JSON(http.StatusOK, gin.H{"data": restored})
}

func (h *FolderHandler) attachWorkflowSummary(ctx context.Context, folder *repository.Folder) {
	if folder == nil {
		return
	}
	h.attachWorkflowSummaries(ctx, []*repository.Folder{folder})
}

func (h *FolderHandler) attachWorkflowSummaries(ctx context.Context, folders []*repository.Folder) {
	if len(folders) == 0 {
		return
	}

	folderIDs := make([]string, 0, len(folders))
	for _, folder := range folders {
		if folder == nil {
			continue
		}
		folderIDs = append(folderIDs, folder.ID)
	}
	if len(folderIDs) == 0 {
		return
	}

	summaries, err := h.folders.ListWorkflowSummariesByFolderIDs(ctx, folderIDs)
	if err != nil {
		return
	}

	for _, folder := range folders {
		if folder == nil {
			continue
		}
		summary, ok := summaries[folder.ID]
		if !ok {
			continue
		}
		folder.WorkflowSummary = summary
	}
}
