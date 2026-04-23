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

type folderObservationReader interface {
	ListPathObservationsByFolderID(ctx context.Context, folderID string) ([]*repository.FolderPathObservation, error)
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

	var (
		items []*repository.Folder
		total int
		err   error
	)
	if filter.TopLevelOnly {
		items, total, err = h.listTopLevelFolders(c.Request.Context(), filter)
	} else {
		items, total, err = h.folders.List(c.Request.Context(), filter)
		if err == nil {
			h.attachWorkflowSummaries(c.Request.Context(), items)
		}
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list folders"})
		return
	}

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

func (h *FolderHandler) listTopLevelFolders(ctx context.Context, filter repository.FolderListFilter) ([]*repository.Folder, int, error) {
	countFilter := filter
	countFilter.Page = 1
	countFilter.Limit = 1

	_, rawTotal, err := h.folders.List(ctx, countFilter)
	if err != nil {
		return nil, 0, err
	}
	if rawTotal == 0 {
		return []*repository.Folder{}, 0, nil
	}

	allFilter := filter
	allFilter.Page = 1
	allFilter.Limit = rawTotal

	items, _, err := h.folders.List(ctx, allFilter)
	if err != nil {
		return nil, 0, err
	}
	h.attachWorkflowSummaries(ctx, items)

	aggregated, err := h.aggregateTopLevelFolders(ctx, items)
	if err != nil {
		return nil, 0, err
	}

	offset := (filter.Page - 1) * filter.Limit
	if offset >= len(aggregated) {
		return []*repository.Folder{}, len(aggregated), nil
	}

	end := offset + filter.Limit
	if end > len(aggregated) {
		end = len(aggregated)
	}

	return aggregated[offset:end], len(aggregated), nil
}

func (h *FolderHandler) aggregateTopLevelFolders(ctx context.Context, items []*repository.Folder) ([]*repository.Folder, error) {
	observationReader, ok := h.folders.(folderObservationReader)
	if !ok || len(items) == 0 {
		return items, nil
	}

	grouped := make(map[string][]*repository.Folder, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		key := normalizeFolderAggregateKey(item.RelativePath)
		if key == "" {
			key = "folder:" + strings.TrimSpace(item.ID)
		}
		grouped[key] = append(grouped[key], item)
	}

	type aggregatePlan struct {
		merged    *repository.Folder
		memberIDs map[string]struct{}
	}

	plans := make(map[string]aggregatePlan, len(grouped))
	hiddenWorkflowOnlyIDs := make(map[string]struct{})
	for key, group := range grouped {
		base := selectTopLevelAggregateBase(group)
		if base == nil {
			for _, item := range group {
				if item == nil || !strings.EqualFold(strings.TrimSpace(item.CategorySource), "workflow") {
					continue
				}
				observations, err := observationReader.ListPathObservationsByFolderID(ctx, item.ID)
				if err != nil {
					return nil, err
				}
				if folderLooksLikePromotedWorkflowChild(item, observations) {
					hiddenWorkflowOnlyIDs[strings.TrimSpace(item.ID)] = struct{}{}
				}
			}
			continue
		}

		matchedWorkflowFolders := make([]*repository.Folder, 0, len(group))
		memberIDs := map[string]struct{}{
			strings.TrimSpace(base.ID): {},
		}
		for _, item := range group {
			if item == nil || strings.TrimSpace(item.ID) == strings.TrimSpace(base.ID) {
				continue
			}
			if !strings.EqualFold(strings.TrimSpace(item.CategorySource), "workflow") {
				continue
			}
			observations, err := observationReader.ListPathObservationsByFolderID(ctx, item.ID)
			if err != nil {
				return nil, err
			}
			if !folderHistoricallyBelongsToRoot(base.Path, observations) {
				continue
			}
			matchedWorkflowFolders = append(matchedWorkflowFolders, item)
			memberIDs[strings.TrimSpace(item.ID)] = struct{}{}
		}
		if len(matchedWorkflowFolders) == 0 {
			continue
		}

		plans[key] = aggregatePlan{
			merged:    mergeTopLevelFolderAggregate(base, matchedWorkflowFolders),
			memberIDs: memberIDs,
		}
	}

	aggregated := make([]*repository.Folder, 0, len(items))
	consumed := make(map[string]struct{}, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		itemID := strings.TrimSpace(item.ID)
		if _, ok := consumed[itemID]; ok {
			continue
		}
		if _, ok := hiddenWorkflowOnlyIDs[itemID]; ok {
			consumed[itemID] = struct{}{}
			continue
		}

		key := normalizeFolderAggregateKey(item.RelativePath)
		plan, ok := plans[key]
		if !ok {
			aggregated = append(aggregated, item)
			consumed[itemID] = struct{}{}
			continue
		}
		if _, ok := plan.memberIDs[itemID]; !ok {
			aggregated = append(aggregated, item)
			consumed[itemID] = struct{}{}
			continue
		}

		aggregated = append(aggregated, plan.merged)
		for memberID := range plan.memberIDs {
			consumed[memberID] = struct{}{}
		}
	}

	return aggregated, nil
}

func normalizeFolderAggregateKey(relativePath string) string {
	trimmed := strings.TrimSpace(relativePath)
	if trimmed == "" {
		return ""
	}

	normalized := strings.ReplaceAll(trimmed, `\`, "/")
	return strings.Trim(normalized, "/")
}

func selectTopLevelAggregateBase(group []*repository.Folder) *repository.Folder {
	var base *repository.Folder
	for _, item := range group {
		if item == nil {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(item.CategorySource), "workflow") {
			continue
		}
		if base != nil {
			return nil
		}
		base = item
	}
	return base
}

func folderLooksLikePromotedWorkflowChild(folder *repository.Folder, observations []*repository.FolderPathObservation) bool {
	if folder == nil || !strings.EqualFold(strings.TrimSpace(folder.CategorySource), "workflow") {
		return false
	}
	currentKey := normalizeFolderAggregateKey(folder.RelativePath)
	if currentKey == "" || strings.Contains(currentKey, "/") {
		return false
	}

	hasCurrentTopLevelObservation := false
	for _, observation := range observations {
		if observation == nil {
			continue
		}
		observationKey := normalizeFolderAggregateKey(observation.RelativePath)
		if observation.IsCurrent && observationKey == currentKey {
			hasCurrentTopLevelObservation = true
			continue
		}
		if observationLooksNestedUnderAggregateKey(currentKey, observationKey, observation.Path) {
			return true
		}
	}

	return !hasCurrentTopLevelObservation && observationLooksNestedUnderAggregateKey(currentKey, "", folder.Path)
}

func observationLooksNestedUnderAggregateKey(currentKey, observationKey, observationPath string) bool {
	if currentKey == "" {
		return false
	}
	if strings.HasPrefix(observationKey, currentKey+"/") {
		return true
	}

	normalizedPath := normalizeFolderAggregatePath(observationPath)
	if normalizedPath == "" {
		return false
	}
	segments := strings.Split(normalizedPath, "/")
	for index := 0; index < len(segments)-1; index++ {
		if strings.EqualFold(strings.TrimSpace(segments[index]), currentKey) {
			return true
		}
	}
	return false
}

func folderHistoricallyBelongsToRoot(rootPath string, observations []*repository.FolderPathObservation) bool {
	normalizedRootPath := normalizeFolderAggregatePath(rootPath)
	if normalizedRootPath == "" {
		return false
	}

	for _, observation := range observations {
		if observation == nil {
			continue
		}
		observedPath := normalizeFolderAggregatePath(observation.Path)
		if observedPath == normalizedRootPath || strings.HasPrefix(observedPath, normalizedRootPath+"/") {
			return true
		}
	}

	return false
}

func normalizeFolderAggregatePath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}

	normalized := strings.ReplaceAll(trimmed, `\`, "/")
	return strings.TrimRight(normalized, "/")
}

func mergeTopLevelFolderAggregate(base *repository.Folder, workflowFolders []*repository.Folder) *repository.Folder {
	merged := *base
	merged.WorkflowSummary = mergeFolderWorkflowSummary(base.WorkflowSummary, repository.FolderWorkflowSummary{})

	for _, item := range workflowFolders {
		if item == nil {
			continue
		}
		merged.WorkflowSummary = mergeFolderWorkflowSummary(merged.WorkflowSummary, item.WorkflowSummary)
		if item.UpdatedAt.After(merged.UpdatedAt) {
			merged.UpdatedAt = item.UpdatedAt
		}
		if merged.CoverImagePath == "" && strings.TrimSpace(item.CoverImagePath) != "" {
			merged.CoverImagePath = item.CoverImagePath
		}
		merged.OutputCheckSummary = mergeFolderOutputCheckSummary(merged.OutputCheckSummary, item.OutputCheckSummary)
	}

	return &merged
}

func mergeFolderWorkflowSummary(current, candidate repository.FolderWorkflowSummary) repository.FolderWorkflowSummary {
	current.Classification = mergeWorkflowStageSummary(current.Classification, candidate.Classification)
	current.Processing = mergeWorkflowStageSummary(current.Processing, candidate.Processing)
	return current
}

func mergeWorkflowStageSummary(current, candidate repository.WorkflowStageSummary) repository.WorkflowStageSummary {
	if !shouldPreferWorkflowStageSummary(current, candidate) {
		return current
	}
	return candidate
}

func shouldPreferWorkflowStageSummary(current, candidate repository.WorkflowStageSummary) bool {
	candidateStatus := strings.TrimSpace(candidate.Status)
	if candidateStatus == "" || candidateStatus == "not_run" {
		return strings.TrimSpace(current.Status) == ""
	}

	currentStatus := strings.TrimSpace(current.Status)
	if currentStatus == "" || currentStatus == "not_run" {
		return true
	}

	if candidate.UpdatedAt != nil && !candidate.UpdatedAt.IsZero() {
		if current.UpdatedAt == nil || current.UpdatedAt.IsZero() {
			return true
		}
		if candidate.UpdatedAt.After(*current.UpdatedAt) {
			return true
		}
		if candidate.UpdatedAt.Before(*current.UpdatedAt) {
			return false
		}
	}

	return workflowStageRank(candidateStatus) > workflowStageRank(currentStatus)
}

func workflowStageRank(status string) int {
	switch strings.TrimSpace(status) {
	case "failed":
		return 6
	case "waiting_input":
		return 5
	case "partial":
		return 4
	case "running":
		return 3
	case "succeeded":
		return 2
	case "rolled_back":
		return 1
	default:
		return 0
	}
}

func mergeFolderOutputCheckSummary(current, candidate repository.FolderOutputCheckSummary) repository.FolderOutputCheckSummary {
	candidateStatus := strings.TrimSpace(candidate.Status)
	currentStatus := strings.TrimSpace(current.Status)
	if candidateStatus == "" {
		return current
	}
	if currentStatus == "" || currentStatus == "pending" {
		if candidateStatus != "pending" {
			return candidate
		}
	}
	if candidate.CheckedAt != nil && !candidate.CheckedAt.IsZero() {
		if current.CheckedAt == nil || current.CheckedAt.IsZero() || candidate.CheckedAt.After(*current.CheckedAt) {
			return candidate
		}
	}
	return current
}
