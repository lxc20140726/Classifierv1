package service

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/liqiye/classifier/internal/repository"
)

func (s *FolderLineageService) buildArtifacts(
	ctx context.Context,
	folderID string,
	latestReview *repository.ProcessingReviewItem,
	reviewPayload *FolderLineageReview,
) ([]folderLineageArtifact, error) {
	artifacts := make([]folderLineageArtifact, 0, 8)
	seen := map[string]struct{}{}

	reviewTime := time.Time{}
	if latestReview != nil {
		reviewTime = latestReview.UpdatedAt
		if latestReview.ReviewedAt != nil && !latestReview.ReviewedAt.IsZero() {
			reviewTime = latestReview.ReviewedAt.UTC()
		}
	}
	if reviewPayload != nil {
		for _, path := range extractArtifactPaths(reviewPayload.After) {
			normalized := normalizeLineagePath(path)
			if normalized == "" {
				continue
			}
			if _, ok := seen[normalized]; ok {
				continue
			}
			seen[normalized] = struct{}{}
			artifacts = append(artifacts, folderLineageArtifact{
				path:          normalized,
				occurredAt:    reviewTime,
				workflowRunID: strings.TrimSpace(reviewPayload.WorkflowRunID),
				jobID:         strings.TrimSpace(reviewPayload.JobID),
				stepType:      resolveArtifactStepType(normalized, reviewPayload.ExecutedSteps),
			})
		}
	}

	if len(artifacts) > 0 || s.mappings == nil {
		return artifacts, nil
	}

	workflowRunID := ""
	if latestReview != nil {
		workflowRunID = strings.TrimSpace(latestReview.WorkflowRunID)
	}
	mappings := []*repository.FolderOutputMapping{}
	if workflowRunID != "" {
		items, listErr := s.mappings.ListByWorkflowRunAndFolderID(ctx, workflowRunID, folderID)
		if listErr != nil && !errors.Is(listErr, repository.ErrNotFound) {
			return nil, fmt.Errorf("folderLineage.buildArtifacts list output mappings for run %q folder %q: %w", workflowRunID, folderID, listErr)
		}
		if listErr == nil {
			mappings = append(mappings, items...)
		}
	}
	if len(mappings) == 0 {
		latestRunID, runErr := s.mappings.GetLatestWorkflowRunIDByFolderID(ctx, folderID)
		if runErr != nil && !errors.Is(runErr, repository.ErrNotFound) {
			return nil, fmt.Errorf("folderLineage.buildArtifacts get latest output mapping run for folder %q: %w", folderID, runErr)
		}
		if runErr == nil && strings.TrimSpace(latestRunID) != "" {
			items, listErr := s.mappings.ListByWorkflowRunAndFolderID(ctx, latestRunID, folderID)
			if listErr != nil && !errors.Is(listErr, repository.ErrNotFound) {
				return nil, fmt.Errorf("folderLineage.buildArtifacts list output mappings for latest run %q folder %q: %w", latestRunID, folderID, listErr)
			}
			if listErr == nil {
				mappings = append(mappings, items...)
			}
		}
	}

	for _, mapping := range mappings {
		if mapping == nil {
			continue
		}
		normalized := normalizeLineagePath(mapping.OutputPath)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		artifacts = append(artifacts, folderLineageArtifact{
			path:          normalized,
			stepType:      strings.TrimSpace(mapping.NodeType),
			occurredAt:    mapping.CreatedAt,
			workflowRunID: strings.TrimSpace(mapping.WorkflowRunID),
		})
	}

	sort.Slice(artifacts, func(i, j int) bool {
		if artifacts[i].occurredAt.Equal(artifacts[j].occurredAt) {
			return artifacts[i].path < artifacts[j].path
		}
		return artifacts[i].occurredAt.Before(artifacts[j].occurredAt)
	})

	return artifacts, nil
}

func (s *FolderLineageService) buildFlow(
	ctx context.Context,
	folderID string,
	latestReview *repository.ProcessingReviewItem,
) (*FolderLineageFlow, error) {
	if s.manifests == nil || s.mappings == nil {
		return nil, nil
	}

	manifests, err := s.manifests.ListByFolderID(ctx, folderID)
	if err != nil {
		return nil, fmt.Errorf("folderLineage.buildFlow list source manifests for folder %q: %w", folderID, err)
	}
	if len(manifests) == 0 {
		return nil, nil
	}

	workflowRunID, err := s.mappings.GetLatestWorkflowRunIDByFolderID(ctx, folderID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("folderLineage.buildFlow get latest output mapping run for folder %q: %w", folderID, err)
	}

	mappings, err := s.mappings.ListByWorkflowRunAndFolderID(ctx, workflowRunID, folderID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("folderLineage.buildFlow list output mappings for latest run %q folder %q: %w", workflowRunID, folderID, err)
	}
	if len(mappings) == 0 {
		return nil, nil
	}

	batches := groupLineageManifestBatches(manifests)
	bestScore := -1
	bestExactMatches := -1
	var bestFlow *FolderLineageFlow
	for _, batch := range batches {
		flow, exactMatches, score := buildFolderLineageFlow(batch, mappings, latestReview)
		if flow == nil {
			continue
		}
		if exactMatches > bestExactMatches || (exactMatches == bestExactMatches && score > bestScore) {
			bestFlow = flow
			bestExactMatches = exactMatches
			bestScore = score
		}
	}

	return bestFlow, nil
}

func groupLineageManifestBatches(manifests []*repository.FolderSourceManifest) [][]*repository.FolderSourceManifest {
	if len(manifests) == 0 {
		return nil
	}

	batches := make([][]*repository.FolderSourceManifest, 0, 4)
	currentBatchID := ""
	current := make([]*repository.FolderSourceManifest, 0, len(manifests))
	for _, manifest := range manifests {
		if manifest == nil {
			continue
		}
		batchID := strings.TrimSpace(manifest.BatchID)
		if len(current) > 0 && batchID != currentBatchID {
			batches = append(batches, current)
			current = make([]*repository.FolderSourceManifest, 0, len(manifests))
		}
		currentBatchID = batchID
		current = append(current, manifest)
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}

	return batches
}

func buildFolderLineageFlow(
	manifests []*repository.FolderSourceManifest,
	mappings []*repository.FolderOutputMapping,
	latestReview *repository.ProcessingReviewItem,
) (*FolderLineageFlow, int, int) {
	sourceDirectoryPath := deriveLineageSourceDirectoryPath(manifests)
	if sourceDirectoryPath == "." {
		sourceDirectoryPath = ""
	}
	sourceDirectoryLabel := filepath.Base(sourceDirectoryPath)
	if sourceDirectoryLabel == "." || sourceDirectoryLabel == "/" || sourceDirectoryLabel == "" {
		sourceDirectoryLabel = "扫描目录"
	}

	sourceFileOrderByID := make(map[string]int, len(manifests))
	sourceFileIDByPath := make(map[string]string, len(manifests))
	sourceFileIDByRelativePath := make(map[string]string, len(manifests))
	sourceFiles := make([]FolderLineageFlowSourceFile, 0, len(manifests))
	for idx, manifest := range manifests {
		if manifest == nil {
			continue
		}
		sourcePath := normalizeLineagePath(manifest.SourcePath)
		relativePath := normalizeLineagePath(manifest.RelativePath)
		fileID := strings.TrimSpace(manifest.ID)
		if fileID == "" {
			fileID = fmt.Sprintf("source-file-%d", idx+1)
		}
		sourceName := strings.TrimSpace(manifest.FileName)
		if sourceName == "" {
			sourceName = filepath.Base(sourcePath)
		}
		if sourceName == "." || sourceName == "/" || sourceName == "" {
			sourceName = strings.TrimSpace(relativePath)
		}
		sourceFiles = append(sourceFiles, FolderLineageFlowSourceFile{
			ID:           fileID,
			DirectoryID:  "source-directory",
			Name:         sourceName,
			Path:         sourcePath,
			RelativePath: relativePath,
			SizeBytes:    manifest.SizeBytes,
		})
		sourceFileOrderByID[fileID] = idx
		if sourcePath != "" {
			sourceFileIDByPath[sourcePath] = fileID
		}
		if relativePath != "" {
			sourceFileIDByRelativePath[relativePath] = fileID
		}
	}

	sort.Slice(sourceFiles, func(i, j int) bool {
		if sourceFiles[i].RelativePath == sourceFiles[j].RelativePath {
			return sourceFiles[i].Name < sourceFiles[j].Name
		}
		return sourceFiles[i].RelativePath < sourceFiles[j].RelativePath
	})
	for idx := range sourceFiles {
		sourceFileOrderByID[sourceFiles[idx].ID] = idx
	}

	type targetDirectoryState struct {
		directory FolderLineageFlowDirectory
		order     int
	}
	targetDirectoryByPath := map[string]*targetDirectoryState{}
	targetDirectories := make([]FolderLineageFlowDirectory, 0, 4)
	targetFiles := make([]FolderLineageFlowTargetFile, 0, len(mappings))
	links := make([]FolderLineageFlowLink, 0, len(mappings))
	exactMatchCount := 0

	reviewJobID := ""
	reviewWorkflowRunID := ""
	if latestReview != nil {
		reviewJobID = strings.TrimSpace(latestReview.JobID)
		reviewWorkflowRunID = strings.TrimSpace(latestReview.WorkflowRunID)
	}

	for idx, mapping := range mappings {
		if mapping == nil {
			continue
		}
		usedExactMatch := false
		sourceFileID := sourceFileIDByPath[normalizeLineagePath(mapping.SourcePath)]
		if sourceFileID != "" {
			usedExactMatch = true
		}
		if sourceFileID == "" {
			sourceFileID = sourceFileIDByRelativePath[normalizeLineagePath(mapping.SourceRelativePath)]
		}
		if sourceFileID == "" {
			continue
		}

		targetPath := normalizeLineagePath(mapping.OutputPath)
		if targetPath == "" {
			continue
		}
		targetDirectoryPath := normalizeLineagePath(filepath.Dir(targetPath))
		directory, ok := targetDirectoryByPath[targetDirectoryPath]
		if !ok {
			directoryID := fmt.Sprintf("target-directory-%d", len(targetDirectories)+1)
			directoryLabel := filepath.Base(targetDirectoryPath)
			if directoryLabel == "." || directoryLabel == "/" || strings.TrimSpace(directoryLabel) == "" {
				directoryLabel = targetDirectoryPath
			}
			state := &targetDirectoryState{
				directory: FolderLineageFlowDirectory{
					ID:           directoryID,
					Path:         targetDirectoryPath,
					Label:        directoryLabel,
					ArtifactType: strings.TrimSpace(mapping.ArtifactType),
				},
				order: len(targetDirectories),
			}
			targetDirectoryByPath[targetDirectoryPath] = state
			targetDirectories = append(targetDirectories, state.directory)
			directory = state
		}
		if directory.directory.ArtifactType == "" && strings.TrimSpace(mapping.ArtifactType) != "" {
			directory.directory.ArtifactType = strings.TrimSpace(mapping.ArtifactType)
			targetDirectories[directory.order].ArtifactType = directory.directory.ArtifactType
		}

		targetFileID := strings.TrimSpace(mapping.ID)
		if targetFileID == "" {
			targetFileID = fmt.Sprintf("target-file-%d", idx+1)
		}
		jobID := ""
		if reviewWorkflowRunID != "" && reviewWorkflowRunID == strings.TrimSpace(mapping.WorkflowRunID) {
			jobID = reviewJobID
		}

		targetFiles = append(targetFiles, FolderLineageFlowTargetFile{
			ID:            targetFileID,
			DirectoryID:   directory.directory.ID,
			Name:          filepath.Base(targetPath),
			Path:          targetPath,
			ArtifactType:  strings.TrimSpace(mapping.ArtifactType),
			NodeType:      strings.TrimSpace(mapping.NodeType),
			WorkflowRunID: strings.TrimSpace(mapping.WorkflowRunID),
			JobID:         jobID,
		})

		linkID := strings.TrimSpace(mapping.ID)
		if linkID == "" {
			linkID = fmt.Sprintf("link-%d", idx+1)
		} else {
			linkID = "link-" + linkID
		}
		links = append(links, FolderLineageFlowLink{
			ID:            linkID,
			SourceFileID:  sourceFileID,
			TargetFileID:  targetFileID,
			WorkflowRunID: strings.TrimSpace(mapping.WorkflowRunID),
			JobID:         jobID,
			NodeType:      strings.TrimSpace(mapping.NodeType),
		})
		if usedExactMatch {
			exactMatchCount++
		}
	}

	if len(links) == 0 {
		return nil, 0, 0
	}

	sourceOrderByTargetFileID := map[string]int{}
	for _, link := range links {
		order, ok := sourceFileOrderByID[link.SourceFileID]
		if !ok {
			continue
		}
		currentOrder, exists := sourceOrderByTargetFileID[link.TargetFileID]
		if !exists || order < currentOrder {
			sourceOrderByTargetFileID[link.TargetFileID] = order
		}
	}

	sort.Slice(targetFiles, func(i, j int) bool {
		leftOrder := sourceOrderByTargetFileID[targetFiles[i].ID]
		rightOrder := sourceOrderByTargetFileID[targetFiles[j].ID]
		if leftOrder != rightOrder {
			return leftOrder < rightOrder
		}
		return targetFiles[i].Name < targetFiles[j].Name
	})

	return &FolderLineageFlow{
		SourceDirectory: FolderLineageFlowDirectory{
			Path:  sourceDirectoryPath,
			Label: sourceDirectoryLabel,
		},
		TargetDirectories: targetDirectories,
		SourceFiles:       sourceFiles,
		TargetFiles:       targetFiles,
		Links:             links,
	}, exactMatchCount, len(links)
}

func deriveLineageSourceDirectoryPath(manifests []*repository.FolderSourceManifest) string {
	for _, manifest := range manifests {
		if manifest == nil {
			continue
		}

		sourcePath := normalizeLineagePath(manifest.SourcePath)
		relativePath := normalizeLineagePath(manifest.RelativePath)
		if sourcePath == "" {
			continue
		}
		if relativePath == "" || relativePath == "." {
			return normalizeLineagePath(filepath.Dir(sourcePath))
		}

		normalizedSourceDir := normalizeLineagePath(filepath.Dir(sourcePath))
		normalizedRelativeDir := normalizeLineagePath(filepath.Dir(relativePath))
		if normalizedRelativeDir == "." || normalizedRelativeDir == "" {
			return normalizedSourceDir
		}

		rootPath := strings.TrimSuffix(normalizedSourceDir, "/"+normalizedRelativeDir)
		rootPath = strings.TrimSuffix(rootPath, "/")
		if rootPath != "" && rootPath != normalizedSourceDir {
			return normalizeLineagePath(rootPath)
		}
		return normalizedSourceDir
	}

	return ""
}

func (s *FolderLineageService) buildTimeline(
	ctx context.Context,
	folderID string,
	observations []*repository.FolderPathObservation,
	transitions []folderLineagePathTransition,
	artifacts []folderLineageArtifact,
) ([]FolderLineageTimelineEvent, error) {
	events := make([]FolderLineageTimelineEvent, 0, len(transitions)+len(artifacts)+2)

	firstObservation := firstNonNilObservation(observations)
	if firstObservation != nil {
		events = append(events, FolderLineageTimelineEvent{
			ID:         "scan-discovered",
			Type:       folderLineageEventTypeScanDiscovered,
			OccurredAt: firstObservation.FirstSeenAt,
			Title:      "扫描发现文件夹",
			PathTo:     normalizeLineagePath(firstObservation.Path),
		})
	}

	for idx, transition := range transitions {
		if transition.from == "" || transition.to == "" {
			continue
		}
		id := fmt.Sprintf("path-%d", idx+1)
		eventType := transition.eventType
		pathFrom := transition.from
		pathTo := transition.to
		title := "路径移动"
		if eventType == folderLineageEventTypeRename {
			title = "路径重命名"
		}
		if eventType == folderLineageEventTypeRollback {
			pathFrom = transition.to
			pathTo = transition.from
			title = "路径回滚"
		}

		events = append(events, FolderLineageTimelineEvent{
			ID:            id,
			Type:          eventType,
			OccurredAt:    transition.occurredAt,
			Title:         title,
			PathFrom:      pathFrom,
			PathTo:        pathTo,
			WorkflowRunID: transition.workflowRunID,
			JobID:         transition.jobID,
			StepType:      transition.stepType,
		})
	}

	for idx, artifact := range artifacts {
		if artifact.path == "" {
			continue
		}
		events = append(events, FolderLineageTimelineEvent{
			ID:            fmt.Sprintf("artifact-%d", idx+1),
			Type:          folderLineageEventTypeArtifactCreate,
			OccurredAt:    artifact.occurredAt,
			Title:         "生成处理产物",
			PathTo:        artifact.path,
			WorkflowRunID: artifact.workflowRunID,
			JobID:         artifact.jobID,
			StepType:      artifact.stepType,
		})
	}

	if s.audits != nil {
		logs, _, listErr := s.audits.List(ctx, repository.AuditListFilter{
			FolderID: folderID,
			Page:     1,
			Limit:    2000,
		})
		if listErr != nil {
			return nil, fmt.Errorf("folderLineage.buildTimeline list audit logs for folder %q: %w", folderID, listErr)
		}
		for _, log := range logs {
			if log == nil || !isFailedAuditLog(log) {
				continue
			}
			description := strings.TrimSpace(log.ErrorMsg)
			if description == "" {
				description = strings.TrimSpace(log.Action)
			}
			events = append(events, FolderLineageTimelineEvent{
				ID:            "audit-" + strings.TrimSpace(log.ID),
				Type:          folderLineageEventTypeProcessingFail,
				OccurredAt:    log.CreatedAt,
				Title:         "处理失败",
				Description:   description,
				PathTo:        normalizeLineagePath(log.FolderPath),
				WorkflowRunID: strings.TrimSpace(log.WorkflowRunID),
				JobID:         strings.TrimSpace(log.JobID),
				StepType:      strings.TrimSpace(log.NodeType),
			})
		}
	}

	sort.Slice(events, func(i, j int) bool {
		if events[i].OccurredAt.Equal(events[j].OccurredAt) {
			return events[i].ID < events[j].ID
		}
		return events[i].OccurredAt.Before(events[j].OccurredAt)
	})

	return events, nil
}

func buildFolderLineageReview(review *repository.ProcessingReviewItem) *FolderLineageReview {
	if review == nil {
		return nil
	}

	before := parseRawObject(review.BeforeJSON)
	after := parseRawObject(review.AfterJSON)
	diff := parseRawObject(review.DiffJSON)
	steps := parseReviewSteps(review.StepResultsJSON)
	if len(steps) == 0 {
		steps = parseDiffExecutedSteps(diff)
	}

	return &FolderLineageReview{
		WorkflowRunID: strings.TrimSpace(review.WorkflowRunID),
		JobID:         strings.TrimSpace(review.JobID),
		Status:        strings.TrimSpace(review.Status),
		Before:        before,
		After:         after,
		Diff:          diff,
		ExecutedSteps: steps,
		UpdatedAt:     review.UpdatedAt,
		ReviewedAt:    review.ReviewedAt,
	}
}

func resolveLastProcessedAt(
	review *repository.ProcessingReviewItem,
	transitions []folderLineagePathTransition,
	artifacts []folderLineageArtifact,
) *time.Time {
	latest := time.Time{}
	update := func(candidate time.Time) {
		if candidate.IsZero() {
			return
		}
		if latest.IsZero() || candidate.After(latest) {
			latest = candidate
		}
	}

	if review != nil {
		update(review.UpdatedAt.UTC())
		if review.ReviewedAt != nil {
			update(review.ReviewedAt.UTC())
		}
	}
	for _, transition := range transitions {
		update(transition.occurredAt.UTC())
	}
	for _, artifact := range artifacts {
		update(artifact.occurredAt.UTC())
	}

	if latest.IsZero() {
		return nil
	}
	return &latest
}
