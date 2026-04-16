package service

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
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
	folders := []*repository.Folder{{ID: folderID}}
	reviewsByFolderID := map[string]*repository.ProcessingReviewItem{}
	if latestReview != nil {
		reviewsByFolderID[strings.TrimSpace(folderID)] = latestReview
	}
	_ = reviewPayload
	return s.buildArtifactsForFolders(ctx, folders, reviewsByFolderID)
}

func (s *FolderLineageService) buildFlow(
	ctx context.Context,
	folderID string,
	latestReview *repository.ProcessingReviewItem,
) (*FolderLineageFlow, error) {
	folders := []*repository.Folder{{ID: folderID}}
	reviewsByFolderID := map[string]*repository.ProcessingReviewItem{}
	if latestReview != nil {
		reviewsByFolderID[strings.TrimSpace(folderID)] = latestReview
	}
	return s.buildFlowForFolders(ctx, folders, reviewsByFolderID)
}

func (s *FolderLineageService) buildArtifactsForFolders(
	ctx context.Context,
	folders []*repository.Folder,
	reviewsByFolderID map[string]*repository.ProcessingReviewItem,
) ([]folderLineageArtifact, error) {
	artifacts := make([]folderLineageArtifact, 0, len(folders)*4)
	seen := map[string]struct{}{}
	reviewJobsByWorkflowRun := buildLineageReviewJobsByWorkflowRun(reviewsByFolderID)

	for _, review := range orderedFolderReviews(reviewsByFolderID) {
		if review == nil {
			continue
		}
		reviewPayload := buildFolderLineageReview(review)
		reviewTime := folderReviewComparisonTime(review)
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

	if s.mappings == nil {
		return sortFolderLineageArtifacts(artifacts), nil
	}

	for _, folder := range folders {
		if folder == nil {
			continue
		}
		folderID := strings.TrimSpace(folder.ID)
		review := reviewsByFolderID[folderID]
		preferredRunID := ""
		if review != nil {
			preferredRunID = strings.TrimSpace(review.WorkflowRunID)
		}

		mappings, _, err := s.loadLineageMappingsForFolder(ctx, folderID, preferredRunID)
		if err != nil {
			return nil, err
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
				jobID:         reviewJobsByWorkflowRun[strings.TrimSpace(mapping.WorkflowRunID)],
			})
		}
	}

	return sortFolderLineageArtifacts(artifacts), nil
}

func (s *FolderLineageService) buildFlowForFolders(
	ctx context.Context,
	folders []*repository.Folder,
	reviewsByFolderID map[string]*repository.ProcessingReviewItem,
) (*FolderLineageFlow, error) {
	if s.manifests == nil || s.mappings == nil {
		return nil, nil
	}

	reviewJobsByWorkflowRun := buildLineageReviewJobsByWorkflowRun(reviewsByFolderID)
	combinedManifests := make([]*repository.FolderSourceManifest, 0, len(folders)*8)
	combinedMappings := make([]*repository.FolderOutputMapping, 0, len(folders)*8)
	seenManifestIDs := make(map[string]struct{})
	seenMappingIDs := make(map[string]struct{})

	for _, folder := range folders {
		if folder == nil {
			continue
		}
		folderID := strings.TrimSpace(folder.ID)
		review := reviewsByFolderID[folderID]
		preferredRunID := ""
		if review != nil {
			preferredRunID = strings.TrimSpace(review.WorkflowRunID)
		}

		mappings, workflowRunID, err := s.loadLineageMappingsForFolder(ctx, folderID, preferredRunID)
		if err != nil {
			return nil, err
		}
		if len(mappings) == 0 {
			continue
		}

		manifests, err := s.loadLineageManifestsForFolderRun(ctx, folderID, workflowRunID, mappings, reviewJobsByWorkflowRun)
		if err != nil {
			return nil, err
		}
		if len(manifests) == 0 {
			continue
		}

		for _, manifest := range manifests {
			if manifest == nil {
				continue
			}
			manifestID := strings.TrimSpace(manifest.ID)
			if manifestID != "" {
				if _, ok := seenManifestIDs[manifestID]; ok {
					continue
				}
				seenManifestIDs[manifestID] = struct{}{}
			}
			combinedManifests = append(combinedManifests, manifest)
		}
		for _, mapping := range mappings {
			if mapping == nil {
				continue
			}
			mappingID := strings.TrimSpace(mapping.ID)
			if mappingID != "" {
				if _, ok := seenMappingIDs[mappingID]; ok {
					continue
				}
				seenMappingIDs[mappingID] = struct{}{}
			}
			combinedMappings = append(combinedMappings, mapping)
		}
	}

	if len(combinedManifests) == 0 || len(combinedMappings) == 0 {
		return nil, nil
	}

	flow, _, _ := buildFolderLineageFlow(combinedManifests, combinedMappings, reviewJobsByWorkflowRun)
	return flow, nil
}

func sortFolderLineageArtifacts(artifacts []folderLineageArtifact) []folderLineageArtifact {
	sort.Slice(artifacts, func(i, j int) bool {
		if artifacts[i].occurredAt.Equal(artifacts[j].occurredAt) {
			return artifacts[i].path < artifacts[j].path
		}
		return artifacts[i].occurredAt.Before(artifacts[j].occurredAt)
	})
	return artifacts
}

func buildLineageReviewJobsByWorkflowRun(
	reviewsByFolderID map[string]*repository.ProcessingReviewItem,
) map[string]string {
	jobsByWorkflowRun := make(map[string]string, len(reviewsByFolderID))
	for _, review := range reviewsByFolderID {
		if review == nil {
			continue
		}
		workflowRunID := strings.TrimSpace(review.WorkflowRunID)
		if workflowRunID == "" {
			continue
		}
		jobsByWorkflowRun[workflowRunID] = strings.TrimSpace(review.JobID)
	}
	return jobsByWorkflowRun
}

func (s *FolderLineageService) loadLineageMappingsForFolder(
	ctx context.Context,
	folderID string,
	preferredWorkflowRunID string,
) ([]*repository.FolderOutputMapping, string, error) {
	if s.mappings == nil {
		return nil, "", nil
	}

	workflowRunID := strings.TrimSpace(preferredWorkflowRunID)
	if workflowRunID != "" {
		items, err := s.mappings.ListByWorkflowRunAndFolderID(ctx, workflowRunID, folderID)
		if err != nil && !errors.Is(err, repository.ErrNotFound) {
			return nil, "", fmt.Errorf("folderLineage.loadLineageMappingsForFolder list output mappings for run %q folder %q: %w", workflowRunID, folderID, err)
		}
		if err == nil && len(items) > 0 {
			return items, workflowRunID, nil
		}
	}

	workflowRunID, err := s.mappings.GetLatestWorkflowRunIDByFolderID(ctx, folderID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("folderLineage.loadLineageMappingsForFolder get latest output mapping run for folder %q: %w", folderID, err)
	}

	items, err := s.mappings.ListByWorkflowRunAndFolderID(ctx, workflowRunID, folderID)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, "", nil
		}
		return nil, "", fmt.Errorf("folderLineage.loadLineageMappingsForFolder list output mappings for latest run %q folder %q: %w", workflowRunID, folderID, err)
	}

	return items, workflowRunID, nil
}

func (s *FolderLineageService) loadLineageManifestsForFolderRun(
	ctx context.Context,
	folderID string,
	workflowRunID string,
	mappings []*repository.FolderOutputMapping,
	reviewJobsByWorkflowRun map[string]string,
) ([]*repository.FolderSourceManifest, error) {
	if s.manifests == nil {
		return nil, nil
	}

	trimmedWorkflowRunID := strings.TrimSpace(workflowRunID)
	if trimmedWorkflowRunID != "" {
		items, err := s.manifests.ListByWorkflowRunAndFolderID(ctx, trimmedWorkflowRunID, folderID)
		if err != nil && !errors.Is(err, repository.ErrNotFound) {
			return nil, fmt.Errorf("folderLineage.loadLineageManifestsForFolderRun list manifests for run %q folder %q: %w", trimmedWorkflowRunID, folderID, err)
		}
		if err == nil && len(items) > 0 {
			return items, nil
		}
	}

	items, err := s.manifests.ListByFolderID(ctx, folderID)
	if err != nil {
		return nil, fmt.Errorf("folderLineage.loadLineageManifestsForFolderRun list manifests for folder %q: %w", folderID, err)
	}
	if len(items) == 0 {
		return nil, nil
	}

	return selectBestLineageManifestBatch(items, mappings, reviewJobsByWorkflowRun), nil
}

func selectBestLineageManifestBatch(
	manifests []*repository.FolderSourceManifest,
	mappings []*repository.FolderOutputMapping,
	reviewJobsByWorkflowRun map[string]string,
) []*repository.FolderSourceManifest {
	batches := groupLineageManifestBatches(manifests)
	bestScore := -1
	bestExactMatches := -1
	var bestBatch []*repository.FolderSourceManifest
	for _, batch := range batches {
		flow, exactMatches, score := buildFolderLineageFlow(batch, mappings, reviewJobsByWorkflowRun)
		if flow == nil {
			continue
		}
		if exactMatches > bestExactMatches || (exactMatches == bestExactMatches && score > bestScore) {
			bestBatch = batch
			bestExactMatches = exactMatches
			bestScore = score
		}
	}
	return bestBatch
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
	reviewJobsByWorkflowRun map[string]string,
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
	artifactSourceFileIDsByPath := make(map[string][]string, len(mappings))
	sortedMappings := cloneAndSortLineageMappings(mappings)
	processedMappings := make([]bool, len(sortedMappings))

	progress := true
	for progress {
		progress = false

		for idx, mapping := range sortedMappings {
			if mapping == nil || processedMappings[idx] {
				continue
			}

			sourceFileIDs, usedExactMatch := resolveLineageFlowSourceFileIDs(
				sourceFiles,
				sourceFileIDByPath,
				sourceFileIDByRelativePath,
				artifactSourceFileIDsByPath,
				mapping,
			)
			if len(sourceFileIDs) == 0 {
				continue
			}

			targetPath := normalizeLineagePath(mapping.OutputPath)
			if targetPath == "" {
				processedMappings[idx] = true
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
			jobID := reviewJobsByWorkflowRun[strings.TrimSpace(mapping.WorkflowRunID)]

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

			linkIDBase := strings.TrimSpace(mapping.ID)
			if linkIDBase == "" {
				linkIDBase = fmt.Sprintf("link-%d", idx+1)
			} else {
				linkIDBase = "link-" + linkIDBase
			}
			for sourceIdx, sourceFileID := range sourceFileIDs {
				links = append(links, FolderLineageFlowLink{
					ID:            buildLineageFlowLinkID(linkIDBase, sourceIdx, len(sourceFileIDs)),
					SourceFileID:  sourceFileID,
					TargetFileID:  targetFileID,
					WorkflowRunID: strings.TrimSpace(mapping.WorkflowRunID),
					JobID:         jobID,
					NodeType:      strings.TrimSpace(mapping.NodeType),
				})
			}
			artifactSourceFileIDsByPath[targetPath] = mergeLineageFlowSourceFileIDs(
				artifactSourceFileIDsByPath[targetPath],
				sourceFileIDs,
				sourceFileOrderByID,
			)
			if usedExactMatch {
				exactMatchCount++
			}
			processedMappings[idx] = true
			progress = true
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

func cloneAndSortLineageMappings(mappings []*repository.FolderOutputMapping) []*repository.FolderOutputMapping {
	cloned := make([]*repository.FolderOutputMapping, 0, len(mappings))
	for _, mapping := range mappings {
		if mapping == nil {
			continue
		}
		cloned = append(cloned, mapping)
	}

	sort.SliceStable(cloned, func(i, j int) bool {
		if cloned[i].CreatedAt.Equal(cloned[j].CreatedAt) {
			leftID := strings.TrimSpace(cloned[i].ID)
			rightID := strings.TrimSpace(cloned[j].ID)
			if leftID == rightID {
				return normalizeLineagePath(cloned[i].OutputPath) < normalizeLineagePath(cloned[j].OutputPath)
			}
			return leftID < rightID
		}
		return cloned[i].CreatedAt.Before(cloned[j].CreatedAt)
	})

	return cloned
}

func resolveLineageFlowSourceFileIDs(
	sourceFiles []FolderLineageFlowSourceFile,
	sourceFileIDByPath map[string]string,
	sourceFileIDByRelativePath map[string]string,
	artifactSourceFileIDsByPath map[string][]string,
	mapping *repository.FolderOutputMapping,
) ([]string, bool) {
	if mapping == nil {
		return nil, false
	}

	normalizedSourcePath := normalizeLineagePath(mapping.SourcePath)
	if sourceFileID := sourceFileIDByPath[normalizedSourcePath]; sourceFileID != "" {
		return []string{sourceFileID}, true
	}

	normalizedRelativePath := normalizeLineagePath(mapping.SourceRelativePath)
	if sourceFileID := sourceFileIDByRelativePath[normalizedRelativePath]; sourceFileID != "" {
		return []string{sourceFileID}, false
	}

	if sourceFileIDs := artifactSourceFileIDsByPath[normalizedSourcePath]; len(sourceFileIDs) > 0 {
		return sourceFileIDs, false
	}

	if sourceFileIDs := resolveLineageFlowInternalStageSourceFileIDs(
		sourceFileIDByPath,
		sourceFileIDByRelativePath,
		normalizedSourcePath,
		normalizedRelativePath,
	); len(sourceFileIDs) > 0 {
		return sourceFileIDs, false
	}

	if normalizedSourcePath == "" {
		return nil, false
	}

	descendantIDs := collectLineageDescendantSourceFileIDs(sourceFiles, normalizedSourcePath)
	if len(descendantIDs) == 0 {
		return nil, false
	}

	return descendantIDs, false
}

func resolveLineageFlowInternalStageSourceFileIDs(
	sourceFileIDByPath map[string]string,
	sourceFileIDByRelativePath map[string]string,
	sourcePaths ...string,
) []string {
	for _, sourcePath := range sourcePaths {
		for _, candidate := range lineageFlowInternalStageSourceCandidates(sourcePath) {
			if sourceFileID := sourceFileIDByPath[candidate]; sourceFileID != "" {
				return []string{sourceFileID}
			}
			if sourceFileID := sourceFileIDByRelativePath[candidate]; sourceFileID != "" {
				return []string{sourceFileID}
			}
		}
	}
	return nil
}

func lineageFlowInternalStageSourceCandidates(sourcePath string) []string {
	normalizedPath := normalizeLineagePath(sourcePath)
	if normalizedPath == "" {
		return nil
	}

	parts := strings.Split(normalizedPath, "/")
	candidates := make([]string, 0, 2)
	for index, part := range parts {
		if !mixedLeafRouterIsInternalStagingDir(part) || index >= len(parts)-1 {
			continue
		}

		stripped := make([]string, 0, len(parts)-1)
		stripped = append(stripped, parts[:index]...)
		stripped = append(stripped, parts[index+1:]...)
		candidates = append(candidates, strings.Join(stripped, "/"))

		fileName := parts[len(parts)-1]
		if strings.Contains(fileName, "__") {
			decoded := append([]string{}, parts[:index]...)
			decoded = append(decoded, strings.Split(fileName, "__")...)
			candidates = append(candidates, strings.Join(decoded, "/"))
		}
	}

	return candidates
}

func collectLineageDescendantSourceFileIDs(
	sourceFiles []FolderLineageFlowSourceFile,
	containerPath string,
) []string {
	prefix := strings.TrimSuffix(containerPath, "/") + "/"
	descendants := make([]string, 0, len(sourceFiles))
	for _, sourceFile := range sourceFiles {
		if strings.HasPrefix(sourceFile.Path, prefix) {
			descendants = append(descendants, sourceFile.ID)
		}
	}
	return descendants
}

func mergeLineageFlowSourceFileIDs(
	existing []string,
	incoming []string,
	sourceFileOrderByID map[string]int,
) []string {
	if len(existing) == 0 {
		return append([]string(nil), incoming...)
	}

	merged := make([]string, 0, len(existing)+len(incoming))
	seen := make(map[string]struct{}, len(existing)+len(incoming))
	for _, sourceFileID := range existing {
		if strings.TrimSpace(sourceFileID) == "" {
			continue
		}
		if _, ok := seen[sourceFileID]; ok {
			continue
		}
		seen[sourceFileID] = struct{}{}
		merged = append(merged, sourceFileID)
	}
	for _, sourceFileID := range incoming {
		if strings.TrimSpace(sourceFileID) == "" {
			continue
		}
		if _, ok := seen[sourceFileID]; ok {
			continue
		}
		seen[sourceFileID] = struct{}{}
		merged = append(merged, sourceFileID)
	}

	sort.Slice(merged, func(i, j int) bool {
		leftOrder, leftOK := sourceFileOrderByID[merged[i]]
		rightOrder, rightOK := sourceFileOrderByID[merged[j]]
		if leftOK && rightOK && leftOrder != rightOrder {
			return leftOrder < rightOrder
		}
		return merged[i] < merged[j]
	})

	return merged
}

func buildLineageFlowLinkID(base string, idx int, total int) string {
	if total <= 1 {
		return base
	}
	return base + "-" + strconv.Itoa(idx+1)
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
	folderIDs []string,
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
		seenAuditIDs := map[string]struct{}{}
		for _, folderID := range folderIDs {
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
				auditID := strings.TrimSpace(log.ID)
				if _, ok := seenAuditIDs[auditID]; ok {
					continue
				}
				seenAuditIDs[auditID] = struct{}{}
				description := strings.TrimSpace(log.ErrorMsg)
				if description == "" {
					description = strings.TrimSpace(log.Action)
				}
				events = append(events, FolderLineageTimelineEvent{
					ID:            "audit-" + auditID,
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
