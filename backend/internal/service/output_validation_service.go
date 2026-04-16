package service

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

type OutputValidationService struct {
	fs           fs.FSAdapter
	folders      repository.FolderRepository
	config       repository.ConfigRepository
	manifests    repository.SourceManifestRepository
	mappings     repository.OutputMappingRepository
	outputChecks repository.OutputCheckRepository
}

func NewOutputValidationService(
	fsAdapter fs.FSAdapter,
	folderRepo repository.FolderRepository,
	configRepo repository.ConfigRepository,
	manifestRepo repository.SourceManifestRepository,
	mappingRepo repository.OutputMappingRepository,
	outputCheckRepo repository.OutputCheckRepository,
) *OutputValidationService {
	return &OutputValidationService{
		fs:           fsAdapter,
		folders:      folderRepo,
		config:       configRepo,
		manifests:    manifestRepo,
		mappings:     mappingRepo,
		outputChecks: outputCheckRepo,
	}
}

func (s *OutputValidationService) ValidateWorkflowRun(ctx context.Context, workflowRunID string) ([]*repository.FolderOutputCheck, error) {
	mappings, err := s.mappings.ListByWorkflowRunID(ctx, workflowRunID)
	if err != nil {
		return nil, fmt.Errorf("outputValidation.ValidateWorkflowRun list mappings for workflow run %q: %w", workflowRunID, err)
	}

	grouped := make(map[string][]*repository.FolderOutputMapping, 8)
	for _, mapping := range mappings {
		if mapping == nil {
			continue
		}
		folderID := strings.TrimSpace(mapping.FolderID)
		if folderID == "" {
			continue
		}
		grouped[folderID] = append(grouped[folderID], mapping)
	}

	folderIDs := make([]string, 0, len(grouped))
	for folderID := range grouped {
		folderIDs = append(folderIDs, folderID)
	}
	sort.Strings(folderIDs)

	checks := make([]*repository.FolderOutputCheck, 0, len(folderIDs))
	for _, folderID := range folderIDs {
		check, err := s.validateAndPersistCheck(ctx, folderID, workflowRunID, grouped[folderID])
		if err != nil {
			return nil, err
		}
		checks = append(checks, check)
	}

	return checks, nil
}

func (s *OutputValidationService) ValidateFolder(ctx context.Context, folderID string) (*repository.FolderOutputCheck, error) {
	workflowRunID, err := s.mappings.GetLatestWorkflowRunIDByFolderID(ctx, folderID)
	if err != nil {
		return nil, fmt.Errorf("outputValidation.ValidateFolder latest workflow run for folder %q: %w", folderID, err)
	}

	mappings, err := s.mappings.ListByWorkflowRunAndFolderID(ctx, workflowRunID, folderID)
	if err != nil {
		return nil, fmt.Errorf("outputValidation.ValidateFolder list mappings for folder %q: %w", folderID, err)
	}

	check, err := s.validateAndPersistCheck(ctx, folderID, workflowRunID, mappings)
	if err != nil {
		return nil, err
	}

	return check, nil
}

func (s *OutputValidationService) GetLatestDetail(ctx context.Context, folderID string) (*FolderOutputCheckDetail, error) {
	folder, err := s.folders.GetByID(ctx, folderID)
	if err != nil {
		return nil, fmt.Errorf("outputValidation.GetLatestDetail get folder %q: %w", folderID, err)
	}

	detail := &FolderOutputCheckDetail{
		FolderID:    folderID,
		Summary:     folder.OutputCheckSummary,
		Mappings:    []*repository.FolderOutputMapping{},
		Errors:      []repository.OutputCheckError{},
		SourceFiles: []*repository.FolderSourceManifest{},
	}

	check, err := s.outputChecks.GetLatestByFolderID(ctx, folderID)
	if err != nil {
		if !errors.Is(err, repository.ErrNotFound) {
			return nil, fmt.Errorf("outputValidation.GetLatestDetail latest check for folder %q: %w", folderID, err)
		}
		manifests, manifestErr := s.manifests.ListLatestByFolderID(ctx, folderID)
		if manifestErr != nil {
			return nil, fmt.Errorf("outputValidation.GetLatestDetail list fallback manifests for folder %q: %w", folderID, manifestErr)
		}
		detail.SourceFiles = manifests
		return detail, nil
	}
	detail.Check = check
	detail.Errors = append([]repository.OutputCheckError(nil), check.Errors...)
	detail.Summary = repository.FolderOutputCheckSummary{
		Status:        check.Status,
		WorkflowRunID: check.WorkflowRunID,
		CheckedAt:     &check.CheckedAt,
		MismatchCount: check.MismatchCount,
		FailedFiles:   append([]string(nil), check.FailedFiles...),
	}

	if strings.TrimSpace(check.WorkflowRunID) != "" {
		mappings, err := s.mappings.ListByWorkflowRunAndFolderID(ctx, check.WorkflowRunID, folderID)
		if err != nil {
			return nil, fmt.Errorf("outputValidation.GetLatestDetail list mappings for folder %q: %w", folderID, err)
		}
		detail.Mappings = mappings

		manifests, manifestErr := s.manifests.ListByWorkflowRunAndFolderID(ctx, check.WorkflowRunID, folderID)
		if manifestErr != nil {
			return nil, fmt.Errorf("outputValidation.GetLatestDetail list run-bound manifests for folder %q: %w", folderID, manifestErr)
		}
		detail.SourceFiles = manifests
	}
	if len(detail.SourceFiles) == 0 {
		manifests, manifestErr := s.manifests.ListLatestByFolderID(ctx, folderID)
		if manifestErr != nil {
			return nil, fmt.Errorf("outputValidation.GetLatestDetail list fallback manifests for folder %q: %w", folderID, manifestErr)
		}
		detail.SourceFiles = manifests
	}

	return detail, nil
}

func (s *OutputValidationService) CanMarkDone(ctx context.Context, folderID string) (bool, error) {
	folder, err := s.folders.GetByID(ctx, folderID)
	if err != nil {
		return false, fmt.Errorf("outputValidation.CanMarkDone get folder %q: %w", folderID, err)
	}
	if strings.EqualFold(strings.TrimSpace(folder.OutputCheckSummary.Status), "passed") {
		return true, nil
	}

	check, validateErr := s.ValidateFolder(ctx, folderID)
	if validateErr != nil {
		if errors.Is(validateErr, repository.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("outputValidation.CanMarkDone validate folder %q: %w", folderID, validateErr)
	}

	return check != nil && strings.EqualFold(strings.TrimSpace(check.Status), "passed"), nil
}

func (s *OutputValidationService) validateAndPersistCheck(
	ctx context.Context,
	folderID string,
	workflowRunID string,
	mappings []*repository.FolderOutputMapping,
) (*repository.FolderOutputCheck, error) {
	folder, err := s.folders.GetByID(ctx, folderID)
	if err != nil {
		return nil, fmt.Errorf("outputValidation.validateAndPersistCheck get folder %q: %w", folderID, err)
	}

	manifests, err := s.manifests.ListByWorkflowRunAndFolderID(ctx, workflowRunID, folderID)
	if err != nil {
		return nil, fmt.Errorf("outputValidation.validateAndPersistCheck list run-bound manifests for folder %q: %w", folderID, err)
	}
	if len(manifests) == 0 {
		return nil, fmt.Errorf("manifest_snapshot_missing: folder %q has no run-bound source manifest for workflow run %q", folderID, workflowRunID)
	}

	appConfig, err := s.config.GetAppConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("outputValidation.validateAndPersistCheck get app config: %w", err)
	}

	allowedOutputDirs := outputValidationAllowedDirs(appConfig)
	allowedOutputDirSet := make(map[string]struct{}, len(allowedOutputDirs))
	for _, outputDir := range allowedOutputDirs {
		allowedOutputDirSet[outputValidationNormalizeForCompare(outputDir)] = struct{}{}
	}

	errorsList := make([]repository.OutputCheckError, 0, 16)
	addError := func(code, message, sourcePath, outputPath, nodeType string) {
		errorsList = append(errorsList, repository.OutputCheckError{
			Code:       strings.TrimSpace(code),
			Message:    strings.TrimSpace(message),
			SourcePath: normalizeWorkflowPath(sourcePath),
			OutputPath: normalizeWorkflowPath(outputPath),
			NodeType:   strings.TrimSpace(nodeType),
		})
	}

	if len(mappings) == 0 {
		addError("mapping_missing", "no output mappings found for folder", "", "", "")
	}

	for _, manifest := range manifests {
		if manifest == nil {
			continue
		}
		sourcePath := normalizeWorkflowPath(manifest.SourcePath)
		sourceMappings := outputValidationResolveSourceMappings(manifest, mappings)
		if len(sourceMappings) == 0 {
			addError("source_unmapped", "source file is not mapped to output", sourcePath, "", "")
			continue
		}

		mappedAnyExisting := false
		for _, candidate := range sourceMappings {
			outputPath := normalizeWorkflowPath(candidate.outputPath)
			mapping := candidate.mapping
			if outputPath == "" {
				addError("output_missing_path", "mapped output path is empty", sourcePath, "", mapping.NodeType)
				continue
			}
			exists, existsErr := s.fs.Exists(ctx, outputPath)
			if existsErr != nil {
				return nil, fmt.Errorf("outputValidation.validateAndPersistCheck check output path %q: %w", outputPath, existsErr)
			}
			if exists {
				mappedAnyExisting = true
			} else {
				addError("output_not_found", "mapped output file does not exist", sourcePath, outputPath, mapping.NodeType)
			}
			if !outputValidationPathAllowed(outputPath, allowedOutputDirSet) {
				addError("output_dir_mismatch", "mapped output path is outside configured output directories", sourcePath, outputPath, mapping.NodeType)
			}
		}
		if !mappedAnyExisting {
			addError("source_not_resolved", "source file has no existing output artifact", sourcePath, "", "")
		}
	}

	for _, mapping := range mappings {
		if mapping == nil || !mapping.RequiredArtifact {
			continue
		}
		outputPath := outputValidationResolveFinalOutputPath(mapping.OutputPath, mappings)
		if outputPath == "" {
			addError("required_artifact_missing_path", "required artifact output path is empty", mapping.SourcePath, "", mapping.NodeType)
			continue
		}
		exists, existsErr := s.fs.Exists(ctx, outputPath)
		if existsErr != nil {
			return nil, fmt.Errorf("outputValidation.validateAndPersistCheck check required artifact path %q: %w", outputPath, existsErr)
		}
		if !exists {
			addError("required_artifact_missing", "required artifact does not exist", mapping.SourcePath, outputPath, mapping.NodeType)
		}
	}

	failedFiles := outputValidationFailedFiles(folder, errorsList)
	status := "passed"
	if len(errorsList) > 0 {
		status = "failed"
	}

	checkedAt := time.Now().UTC()
	check := &repository.FolderOutputCheck{
		ID:            uuid.NewString(),
		FolderID:      folderID,
		WorkflowRunID: strings.TrimSpace(workflowRunID),
		Status:        status,
		MismatchCount: len(errorsList),
		FailedFiles:   failedFiles,
		Errors:        errorsList,
		CheckedAt:     checkedAt,
		CreatedAt:     checkedAt,
	}
	if err := s.outputChecks.Create(ctx, check); err != nil {
		return nil, fmt.Errorf("outputValidation.validateAndPersistCheck create check for folder %q: %w", folderID, err)
	}
	if err := s.outputChecks.UpdateFolderSummary(ctx, folderID, repository.FolderOutputCheckSummary{
		Status:        check.Status,
		WorkflowRunID: check.WorkflowRunID,
		CheckedAt:     &check.CheckedAt,
		MismatchCount: check.MismatchCount,
		FailedFiles:   check.FailedFiles,
	}); err != nil {
		return nil, fmt.Errorf("outputValidation.validateAndPersistCheck update summary for folder %q: %w", folderID, err)
	}

	return check, nil
}

type outputValidationResolvedMapping struct {
	mapping    *repository.FolderOutputMapping
	outputPath string
}

func outputValidationResolveSourceMappings(
	manifest *repository.FolderSourceManifest,
	mappings []*repository.FolderOutputMapping,
) []outputValidationResolvedMapping {
	if manifest == nil {
		return nil
	}

	sourcePath := normalizeWorkflowPath(manifest.SourcePath)
	if sourcePath == "" {
		return nil
	}

	resolved := make([]outputValidationResolvedMapping, 0, len(mappings))
	for _, mapping := range mappings {
		if !outputValidationCanResolveSourceMapping(mapping) {
			continue
		}

		mappingSourcePath := normalizeWorkflowPath(mapping.SourcePath)
		if mappingSourcePath == "" {
			continue
		}

		if mappingSourcePath == sourcePath {
			resolved = append(resolved, outputValidationResolvedMapping{
				mapping:    mapping,
				outputPath: outputValidationResolveFinalOutputPath(mapping.OutputPath, mappings),
			})
			continue
		}

		if outputValidationIsArchiveMapping(mapping) {
			if outputValidationHasWorkflowPrefix(sourcePath, mappingSourcePath) {
				resolved = append(resolved, outputValidationResolvedMapping{
					mapping:    mapping,
					outputPath: outputValidationResolveFinalOutputPath(mapping.OutputPath, mappings),
				})
			}
			continue
		}

		relativePath, ok := outputValidationManifestRelativePath(manifest, mappingSourcePath, sourcePath)
		if !ok {
			continue
		}

		resolved = append(resolved, outputValidationResolvedMapping{
			mapping:    mapping,
			outputPath: outputValidationResolveFinalOutputPath(joinWorkflowPath(mapping.OutputPath, relativePath), mappings),
		})
	}

	return resolved
}

func outputValidationCanResolveSourceMapping(mapping *repository.FolderOutputMapping) bool {
	if mapping == nil {
		return false
	}

	artifactType := strings.ToLower(strings.TrimSpace(mapping.ArtifactType))
	return artifactType == "" || artifactType == "primary" || artifactType == "rename" || artifactType == "archive"
}

func outputValidationIsArchiveMapping(mapping *repository.FolderOutputMapping) bool {
	if mapping == nil {
		return false
	}

	return strings.EqualFold(strings.TrimSpace(mapping.ArtifactType), "archive")
}

func outputValidationResolveFinalOutputPath(outputPath string, mappings []*repository.FolderOutputMapping) string {
	current := normalizeWorkflowPath(outputPath)
	if current == "" {
		return ""
	}

	seen := map[string]struct{}{}
	for {
		key := outputValidationNormalizeForCompare(current)
		if key == "" {
			return current
		}
		if _, exists := seen[key]; exists {
			return current
		}
		seen[key] = struct{}{}

		next := ""
		for _, mapping := range mappings {
			if mapping == nil {
				continue
			}
			if outputValidationNormalizeForCompare(mapping.SourcePath) != key {
				continue
			}
			candidate := normalizeWorkflowPath(mapping.OutputPath)
			if candidate == "" || outputValidationNormalizeForCompare(candidate) == key {
				continue
			}
			next = candidate
			break
		}
		if next == "" {
			return current
		}
		current = next
	}
}

func outputValidationManifestRelativePath(
	manifest *repository.FolderSourceManifest,
	mappingSourcePath string,
	sourcePath string,
) (string, bool) {
	if manifest == nil {
		return "", false
	}

	manifestRelativePath := normalizeWorkflowPath(manifest.RelativePath)
	if manifestRelativePath != "" && manifestRelativePath != "." {
		if outputValidationHasWorkflowPrefix(sourcePath, mappingSourcePath) {
			return manifestRelativePath, true
		}
	}

	if sourcePath == mappingSourcePath {
		return "", false
	}
	if !outputValidationHasWorkflowPrefix(sourcePath, mappingSourcePath) {
		return "", false
	}

	relativePath, err := filepath.Rel(mappingSourcePath, sourcePath)
	if err != nil {
		return "", false
	}
	relativePath = normalizeWorkflowPath(relativePath)
	if relativePath == "" || relativePath == "." || strings.HasPrefix(relativePath, "..") {
		return "", false
	}

	return relativePath, true
}

func outputValidationAllowedDirs(config *repository.AppConfig) []string {
	if config == nil {
		return nil
	}

	out := make([]string, 0,
		len(config.OutputDirs.Video)+
			len(config.OutputDirs.Photo)+
			len(config.OutputDirs.Manga)+
			len(config.OutputDirs.Mixed)+
			len(config.OutputDirs.Other),
	)
	out = append(out, config.OutputDirs.Video...)
	out = append(out, config.OutputDirs.Photo...)
	out = append(out, config.OutputDirs.Manga...)
	out = append(out, config.OutputDirs.Mixed...)
	out = append(out, config.OutputDirs.Other...)
	return out
}

func outputValidationPathAllowed(outputPath string, allowedOutputDirSet map[string]struct{}) bool {
	if len(allowedOutputDirSet) == 0 {
		return true
	}
	normalizedPath := outputValidationNormalizeForCompare(outputPath)
	if normalizedPath == "" {
		return false
	}
	for allowedDir := range allowedOutputDirSet {
		if allowedDir == "" {
			continue
		}
		if outputValidationHasWorkflowPrefix(normalizedPath, allowedDir) {
			return true
		}
	}
	return false
}

func outputValidationNormalizeForCompare(path string) string {
	normalized := normalizeWorkflowPath(path)
	if normalized == "" {
		return ""
	}
	normalized = strings.ReplaceAll(normalized, `\`, "/")
	return strings.ToLower(normalized)
}

func outputValidationHasWorkflowPrefix(path string, prefix string) bool {
	normalizedPath := outputValidationNormalizeForCompare(path)
	normalizedPrefix := outputValidationNormalizeForCompare(prefix)
	if normalizedPath == "" || normalizedPrefix == "" {
		return false
	}
	return normalizedPath == normalizedPrefix || strings.HasPrefix(normalizedPath, normalizedPrefix+"/")
}

func outputValidationFailedFiles(folder *repository.Folder, errorsList []repository.OutputCheckError) []string {
	if len(errorsList) == 0 {
		return []string{}
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(errorsList))
	rootPath := ""
	if folder != nil {
		rootPath = normalizeWorkflowPath(folder.Path)
	}
	for _, item := range errorsList {
		sourcePath := normalizeWorkflowPath(item.SourcePath)
		if sourcePath == "" {
			continue
		}
		displayPath := sourcePath
		if rootPath != "" {
			relativePath, err := filepath.Rel(rootPath, sourcePath)
			if err == nil && strings.TrimSpace(relativePath) != "" && relativePath != "." && !strings.HasPrefix(relativePath, "..") {
				displayPath = normalizeWorkflowPath(relativePath)
			}
		}
		if _, exists := seen[displayPath]; exists {
			continue
		}
		seen[displayPath] = struct{}{}
		out = append(out, displayPath)
	}
	sort.Strings(out)
	return out
}
