package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/sse"
)

type ScanInput struct {
	JobID       string
	SourceDirs  []string
	ExcludeDirs []string
}

type ScanSnapshotRecorder interface {
	CreateBefore(ctx context.Context, jobID, folderID, operationType string) (string, error)
	CreateBeforeWithState(ctx context.Context, jobID, folderID, operationType string, before json.RawMessage) (string, error)
	CommitAfter(ctx context.Context, snapshotID string, after json.RawMessage) error
	UpdateDetail(ctx context.Context, snapshotID string, detail json.RawMessage) error
}

type ScannerService struct {
	fs        fs.FSAdapter
	folders   repository.FolderRepository
	jobs      repository.JobRepository
	snapshots ScanSnapshotRecorder
	manifests SourceManifestBuilder
	audit     AuditWriter
	broker    *sse.Broker
}

type scanTarget struct {
	sourceDir    string
	folderPath   string
	folderName   string
	relativePath string
}

type scanMetrics struct {
	fileNames      []string
	identityItems  []identityFingerprintItem
	totalSize      int64
	totalFiles     int
	imageCount     int
	videoCount     int
	otherFileCount int
	hasOtherFiles  bool
}

type identityFingerprintItem struct {
	relativePath string
	sizeBytes    int64
}

func NewScannerService(
	fsAdapter fs.FSAdapter,
	folderRepo repository.FolderRepository,
	jobRepo repository.JobRepository,
	snapshots ScanSnapshotRecorder,
	audit AuditWriter,
	broker *sse.Broker,
) *ScannerService {
	return &ScannerService{
		fs:        fsAdapter,
		folders:   folderRepo,
		jobs:      jobRepo,
		snapshots: snapshots,
		audit:     audit,
		broker:    broker,
	}
}

func (s *ScannerService) SetSourceManifestBuilder(builder SourceManifestBuilder) {
	s.manifests = builder
}

func (s *ScannerService) Scan(ctx context.Context, input ScanInput) (int, error) {
	sourceDirs := normalizeSourceDirs(input.SourceDirs)
	if len(sourceDirs) == 0 {
		return 0, fmt.Errorf("scanner.Scan: source dirs are required")
	}
	excludeDirs := normalizeScanPaths(input.ExcludeDirs)
	if err := s.suppressExcludedTargets(ctx, excludeDirs); err != nil {
		return 0, fmt.Errorf("scanner.Scan suppress excluded targets: %w", err)
	}

	targets, err := s.discoverTargets(ctx, sourceDirs, excludeDirs)
	if err != nil {
		if input.JobID != "" && s.jobs != nil {
			_ = s.jobs.UpdateStatus(ctx, input.JobID, "failed", err.Error())
		}
		s.publish("scan.failed", map[string]any{
			"job_id": input.JobID,
			"error":  err.Error(),
		})
		return 0, err
	}

	total := len(targets)
	if input.JobID != "" && s.jobs != nil {
		if err := s.jobs.UpdateStatus(ctx, input.JobID, "running", ""); err != nil {
			return 0, fmt.Errorf("scanner.Scan start job %q: %w", input.JobID, err)
		}
		if err := s.jobs.UpdateTotal(ctx, input.JobID, total); err != nil {
			return 0, fmt.Errorf("scanner.Scan set total for job %q: %w", input.JobID, err)
		}
	}

	s.publish("scan.started", map[string]any{
		"job_id":      input.JobID,
		"source_dirs": sourceDirs,
		"total":       total,
	})

	processed := 0
	failed := 0
	for index, target := range targets {
		folder, scanErr := s.scanOne(ctx, input.JobID, target, excludeDirs)
		if scanErr != nil {
			failed++
			existingFolder := s.resolveScanErrorFolder(ctx, target)
			if input.JobID != "" && s.jobs != nil {
				_ = s.jobs.IncrementProgress(ctx, input.JobID, 0, 1)
			}
			s.publish("scan.error", map[string]any{
				"job_id":        input.JobID,
				"folder_id":     folderIDForScanError(existingFolder, target),
				"folder_name":   target.folderName,
				"folder_path":   target.folderPath,
				"source_dir":    target.sourceDir,
				"relative_path": target.relativePath,
				"done":          index + 1,
				"total":         total,
				"error":         scanErr.Error(),
			})
			s.publishFolderClassificationScanError(input.JobID, target, existingFolder, scanErr.Error())
			continue
		}

		processed++
		if input.JobID != "" && s.jobs != nil {
			if err := s.jobs.IncrementProgress(ctx, input.JobID, 1, 0); err != nil {
				return processed, fmt.Errorf("scanner.Scan update progress for job %q: %w", input.JobID, err)
			}
		}

		payload := map[string]any{
			"job_id":        input.JobID,
			"folder_id":     folder.ID,
			"folder_name":   folder.Name,
			"folder_path":   folder.Path,
			"source_dir":    folder.SourceDir,
			"relative_path": folder.RelativePath,
			"category":      folder.Category,
			"done":          index + 1,
			"total":         total,
		}
		s.publish("scan.progress", payload)
		s.publish("job.progress", payload)
		s.publishFolderClassificationUpdated(input.JobID, folder, "scanning", "", "", "")
	}

	status := "succeeded"
	if failed > 0 {
		status = "partial"
	}
	if input.JobID != "" && s.jobs != nil {
		if err := s.jobs.UpdateStatus(ctx, input.JobID, status, ""); err != nil {
			return processed, fmt.Errorf("scanner.Scan finish job %q: %w", input.JobID, err)
		}
	}

	completion := map[string]any{
		"job_id":    input.JobID,
		"status":    status,
		"processed": processed,
		"failed":    failed,
		"total":     total,
	}
	completion = attachJobTiming(ctx, s.jobs, input.JobID, completion)
	s.publish("scan.done", completion)
	s.publish("job.done", completion)

	return processed, nil
}

func (s *ScannerService) suppressExcludedTargets(ctx context.Context, excludeDirs []string) error {
	if s.folders == nil || len(excludeDirs) == 0 {
		return nil
	}

	seenFolderIDs := make(map[string]struct{})
	for _, excludedDir := range excludeDirs {
		folders, err := s.folders.ListByPathPrefix(ctx, excludedDir)
		if err != nil {
			return fmt.Errorf("list excluded path prefix %q: %w", excludedDir, err)
		}

		for _, folder := range folders {
			if folder == nil || folder.DeletedAt != nil {
				continue
			}
			if _, ok := seenFolderIDs[folder.ID]; ok {
				continue
			}
			seenFolderIDs[folder.ID] = struct{}{}
			if err := s.folders.Suppress(ctx, folder.ID, "", ""); err != nil {
				return fmt.Errorf("suppress excluded folder %q: %w", folder.ID, err)
			}
		}
	}

	return nil
}

func (s *ScannerService) discoverTargets(ctx context.Context, sourceDirs []string, excludeDirs []string) ([]scanTarget, error) {
	targets := make([]scanTarget, 0)
	for _, sourceDir := range sourceDirs {
		entries, err := s.fs.ReadDir(ctx, sourceDir)
		if err != nil {
			return nil, fmt.Errorf("scanner.discoverTargets read source directory %q: %w", sourceDir, err)
		}

		for _, entry := range entries {
			if !entry.IsDir {
				continue
			}

			folderPath := filepath.Join(sourceDir, entry.Name)
			if isExcludedScanPath(folderPath, excludeDirs) {
				continue
			}
			isSuppressed, err := s.folders.IsSuppressedPath(ctx, folderPath)
			if err != nil {
				return nil, fmt.Errorf("scanner.discoverTargets check suppressed path %q: %w", folderPath, err)
			}
			if isSuppressed {
				continue
			}

			targets = append(targets, scanTarget{
				sourceDir:    sourceDir,
				folderPath:   folderPath,
				folderName:   entry.Name,
				relativePath: entry.Name,
			})
		}
	}

	return targets, nil
}

func (s *ScannerService) scanOne(ctx context.Context, jobID string, target scanTarget, excludeDirs []string) (*repository.Folder, error) {
	metrics, err := s.collectFolderMetrics(ctx, target.folderPath, excludeDirs)
	if err != nil {
		s.writeScanAudit(ctx, jobID, "", target.folderPath, target.sourceDir, target.relativePath, "", "failed", "", err)
		return nil, err
	}
	category := Classify(target.folderName, metrics.fileNames)
	now := time.Now().UTC()

	existing, matchType, err := s.folders.ResolveScanTarget(ctx, target.folderPath, target.sourceDir, target.relativePath)
	if err != nil {
		s.writeScanAudit(ctx, jobID, "", target.folderPath, target.sourceDir, target.relativePath, "", "failed", category, err)
		return nil, fmt.Errorf("scanner.scanOne resolve target %q: %w", target.folderPath, err)
	}

	folderID := uuid.NewString()
	if existing != nil {
		folderID = existing.ID
	}
	identityFingerprint := buildIdentityFingerprint(metrics.identityItems)

	folder := &repository.Folder{
		ID:                  folderID,
		Path:                target.folderPath,
		SourceDir:           target.sourceDir,
		RelativePath:        target.relativePath,
		IdentityFingerprint: identityFingerprint,
		Name:                target.folderName,
		Category:            category,
		CategorySource:      "auto",
		Status:              "pending",
		ImageCount:          metrics.imageCount,
		VideoCount:          metrics.videoCount,
		OtherFileCount:      metrics.otherFileCount,
		HasOtherFiles:       metrics.hasOtherFiles,
		TotalFiles:          metrics.totalFiles,
		TotalSize:           metrics.totalSize,
		MarkedForMove:       false,
		ScannedAt:           now,
		UpdatedAt:           now,
	}
	if existing != nil {
		folder.DeletedAt = existing.DeletedAt
		folder.DeleteStagingPath = existing.DeleteStagingPath
		folder.CoverImagePath = existing.CoverImagePath
	}

	afterStateJSON, err := marshalClassificationSnapshotState(folder)
	if err != nil {
		return nil, fmt.Errorf("scanner.scanOne marshal after state for %q: %w", target.folderPath, err)
	}
	snapshotID := ""
	if s.snapshots != nil {
		beforeStateJSON, marshalErr := marshalClassificationSnapshotState(existing)
		if marshalErr != nil {
			return nil, fmt.Errorf("scanner.scanOne marshal before state for %q: %w", target.folderPath, marshalErr)
		}
		snapshotID, err = s.snapshots.CreateBeforeWithState(ctx, jobID, folder.ID, "classify", beforeStateJSON)
		if err != nil {
			return nil, fmt.Errorf("scanner.scanOne create snapshot for %q: %w", target.folderPath, err)
		}
	}

	if err := s.folders.Upsert(ctx, folder); err != nil {
		s.writeScanAudit(ctx, jobID, folder.ID, target.folderPath, target.sourceDir, target.relativePath, string(matchType), "failed", category, err)
		return nil, fmt.Errorf("scanner.scanOne upsert %q: %w", target.folderPath, err)
	}

	if err := s.recordClassificationSnapshot(ctx, snapshotID, matchType, existing, folder, afterStateJSON); err != nil {
		return nil, err
	}
	if s.manifests != nil {
		if err := s.manifests.Build(ctx, folder.ID); err != nil {
			return nil, fmt.Errorf("scanner.scanOne build source manifest for %q: %w", folder.ID, err)
		}
	}

	s.writeScanAudit(ctx, jobID, folder.ID, folder.Path, folder.SourceDir, folder.RelativePath, string(matchType), "success", folder.Category, nil)
	return folder, nil
}

func (s *ScannerService) collectFolderMetrics(ctx context.Context, folderPath string, excludeDirs []string) (scanMetrics, error) {
	return s.collectFolderMetricsWithRoot(ctx, folderPath, folderPath, excludeDirs)
}

func (s *ScannerService) collectFolderMetricsWithRoot(ctx context.Context, rootPath, currentPath string, excludeDirs []string) (scanMetrics, error) {
	entries, err := s.fs.ReadDir(ctx, currentPath)
	if err != nil {
		return scanMetrics{}, fmt.Errorf("scanner.collectFolderMetrics read folder %q: %w", currentPath, err)
	}

	result := scanMetrics{
		fileNames: make([]string, 0, len(entries)),
	}

	for _, entry := range entries {
		childPath := filepath.Join(currentPath, entry.Name)
		if entry.IsDir {
			if isExcludedScanPath(childPath, excludeDirs) {
				continue
			}
			nested, nestedErr := s.collectFolderMetricsWithRoot(ctx, rootPath, childPath, excludeDirs)
			if nestedErr != nil {
				return scanMetrics{}, nestedErr
			}
			result.totalFiles += nested.totalFiles
			result.totalSize += nested.totalSize
			result.imageCount += nested.imageCount
			result.videoCount += nested.videoCount
			result.otherFileCount += nested.otherFileCount
			result.hasOtherFiles = result.hasOtherFiles || nested.hasOtherFiles
			result.fileNames = append(result.fileNames, nested.fileNames...)
			result.identityItems = append(result.identityItems, nested.identityItems...)
			continue
		}
		info, statErr := s.fs.Stat(ctx, childPath)
		if statErr != nil {
			return scanMetrics{}, fmt.Errorf("scanner.collectFolderMetrics stat %q: %w", childPath, statErr)
		}

		result.totalFiles++
		result.totalSize += info.Size
		result.fileNames = append(result.fileNames, entry.Name)
		relativePath, relErr := filepath.Rel(rootPath, childPath)
		if relErr != nil {
			return scanMetrics{}, fmt.Errorf("scanner.collectFolderMetrics relative path %q from %q: %w", childPath, rootPath, relErr)
		}
		result.identityItems = append(result.identityItems, identityFingerprintItem{
			relativePath: normalizeFingerprintRelativePath(relativePath),
			sizeBytes:    info.Size,
		})
		ext := strings.ToLower(filepath.Ext(entry.Name))
		switch {
		case imageExts[ext]:
			result.imageCount++
		case videoExts[ext]:
			result.videoCount++
		case mangaExts[ext]:
			// 漫画压缩包不算 other file。
		default:
			result.otherFileCount++
			result.hasOtherFiles = true
		}
	}

	return result, nil
}

type classificationSnapshotState struct {
	ID                  string `json:"id,omitempty"`
	Path                string `json:"path,omitempty"`
	SourceDir           string `json:"source_dir,omitempty"`
	RelativePath        string `json:"relative_path,omitempty"`
	IdentityFingerprint string `json:"identity_fingerprint,omitempty"`
	Category            string `json:"category,omitempty"`
	CategorySource      string `json:"category_source,omitempty"`
	Status              string `json:"status,omitempty"`
	TotalFiles          int    `json:"total_files,omitempty"`
	TotalSize           int64  `json:"total_size,omitempty"`
	ImageCount          int    `json:"image_count,omitempty"`
	VideoCount          int    `json:"video_count,omitempty"`
	OtherFileCount      int    `json:"other_file_count,omitempty"`
	HasOtherFiles       bool   `json:"has_other_files,omitempty"`
}

func marshalClassificationSnapshotState(folder *repository.Folder) (json.RawMessage, error) {
	if folder == nil {
		return json.RawMessage(`null`), nil
	}
	state := classificationSnapshotState{
		ID:                  folder.ID,
		Path:                folder.Path,
		SourceDir:           folder.SourceDir,
		RelativePath:        folder.RelativePath,
		IdentityFingerprint: folder.IdentityFingerprint,
		Category:            folder.Category,
		CategorySource:      folder.CategorySource,
		Status:              folder.Status,
		TotalFiles:          folder.TotalFiles,
		TotalSize:           folder.TotalSize,
		ImageCount:          folder.ImageCount,
		VideoCount:          folder.VideoCount,
		OtherFileCount:      folder.OtherFileCount,
		HasOtherFiles:       folder.HasOtherFiles,
	}
	payload, err := json.Marshal(state)
	if err != nil {
		return nil, err
	}
	return payload, nil
}

func buildIdentityFingerprint(items []identityFingerprintItem) string {
	if len(items) == 0 {
		return ""
	}

	sorted := make([]identityFingerprintItem, 0, len(items))
	for _, item := range items {
		relativePath := strings.TrimSpace(item.relativePath)
		if relativePath == "" {
			continue
		}
		sorted = append(sorted, identityFingerprintItem{
			relativePath: relativePath,
			sizeBytes:    item.sizeBytes,
		})
	}
	if len(sorted) == 0 {
		return ""
	}

	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].relativePath < sorted[j].relativePath
	})

	hasher := sha256.New()
	for _, item := range sorted {
		_, _ = hasher.Write([]byte(item.relativePath))
		_, _ = hasher.Write([]byte{0})
		_, _ = hasher.Write([]byte(strconv.FormatInt(item.sizeBytes, 10)))
		_, _ = hasher.Write([]byte{0})
	}

	return hex.EncodeToString(hasher.Sum(nil))
}

func normalizeFingerprintRelativePath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}
	return filepath.ToSlash(trimmed)
}

func (s *ScannerService) recordClassificationSnapshot(
	ctx context.Context,
	snapshotID string,
	matchType repository.FolderScanMatchType,
	beforeFolder *repository.Folder,
	afterFolder *repository.Folder,
	afterStateJSON json.RawMessage,
) error {
	if s.snapshots == nil || afterFolder == nil || strings.TrimSpace(snapshotID) == "" {
		return nil
	}

	detailJSON, err := json.Marshal(map[string]any{
		"match_type":             string(matchType),
		"identity_fingerprint":   afterFolder.IdentityFingerprint,
		"source_dir":             afterFolder.SourceDir,
		"relative_path":          afterFolder.RelativePath,
		"before_category":        snapshotCategory(beforeFolder),
		"before_category_source": snapshotCategorySource(beforeFolder),
		"after_category":         afterFolder.Category,
		"after_category_source":  afterFolder.CategorySource,
		"total_files":            afterFolder.TotalFiles,
		"total_size":             afterFolder.TotalSize,
		"image_count":            afterFolder.ImageCount,
		"video_count":            afterFolder.VideoCount,
		"other_file_count":       afterFolder.OtherFileCount,
		"has_other_files":        afterFolder.HasOtherFiles,
	})
	if err != nil {
		return fmt.Errorf("scanner.recordClassificationSnapshot marshal detail for folder %q: %w", afterFolder.ID, err)
	}

	if err := s.snapshots.UpdateDetail(ctx, snapshotID, detailJSON); err != nil {
		return fmt.Errorf("scanner.recordClassificationSnapshot update detail for folder %q: %w", afterFolder.ID, err)
	}

	if err := s.snapshots.CommitAfter(ctx, snapshotID, afterStateJSON); err != nil {
		return fmt.Errorf("scanner.recordClassificationSnapshot commit snapshot for folder %q: %w", afterFolder.ID, err)
	}

	return nil
}

func snapshotCategory(folder *repository.Folder) string {
	if folder == nil {
		return ""
	}
	return folder.Category
}

func snapshotCategorySource(folder *repository.Folder) string {
	if folder == nil {
		return ""
	}
	return folder.CategorySource
}

func (s *ScannerService) writeScanAudit(ctx context.Context, jobID, folderID, folderPath, sourceDir, relativePath, matchType, result, category string, scanErr error) {
	if s.audit == nil {
		return
	}

	detail, err := json.Marshal(map[string]any{
		"source_dir":    sourceDir,
		"relative_path": relativePath,
		"match_type":    matchType,
		"category":      category,
	})
	if err != nil {
		detail = nil
	}

	logItem := &repository.AuditLog{
		ID:         fmt.Sprintf("audit-scan-%s-%d", folderID, time.Now().UTC().UnixNano()),
		JobID:      jobID,
		FolderID:   folderID,
		FolderPath: folderPath,
		Action:     "scan",
		Level:      "info",
		Detail:     detail,
		Result:     result,
	}
	if scanErr != nil {
		logItem.Level = "error"
		logItem.ErrorMsg = scanErr.Error()
	}

	_ = s.audit.Write(ctx, logItem)
}

func (s *ScannerService) publish(eventType string, payload any) {
	if s.broker == nil {
		return
	}

	_ = s.broker.Publish(eventType, payload)
}

func (s *ScannerService) publishFolderClassificationUpdated(
	jobID string,
	folder *repository.Folder,
	classificationStatus string,
	nodeID string,
	nodeType string,
	errMsg string,
) {
	if s.broker == nil || folder == nil {
		return
	}

	updatedAt := time.Now().UTC()

	s.publish("folder.classification.updated", map[string]any{
		"folder_id":             folder.ID,
		"job_id":                strings.TrimSpace(jobID),
		"workflow_run_id":       "",
		"folder_name":           folder.Name,
		"folder_path":           folder.Path,
		"source_dir":            folder.SourceDir,
		"relative_path":         folder.RelativePath,
		"category":              folder.Category,
		"category_source":       folder.CategorySource,
		"classification_status": classificationStatus,
		"node_id":               strings.TrimSpace(nodeID),
		"node_type":             strings.TrimSpace(nodeType),
		"error":                 strings.TrimSpace(errMsg),
		"updated_at":            updatedAt.Format(time.RFC3339Nano),
	})
}

func (s *ScannerService) publishFolderClassificationScanError(
	jobID string,
	target scanTarget,
	folder *repository.Folder,
	errMsg string,
) {
	if s.broker == nil {
		return
	}

	category := "other"
	categorySource := "auto"
	if folder != nil {
		if strings.TrimSpace(folder.Category) != "" {
			category = folder.Category
		}
		if strings.TrimSpace(folder.CategorySource) != "" {
			categorySource = folder.CategorySource
		}
	}

	s.publish("folder.classification.updated", map[string]any{
		"folder_id":             folderIDForScanError(folder, target),
		"job_id":                strings.TrimSpace(jobID),
		"workflow_run_id":       "",
		"folder_name":           target.folderName,
		"folder_path":           target.folderPath,
		"source_dir":            target.sourceDir,
		"relative_path":         target.relativePath,
		"category":              category,
		"category_source":       categorySource,
		"classification_status": "failed",
		"node_id":               "",
		"node_type":             "",
		"error":                 strings.TrimSpace(errMsg),
		"updated_at":            time.Now().UTC().Format(time.RFC3339Nano),
	})
}

func (s *ScannerService) resolveScanErrorFolder(ctx context.Context, target scanTarget) *repository.Folder {
	if s.folders == nil {
		return nil
	}

	folder, _, err := s.folders.ResolveScanTarget(ctx, target.folderPath, target.sourceDir, target.relativePath)
	if err != nil {
		return nil
	}

	return folder
}

func folderIDForScanError(folder *repository.Folder, target scanTarget) string {
	if folder != nil {
		return strings.TrimSpace(folder.ID)
	}

	path := strings.TrimSpace(target.folderPath)
	if path == "" {
		path = strings.TrimSpace(target.folderName)
	}
	if path == "" {
		return ""
	}

	replacer := strings.NewReplacer("\\", "_", "/", "_", ":", "_")
	return "scan_error_" + replacer.Replace(path)
}

func normalizeSourceDirs(sourceDirs []string) []string {
	return normalizeScanPaths(sourceDirs)
}

func normalizeScanPaths(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	result := make([]string, 0, len(paths))
	for _, item := range paths {
		trimmed := normalizeScanPath(item)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		result = append(result, trimmed)
	}
	return result
}

func normalizeScanPath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}

	return filepath.Clean(trimmed)
}

func isExcludedScanPath(path string, excludeDirs []string) bool {
	normalizedPath := normalizePathForCompare(path)
	if normalizedPath == "" {
		return false
	}

	for _, excludedDir := range excludeDirs {
		normalizedExcludedDir := normalizePathForCompare(excludedDir)
		if normalizedExcludedDir == "" {
			continue
		}
		if normalizedPath == normalizedExcludedDir {
			return true
		}
		if strings.HasPrefix(normalizedPath, normalizedExcludedDir+string(filepath.Separator)) {
			return true
		}
	}

	return false
}

func normalizePathForCompare(path string) string {
	normalized := normalizeScanPath(path)
	if normalized == "" {
		return ""
	}

	return strings.ToLower(normalized)
}
