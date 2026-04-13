package service

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/google/uuid"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

type SourceManifestService struct {
	fs           fs.FSAdapter
	folders      repository.FolderRepository
	manifests    repository.SourceManifestRepository
	outputChecks repository.OutputCheckRepository
}

func NewSourceManifestService(
	fsAdapter fs.FSAdapter,
	folderRepo repository.FolderRepository,
	manifestRepo repository.SourceManifestRepository,
	outputCheckRepo repository.OutputCheckRepository,
) *SourceManifestService {
	return &SourceManifestService{
		fs:           fsAdapter,
		folders:      folderRepo,
		manifests:    manifestRepo,
		outputChecks: outputCheckRepo,
	}
}

func (s *SourceManifestService) Build(ctx context.Context, folderID string) error {
	return s.build(ctx, "", folderID)
}

func (s *SourceManifestService) EnsureForWorkflowRun(ctx context.Context, workflowRunID string, items []ProcessingItem) error {
	workflowRunID = strings.TrimSpace(workflowRunID)
	if workflowRunID == "" {
		return fmt.Errorf("sourceManifest.EnsureForWorkflowRun workflow_run_id is required")
	}

	folderIDs := make([]string, 0, len(items))
	seen := map[string]struct{}{}
	for _, item := range items {
		folderID := strings.TrimSpace(item.FolderID)
		if folderID == "" {
			continue
		}
		if _, ok := seen[folderID]; ok {
			continue
		}
		seen[folderID] = struct{}{}
		folderIDs = append(folderIDs, folderID)
	}
	for _, folderID := range folderIDs {
		exists, err := s.manifests.ExistsByWorkflowRunAndFolderID(ctx, workflowRunID, folderID)
		if err != nil {
			return fmt.Errorf("sourceManifest.EnsureForWorkflowRun check existing for folder %q: %w", folderID, err)
		}
		if exists {
			continue
		}
		if err := s.build(ctx, workflowRunID, folderID); err != nil {
			return err
		}
	}

	return nil
}

func (s *SourceManifestService) build(ctx context.Context, workflowRunID, folderID string) error {
	folder, err := s.folders.GetByID(ctx, folderID)
	if err != nil {
		return fmt.Errorf("sourceManifest.Build get folder %q: %w", folderID, err)
	}

	rootPath := normalizeWorkflowPath(folder.Path)
	if rootPath == "" {
		return fmt.Errorf("sourceManifest.Build folder %q path is empty", folderID)
	}

	entries := make([]*repository.FolderSourceManifest, 0, 64)
	if err := s.collectManifests(ctx, rootPath, rootPath, folder.ID, &entries); err != nil {
		return fmt.Errorf("sourceManifest.Build collect manifests for folder %q: %w", folderID, err)
	}
	sort.Slice(entries, func(i, j int) bool {
		return strings.TrimSpace(entries[i].RelativePath) < strings.TrimSpace(entries[j].RelativePath)
	})

	batchID := uuid.NewString()
	for _, item := range entries {
		item.ID = uuid.NewString()
		item.BatchID = batchID
		item.WorkflowRunID = workflowRunID
	}

	var createErr error
	if strings.TrimSpace(workflowRunID) == "" {
		createErr = s.manifests.CreateBatch(ctx, folder.ID, batchID, entries)
	} else {
		createErr = s.manifests.CreateBatchForWorkflowRun(ctx, workflowRunID, folder.ID, batchID, entries)
	}
	if createErr != nil {
		return fmt.Errorf("sourceManifest.Build create batch for folder %q: %w", folderID, createErr)
	}
	if s.outputChecks != nil {
		if err := s.outputChecks.MarkFolderPending(ctx, folder.ID); err != nil {
			return fmt.Errorf("sourceManifest.Build mark pending for folder %q: %w", folderID, err)
		}
	}

	return nil
}

func (s *SourceManifestService) collectManifests(
	ctx context.Context,
	rootPath string,
	currentPath string,
	folderID string,
	out *[]*repository.FolderSourceManifest,
) error {
	entries, err := s.fs.ReadDir(ctx, currentPath)
	if err != nil {
		return fmt.Errorf("read dir %q: %w", currentPath, err)
	}

	sort.Slice(entries, func(i, j int) bool {
		return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name)
	})

	for _, entry := range entries {
		childPath := joinWorkflowPath(currentPath, entry.Name)
		if entry.IsDir {
			if err := s.collectManifests(ctx, rootPath, childPath, folderID, out); err != nil {
				return err
			}
			continue
		}

		info, err := s.fs.Stat(ctx, childPath)
		if err != nil {
			return fmt.Errorf("stat %q: %w", childPath, err)
		}
		if info.IsDir {
			continue
		}

		relativePath, err := filepath.Rel(rootPath, childPath)
		if err != nil {
			return fmt.Errorf("resolve relative path %q from root %q: %w", childPath, rootPath, err)
		}
		normalizedRelativePath := normalizeWorkflowPath(relativePath)
		if normalizedRelativePath == "." {
			normalizedRelativePath = strings.TrimSpace(entry.Name)
		}

		*out = append(*out, &repository.FolderSourceManifest{
			FolderID:     folderID,
			SourcePath:   normalizeWorkflowPath(childPath),
			RelativePath: normalizedRelativePath,
			FileName:     strings.TrimSpace(entry.Name),
			SizeBytes:    info.Size,
		})
	}

	return nil
}
