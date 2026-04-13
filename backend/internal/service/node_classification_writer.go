package service

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/google/uuid"

	"github.com/liqiye/classifier/internal/repository"
)

const classificationWriterExecutorType = "classification-writer"

type classificationWriterNodeExecutor struct {
	folders   repository.FolderRepository
	snapshots repository.SnapshotRepository
	manifests SourceManifestBuilder
}

func newClassificationWriterExecutor(folders repository.FolderRepository, snapshots repository.SnapshotRepository) *classificationWriterNodeExecutor {
	return &classificationWriterNodeExecutor{folders: folders, snapshots: snapshots}
}

func (e *classificationWriterNodeExecutor) SetSourceManifestBuilder(builder SourceManifestBuilder) {
	e.manifests = builder
}

func (e *classificationWriterNodeExecutor) Type() string {
	return classificationWriterExecutorType
}

func (e *classificationWriterNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "分类写入器",
		Description: "将分类条目写入数据库并记录快照",
		Inputs: []PortDef{
			{Name: "entries", Type: PortTypeClassifiedEntryList, Required: true, Description: "分类条目列表"},
		},
		Outputs: []PortDef{
			{Name: "entries", Type: PortTypeClassifiedEntryList, RequiredOutput: true, Description: "写入后的分类条目列表"},
		},
	}
}

func (e *classificationWriterNodeExecutor) Execute(ctx context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	if e.folders == nil {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: folder repository is required", e.Type())
	}

	rawEntries, ok := firstPresentTyped(input.Inputs, "entries")
	if !ok {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: entries input is required", e.Type())
	}

	entries, err := parseClassifiedEntryList(rawEntries)
	if err != nil {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute parse entries: %w", e.Type(), err)
	}

	written := make([]ClassifiedEntry, 0, len(entries))
	for _, entry := range entries {
		out, err := e.persistEntry(ctx, input, entry)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute persist entry %q: %w", e.Type(), entry.Path, err)
		}
		written = append(written, out)
	}

	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{"entries": {Type: PortTypeClassifiedEntryList, Value: written}},
		Status:  ExecutionSuccess,
	}, nil
}

func (e *classificationWriterNodeExecutor) persistEntry(ctx context.Context, input NodeExecutionInput, entry ClassifiedEntry) (ClassifiedEntry, error) {
	path := strings.TrimSpace(entry.Path)
	if path == "" {
		return ClassifiedEntry{}, fmt.Errorf("entry path is required")
	}

	name := strings.TrimSpace(entry.Name)
	if name == "" {
		name = filepath.Base(path)
	}
	category := strings.TrimSpace(entry.Category)
	if category == "" {
		category = "other"
	}

	folder := &repository.Folder{
		ID:             strings.TrimSpace(entry.FolderID),
		Path:           path,
		SourceDir:      strings.TrimSpace(input.SourceDir),
		RelativePath:   relativePathFromSourceDir(input.SourceDir, path),
		Name:           name,
		Category:       category,
		CategorySource: "workflow",
		Status:         "pending",
	}
	stats := summarizeClassifiedEntryMedia(entry)
	folder.ImageCount = stats.imageCount
	folder.VideoCount = stats.videoCount
	folder.OtherFileCount = stats.otherFileCount
	folder.HasOtherFiles = stats.hasOtherFiles
	folder.TotalFiles = stats.totalFiles
	if folder.ID == "" {
		folder.ID = uuid.NewString()
	}

	existing, err := e.folders.GetCurrentByPath(ctx, path)
	if err == nil && existing != nil {
		folder.ID = existing.ID
		if folder.TotalFiles == 0 {
			folder.TotalFiles = existing.TotalFiles
		}
		if folder.ImageCount == 0 {
			folder.ImageCount = existing.ImageCount
		}
		if folder.VideoCount == 0 {
			folder.VideoCount = existing.VideoCount
		}
		if folder.OtherFileCount == 0 {
			folder.OtherFileCount = existing.OtherFileCount
		}
		if !folder.HasOtherFiles {
			folder.HasOtherFiles = existing.HasOtherFiles
		}
	}

	snapshotID, err := e.createCategorySnapshot(ctx, input, folder)
	if err != nil {
		return ClassifiedEntry{}, err
	}

	if err := e.folders.Upsert(ctx, folder); err != nil {
		return ClassifiedEntry{}, err
	}
	if err := e.folders.UpdateCategory(ctx, folder.ID, category, "workflow"); err != nil {
		return ClassifiedEntry{}, err
	}
	if e.manifests != nil {
		if err := e.manifests.Build(ctx, folder.ID); err != nil {
			return ClassifiedEntry{}, fmt.Errorf("build source manifest for folder %q: %w", folder.ID, err)
		}
	}
	if err := e.commitCategorySnapshot(ctx, snapshotID, category); err != nil {
		return ClassifiedEntry{}, err
	}

	children := make([]ClassifiedEntry, 0, len(entry.Subtree))
	for _, child := range entry.Subtree {
		writtenChild, childErr := e.persistEntry(ctx, input, child)
		if childErr != nil {
			return ClassifiedEntry{}, childErr
		}
		children = append(children, writtenChild)
	}

	entry.FolderID = folder.ID
	entry.Name = name
	entry.Category = category
	entry.HasOtherFiles = folder.HasOtherFiles
	entry.Subtree = children
	return entry, nil
}

func (e *classificationWriterNodeExecutor) createCategorySnapshot(ctx context.Context, input NodeExecutionInput, folder *repository.Folder) (string, error) {
	if e.snapshots == nil || folder == nil {
		return "", nil
	}

	before, err := json.Marshal(snapshotBeforeFolder{Category: folder.Category, CategorySource: folder.CategorySource})
	if err != nil {
		return "", err
	}
	detail, err := json.Marshal(map[string]any{
		"workflow_run_id": workflowRunID(input.WorkflowRun),
		"node_run_id":     nodeRunIDForSnapshot(input.NodeRun),
		"folder_path":     folder.Path,
		"node_type":       e.Type(),
	})
	if err != nil {
		return "", err
	}

	snapshotID := uuid.NewString()
	if err := e.snapshots.Create(ctx, &repository.Snapshot{
		ID:            snapshotID,
		JobID:         workflowJobIDForSnapshot(input.WorkflowRun),
		FolderID:      folder.ID,
		OperationType: "classify",
		Before:        before,
		After:         nil,
		Detail:        detail,
		Status:        "pending",
	}); err != nil {
		return "", err
	}

	return snapshotID, nil
}

func (e *classificationWriterNodeExecutor) commitCategorySnapshot(ctx context.Context, snapshotID string, category string) error {
	if e.snapshots == nil || strings.TrimSpace(snapshotID) == "" {
		return nil
	}

	after, err := json.Marshal(snapshotAfterFolder{Category: category, CategorySource: "workflow"})
	if err != nil {
		return err
	}
	if err := e.snapshots.CommitAfter(ctx, snapshotID, after); err != nil {
		return err
	}
	return e.snapshots.UpdateStatus(ctx, snapshotID, "committed")
}

func (e *classificationWriterNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *classificationWriterNodeExecutor) Rollback(ctx context.Context, input NodeRollbackInput) error {
	if e.snapshots == nil || input.WorkflowRun == nil || input.NodeRun == nil {
		return nil
	}

	allSnapshots, err := e.snapshots.ListByJobID(ctx, input.WorkflowRun.JobID)
	if err != nil {
		return fmt.Errorf("%s.Rollback list snapshots: %w", e.Type(), err)
	}

	for _, snapshot := range allSnapshots {
		if snapshot == nil || snapshot.OperationType != "classify" {
			continue
		}
		if !snapshotMatchesNodeRun(snapshot.Detail, input.WorkflowRun.ID, input.NodeRun.ID) {
			continue
		}
		before := snapshotBeforeFolder{}
		if err := json.Unmarshal(snapshot.Before, &before); err != nil {
			return fmt.Errorf("%s.Rollback parse snapshot before %q: %w", e.Type(), snapshot.ID, err)
		}
		category := strings.TrimSpace(before.Category)
		if category == "" {
			category = "other"
		}
		source := strings.TrimSpace(before.CategorySource)
		if source == "" {
			source = "workflow"
		}
		if err := e.folders.UpdateCategory(ctx, snapshot.FolderID, category, source); err != nil {
			return fmt.Errorf("%s.Rollback restore category for folder %q: %w", e.Type(), snapshot.FolderID, err)
		}
		_ = e.snapshots.UpdateStatus(ctx, snapshot.ID, "reverted")
	}

	return nil
}
