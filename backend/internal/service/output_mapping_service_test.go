package service

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	"github.com/liqiye/classifier/internal/repository"
)

func TestOutputMappingServiceBuildFallsBackToRunFolderIDForSplitItems(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	mappingRepo := repository.NewOutputMappingRepository(database)

	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	albumPath := filepath.Join(sourceRoot, "album")
	stagePath := filepath.Join(albumPath, "__photo")
	sourceFile := filepath.Join(stagePath, "cover.jpg")
	targetFile := filepath.Join(root, "target", "photo", "cover.jpg")

	folder := &repository.Folder{
		ID:             "folder-output-mapping-fallback",
		Path:           albumPath,
		SourceDir:      sourceRoot,
		RelativePath:   "album",
		Name:           "album",
		Category:       "mixed",
		CategorySource: "workflow",
		Status:         "pending",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	run := &repository.WorkflowRun{
		ID:            "run-output-mapping-fallback",
		JobID:         "job-output-mapping-fallback",
		FolderID:      folder.ID,
		WorkflowDefID: "wf-output-mapping-fallback",
		Status:        "succeeded",
	}
	if err := workflowRunRepo.Create(ctx, run); err != nil {
		t.Fatalf("workflowRunRepo.Create() error = %v", err)
	}

	outputJSON := mustOutputMappingTypedOutputs(t, map[string]TypedValue{
		"items": {
			Type: PortTypeProcessingItemList,
			Value: []ProcessingItem{{
				SourcePath:         stagePath,
				CurrentPath:        stagePath,
				FolderName:         "album",
				Category:           "photo",
				RootPath:           albumPath,
				OriginalSourcePath: albumPath,
				SourceKind:         ProcessingItemSourceKindDirectory,
			}},
		},
		"step_results": {
			Type: PortTypeProcessingStepResultList,
			Value: []ProcessingStepResult{{
				SourcePath: sourceFile,
				TargetPath: targetFile,
				NodeType:   phase4MoveNodeExecutorType,
				Status:     "moved",
			}},
		},
	})
	if err := nodeRunRepo.Create(ctx, &repository.NodeRun{
		ID:            "node-run-output-mapping-fallback",
		WorkflowRunID: run.ID,
		NodeID:        "move-photo",
		NodeType:      phase4MoveNodeExecutorType,
		Sequence:      1,
		Status:        "succeeded",
		OutputJSON:    outputJSON,
	}); err != nil {
		t.Fatalf("nodeRunRepo.Create() error = %v", err)
	}

	svc := NewOutputMappingService(workflowRunRepo, nodeRunRepo, folderRepo, mappingRepo)
	if err := svc.Build(ctx, run.ID); err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	mappings, err := mappingRepo.ListByWorkflowRunID(ctx, run.ID)
	if err != nil {
		t.Fatalf("mappingRepo.ListByWorkflowRunID() error = %v", err)
	}
	if len(mappings) != 1 {
		t.Fatalf("mappings len = %d, want 1", len(mappings))
	}
	mapping := mappings[0]
	if mapping.FolderID != folder.ID {
		t.Fatalf("mapping folder_id = %q, want %q", mapping.FolderID, folder.ID)
	}
	if mapping.SourcePath != normalizeWorkflowPath(sourceFile) {
		t.Fatalf("mapping source_path = %q, want %q", mapping.SourcePath, normalizeWorkflowPath(sourceFile))
	}
	if mapping.OutputPath != normalizeWorkflowPath(targetFile) {
		t.Fatalf("mapping output_path = %q, want %q", mapping.OutputPath, normalizeWorkflowPath(targetFile))
	}
	if mapping.SourceRelativePath != normalizeWorkflowPath(filepath.Join("__photo", "cover.jpg")) {
		t.Fatalf("mapping source_relative_path = %q, want __photo/cover.jpg", mapping.SourceRelativePath)
	}
	if mapping.NodeType != phase4MoveNodeExecutorType {
		t.Fatalf("mapping node_type = %q, want %q", mapping.NodeType, phase4MoveNodeExecutorType)
	}
}

func TestOutputMappingResolveFolderIDMatchesNormalizedChildPath(t *testing.T) {
	t.Parallel()

	items := []ProcessingItem{{
		FolderID:    "folder-images",
		SourcePath:  `E:\TEST\sample\yourpersonalwaifu\Images`,
		CurrentPath: `E:\TEST\sample\yourpersonalwaifu\Images`,
	}}
	step := ProcessingStepResult{
		SourcePath: "E:/TEST/sample/yourpersonalwaifu/Images/001.jpg",
		TargetPath: "E:/target/photo/001.jpg",
	}

	got := outputMappingResolveFolderID(items, step, outputMappingFolderPathMap(items))
	if got != "folder-images" {
		t.Fatalf("outputMappingResolveFolderID() = %q, want folder-images", got)
	}
}

func TestOutputMappingServiceBuildSkipsFailedStepResults(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	mappingRepo := repository.NewOutputMappingRepository(database)

	folder := &repository.Folder{
		ID:             "folder-output-mapping-failed-step",
		Path:           "/source/album",
		SourceDir:      "/source",
		RelativePath:   "album",
		Name:           "album",
		Category:       "video",
		CategorySource: "workflow",
		Status:         "pending",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	run := &repository.WorkflowRun{
		ID:            "run-output-mapping-failed-step",
		JobID:         "job-output-mapping-failed-step",
		FolderID:      folder.ID,
		WorkflowDefID: "wf-output-mapping-failed-step",
		Status:        "succeeded",
	}
	if err := workflowRunRepo.Create(ctx, run); err != nil {
		t.Fatalf("workflowRunRepo.Create() error = %v", err)
	}

	outputJSON := mustOutputMappingTypedOutputs(t, map[string]TypedValue{
		"items": {
			Type: PortTypeProcessingItemList,
			Value: []ProcessingItem{{
				FolderID:    folder.ID,
				SourcePath:  "/source/album",
				CurrentPath: "/source/album",
				FolderName:  "album",
				Category:    "video",
				SourceKind:  ProcessingItemSourceKindDirectory,
			}},
		},
		"step_results": {
			Type: PortTypeProcessingStepResultList,
			Value: []ProcessingStepResult{{
				FolderID:   folder.ID,
				SourcePath: "/source/album/bad.mp4",
				TargetPath: "/source/album/bad.jpg",
				NodeType:   thumbnailNodeExecutorType,
				Status:     "failed",
				Error:      "moov atom not found",
			}},
		},
	})
	if err := nodeRunRepo.Create(ctx, &repository.NodeRun{
		ID:            "node-run-output-mapping-failed-step",
		WorkflowRunID: run.ID,
		NodeID:        "thumbnail-video",
		NodeType:      thumbnailNodeExecutorType,
		Sequence:      1,
		Status:        "succeeded",
		OutputJSON:    outputJSON,
	}); err != nil {
		t.Fatalf("nodeRunRepo.Create() error = %v", err)
	}

	svc := NewOutputMappingService(workflowRunRepo, nodeRunRepo, folderRepo, mappingRepo)
	if err := svc.Build(ctx, run.ID); err != nil {
		t.Fatalf("Build() error = %v", err)
	}

	mappings, err := mappingRepo.ListByWorkflowRunID(ctx, run.ID)
	if err != nil {
		t.Fatalf("mappingRepo.ListByWorkflowRunID() error = %v", err)
	}
	if len(mappings) != 0 {
		t.Fatalf("mappings len = %d, want 0", len(mappings))
	}
}

func mustOutputMappingTypedOutputs(t *testing.T, values map[string]TypedValue) string {
	t.Helper()

	encoded, err := typedValueMapToJSON(values, NewTypeRegistry())
	if err != nil {
		t.Fatalf("typedValueMapToJSON() error = %v", err)
	}
	raw, err := json.Marshal(encoded)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	return string(raw)
}
