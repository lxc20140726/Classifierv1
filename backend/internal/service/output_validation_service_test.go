package service

import (
	"context"
	"testing"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

func TestOutputValidationValidateFolderDirectoryMovePassesForNestedFiles(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)
	configRepo := repository.NewConfigRepository(database)
	manifestRepo := repository.NewSourceManifestRepository(database)
	mappingRepo := repository.NewOutputMappingRepository(database)
	outputCheckRepo := repository.NewOutputCheckRepository(database)
	adapter := fs.NewMockAdapter()

	if err := configRepo.SaveAppConfig(ctx, &repository.AppConfig{
		Version: 1,
		OutputDirs: repository.AppConfigOutputDirs{
			Photo: []string{"/target/photo"},
		},
	}); err != nil {
		t.Fatalf("configRepo.SaveAppConfig() error = %v", err)
	}

	folder := &repository.Folder{
		ID:             "folder-output-validation-dir-move",
		Path:           "/target/photo/album",
		SourceDir:      "/source",
		RelativePath:   "album",
		Name:           "album",
		Category:       "photo",
		CategorySource: "workflow",
		Status:         "pending",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	if err := manifestRepo.CreateBatchForWorkflowRun(ctx, "wr-output-validation-dir-move", folder.ID, "batch-1", []*repository.FolderSourceManifest{
		{
			ID:            "manifest-1",
			WorkflowRunID: "wr-output-validation-dir-move",
			FolderID:      folder.ID,
			BatchID:       "batch-1",
			SourcePath:    "/source/album/a.jpg",
			RelativePath:  "a.jpg",
			FileName:      "a.jpg",
			SizeBytes:     10,
		},
		{
			ID:            "manifest-2",
			WorkflowRunID: "wr-output-validation-dir-move",
			FolderID:      folder.ID,
			BatchID:       "batch-1",
			SourcePath:    "/source/album/sub/b.jpg",
			RelativePath:  "sub/b.jpg",
			FileName:      "b.jpg",
			SizeBytes:     20,
		},
	}); err != nil {
		t.Fatalf("manifestRepo.CreateBatchForWorkflowRun() error = %v", err)
	}

	if err := mappingRepo.ReplaceByWorkflowRunID(ctx, "wr-output-validation-dir-move", []*repository.FolderOutputMapping{{
		ID:               "mapping-1",
		WorkflowRunID:    "wr-output-validation-dir-move",
		FolderID:         folder.ID,
		SourcePath:       "/source/album",
		OutputPath:       "/target/photo/album",
		OutputContainer:  "/target/photo",
		NodeType:         "move-node",
		ArtifactType:     "primary",
		RequiredArtifact: false,
	}}); err != nil {
		t.Fatalf("mappingRepo.ReplaceByWorkflowRunID() error = %v", err)
	}

	adapter.AddFile("/target/photo/album/a.jpg", []byte("a"))
	adapter.AddFile("/target/photo/album/sub/b.jpg", []byte("b"))

	svc := NewOutputValidationService(adapter, folderRepo, configRepo, manifestRepo, mappingRepo, outputCheckRepo)

	check, err := svc.ValidateFolder(ctx, folder.ID)
	if err != nil {
		t.Fatalf("ValidateFolder() error = %v", err)
	}
	if check.Status != "passed" {
		t.Fatalf("check.Status = %q, want passed (errors=%+v)", check.Status, check.Errors)
	}
	if check.MismatchCount != 0 {
		t.Fatalf("check.MismatchCount = %d, want 0", check.MismatchCount)
	}

	updated, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() error = %v", err)
	}
	if updated.OutputCheckSummary.Status != "passed" {
		t.Fatalf("folder output_check_summary status = %q, want passed", updated.OutputCheckSummary.Status)
	}
}

func TestOutputValidationCanMarkDoneRevalidatesStaleSummary(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)
	configRepo := repository.NewConfigRepository(database)
	manifestRepo := repository.NewSourceManifestRepository(database)
	mappingRepo := repository.NewOutputMappingRepository(database)
	outputCheckRepo := repository.NewOutputCheckRepository(database)
	adapter := fs.NewMockAdapter()

	if err := configRepo.SaveAppConfig(ctx, &repository.AppConfig{
		Version: 1,
		OutputDirs: repository.AppConfigOutputDirs{
			Photo: []string{"/target/photo"},
		},
	}); err != nil {
		t.Fatalf("configRepo.SaveAppConfig() error = %v", err)
	}

	folder := &repository.Folder{
		ID:             "folder-output-validation-can-mark-done",
		Path:           "/target/photo/album",
		SourceDir:      "/source",
		RelativePath:   "album",
		Name:           "album",
		Category:       "photo",
		CategorySource: "workflow",
		Status:         "pending",
		OutputCheckSummary: repository.FolderOutputCheckSummary{
			Status: "failed",
		},
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	if err := manifestRepo.CreateBatchForWorkflowRun(ctx, "wr-output-validation-can-mark-done", folder.ID, "batch-1", []*repository.FolderSourceManifest{{
		ID:            "manifest-can-mark-done",
		WorkflowRunID: "wr-output-validation-can-mark-done",
		FolderID:      folder.ID,
		BatchID:       "batch-1",
		SourcePath:    "/source/album/a.jpg",
		RelativePath:  "a.jpg",
		FileName:      "a.jpg",
		SizeBytes:     10,
	}}); err != nil {
		t.Fatalf("manifestRepo.CreateBatchForWorkflowRun() error = %v", err)
	}

	if err := mappingRepo.ReplaceByWorkflowRunID(ctx, "wr-output-validation-can-mark-done", []*repository.FolderOutputMapping{{
		ID:               "mapping-can-mark-done",
		WorkflowRunID:    "wr-output-validation-can-mark-done",
		FolderID:         folder.ID,
		SourcePath:       "/source/album",
		OutputPath:       "/target/photo/album",
		OutputContainer:  "/target/photo",
		NodeType:         "move-node",
		ArtifactType:     "primary",
		RequiredArtifact: false,
	}}); err != nil {
		t.Fatalf("mappingRepo.ReplaceByWorkflowRunID() error = %v", err)
	}

	adapter.AddFile("/target/photo/album/a.jpg", []byte("a"))

	svc := NewOutputValidationService(adapter, folderRepo, configRepo, manifestRepo, mappingRepo, outputCheckRepo)

	canMarkDone, err := svc.CanMarkDone(ctx, folder.ID)
	if err != nil {
		t.Fatalf("CanMarkDone() error = %v", err)
	}
	if !canMarkDone {
		check, checkErr := outputCheckRepo.GetLatestByFolderID(ctx, folder.ID)
		if checkErr != nil {
			t.Fatalf("CanMarkDone() = false, want true; latest check lookup error = %v", checkErr)
		}
		t.Fatalf("CanMarkDone() = false, want true (latest_status=%q errors=%+v)", check.Status, check.Errors)
	}

	updated, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() error = %v", err)
	}
	if updated.OutputCheckSummary.Status != "passed" {
		t.Fatalf("folder output_check_summary status = %q, want passed", updated.OutputCheckSummary.Status)
	}
}
