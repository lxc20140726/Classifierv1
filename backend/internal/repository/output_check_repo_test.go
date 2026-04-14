package repository

import (
	"context"
	"errors"
	"testing"
)

func TestOutputCheckRepository_CreateGetAndSummary(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	folderRepo := NewFolderRepository(database)
	repo := NewOutputCheckRepository(database)
	ctx := context.Background()

	if err := folderRepo.Upsert(ctx, &Folder{
		ID:             "folder-1",
		Path:           "/media/folder-1",
		Name:           "folder-1",
		Category:       "photo",
		CategorySource: "auto",
		Status:         "pending",
	}); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	first := &FolderOutputCheck{
		ID:            "oc-1",
		FolderID:      "folder-1",
		WorkflowRunID: "wr-1",
		Status:        "failed",
		MismatchCount: 2,
		FailedFiles:   []string{"a.jpg"},
		Errors: []OutputCheckError{
			{Code: "output_not_found", Message: "missing file", SourcePath: "/media/folder-1/a.jpg"},
		},
	}
	if err := repo.Create(ctx, first); err != nil {
		t.Fatalf("Create(first) error = %v", err)
	}

	second := &FolderOutputCheck{
		ID:            "oc-2",
		FolderID:      "folder-1",
		WorkflowRunID: "wr-2",
		Status:        "passed",
		MismatchCount: 0,
		FailedFiles:   []string{},
		Errors:        []OutputCheckError{},
	}
	if err := repo.Create(ctx, second); err != nil {
		t.Fatalf("Create(second) error = %v", err)
	}

	latest, err := repo.GetLatestByFolderID(ctx, "folder-1")
	if err != nil {
		t.Fatalf("GetLatestByFolderID() error = %v", err)
	}
	if latest.ID != "oc-2" || latest.Status != "passed" {
		t.Fatalf("latest id/status = %q/%q, want oc-2/passed", latest.ID, latest.Status)
	}

	if err := repo.UpdateFolderSummary(ctx, "folder-1", FolderOutputCheckSummary{
		Status:        "failed",
		WorkflowRunID: "wr-2",
		MismatchCount: 1,
		FailedFiles:   []string{"a.jpg"},
	}); err != nil {
		t.Fatalf("UpdateFolderSummary() error = %v", err)
	}

	folder, err := folderRepo.GetByID(ctx, "folder-1")
	if err != nil {
		t.Fatalf("GetByID(folder-1) error = %v", err)
	}
	if folder.OutputCheckSummary.Status != "failed" || folder.OutputCheckSummary.WorkflowRunID != "wr-2" {
		t.Fatalf("output_check_summary status/workflow_run_id = %q/%q, want failed/wr-2", folder.OutputCheckSummary.Status, folder.OutputCheckSummary.WorkflowRunID)
	}

	if err := repo.MarkFolderPending(ctx, "folder-1"); err != nil {
		t.Fatalf("MarkFolderPending() error = %v", err)
	}
	folder, err = folderRepo.GetByID(ctx, "folder-1")
	if err != nil {
		t.Fatalf("GetByID(folder-1) after pending error = %v", err)
	}
	if folder.OutputCheckSummary.Status != "pending" || folder.OutputCheckSummary.MismatchCount != 0 {
		t.Fatalf("pending summary status/mismatch = %q/%d, want pending/0", folder.OutputCheckSummary.Status, folder.OutputCheckSummary.MismatchCount)
	}
}

func TestOutputCheckRepository_NotFoundBranches(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewOutputCheckRepository(database)
	ctx := context.Background()

	_, err := repo.GetLatestByFolderID(ctx, "missing-folder")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetLatestByFolderID(missing) error = %v, want ErrNotFound", err)
	}

	if err := repo.UpdateFolderSummary(ctx, "missing-folder", FolderOutputCheckSummary{Status: "failed"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateFolderSummary(missing) error = %v, want ErrNotFound", err)
	}
}
