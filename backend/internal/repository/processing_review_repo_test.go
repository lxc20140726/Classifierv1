package repository

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestProcessingReviewRepositoryGetLatestByFolderID(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewProcessingReviewRepository(database)
	ctx := context.Background()

	writer, ok := repo.(interface {
		Create(ctx context.Context, item *ProcessingReviewItem) error
	})
	if !ok {
		t.Fatalf("processing review repository does not expose Create")
	}
	reader, ok := repo.(interface {
		GetLatestByFolderID(ctx context.Context, folderID string) (*ProcessingReviewItem, error)
	})
	if !ok {
		t.Fatalf("processing review repository does not expose GetLatestByFolderID")
	}

	if err := writer.Create(ctx, &ProcessingReviewItem{
		ID:            "review-1",
		WorkflowRunID: "wr-1",
		JobID:         "job-1",
		FolderID:      "folder-1",
		Status:        "pending",
	}); err != nil {
		t.Fatalf("Create(review-1) error = %v", err)
	}
	if err := writer.Create(ctx, &ProcessingReviewItem{
		ID:            "review-2",
		WorkflowRunID: "wr-2",
		JobID:         "job-2",
		FolderID:      "folder-1",
		Status:        "approved",
	}); err != nil {
		t.Fatalf("Create(review-2) error = %v", err)
	}

	if _, err := database.ExecContext(ctx, "UPDATE processing_review_items SET updated_at = ? WHERE id = ?", "2026-01-01 00:00:01", "review-1"); err != nil {
		t.Fatalf("update review-1 updated_at error = %v", err)
	}
	if _, err := database.ExecContext(ctx, "UPDATE processing_review_items SET updated_at = ? WHERE id = ?", "2026-01-01 00:00:02", "review-2"); err != nil {
		t.Fatalf("update review-2 updated_at error = %v", err)
	}
	reviewedAt := time.Date(2026, 1, 1, 0, 0, 3, 0, time.UTC).Format("2006-01-02 15:04:05")
	if _, err := database.ExecContext(ctx, "UPDATE processing_review_items SET reviewed_at = ? WHERE id = ?", reviewedAt, "review-2"); err != nil {
		t.Fatalf("update review-2 reviewed_at error = %v", err)
	}

	got, err := reader.GetLatestByFolderID(ctx, "folder-1")
	if err != nil {
		t.Fatalf("GetLatestByFolderID(folder-1) error = %v", err)
	}
	if got.ID != "review-2" {
		t.Fatalf("GetLatestByFolderID(folder-1).ID = %q, want review-2", got.ID)
	}

	_, err = reader.GetLatestByFolderID(ctx, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetLatestByFolderID(missing) error = %v, want ErrNotFound", err)
	}
}
