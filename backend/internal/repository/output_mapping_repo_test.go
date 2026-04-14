package repository

import (
	"context"
	"errors"
	"testing"
)

func TestOutputMappingRepository_ReplaceListAndLatest(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewOutputMappingRepository(database)
	ctx := context.Background()

	firstBatch := []*FolderOutputMapping{
		{
			ID:                 "map-1",
			WorkflowRunID:      "wr-1",
			FolderID:           "folder-1",
			SourcePath:         "/media/f1/a.jpg",
			SourceRelativePath: "a.jpg",
			OutputPath:         "/target/photo/a.jpg",
			OutputContainer:    "/target/photo",
			NodeType:           "move-node",
			ArtifactType:       "primary",
			RequiredArtifact:   false,
		},
		nil,
		{
			ID:                 "map-2",
			WorkflowRunID:      "wr-1",
			FolderID:           "folder-1",
			SourcePath:         "/media/f1/b.jpg",
			SourceRelativePath: "b.jpg",
			OutputPath:         "/target/photo/b.jpg",
			OutputContainer:    "/target/photo",
			NodeType:           "thumbnail-node",
			ArtifactType:       "thumbnail",
			RequiredArtifact:   true,
		},
	}
	if err := repo.ReplaceByWorkflowRunID(ctx, "wr-1", firstBatch); err != nil {
		t.Fatalf("ReplaceByWorkflowRunID(first) error = %v", err)
	}

	list, err := repo.ListByWorkflowRunID(ctx, "wr-1")
	if err != nil {
		t.Fatalf("ListByWorkflowRunID() error = %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("len(list) = %d, want 2", len(list))
	}

	forFolder, err := repo.ListByWorkflowRunAndFolderID(ctx, "wr-1", "folder-1")
	if err != nil {
		t.Fatalf("ListByWorkflowRunAndFolderID() error = %v", err)
	}
	if len(forFolder) != 2 {
		t.Fatalf("len(forFolder) = %d, want 2", len(forFolder))
	}

	if err := repo.ReplaceByWorkflowRunID(ctx, "wr-1", []*FolderOutputMapping{{
		ID:                 "map-3",
		WorkflowRunID:      "wr-1",
		FolderID:           "folder-1",
		SourcePath:         "/media/f1/c.jpg",
		SourceRelativePath: "c.jpg",
		OutputPath:         "/target/photo/c.jpg",
		OutputContainer:    "/target/photo",
		NodeType:           "move-node",
		ArtifactType:       "primary",
	}}); err != nil {
		t.Fatalf("ReplaceByWorkflowRunID(second) error = %v", err)
	}

	list, err = repo.ListByWorkflowRunID(ctx, "wr-1")
	if err != nil {
		t.Fatalf("ListByWorkflowRunID() after replace error = %v", err)
	}
	if len(list) != 1 || list[0].ID != "map-3" {
		t.Fatalf("after replace len/id = %d/%q, want 1/map-3", len(list), list[0].ID)
	}

	if err := repo.ReplaceByWorkflowRunID(ctx, "wr-2", []*FolderOutputMapping{{
		ID:                 "map-4",
		WorkflowRunID:      "wr-2",
		FolderID:           "folder-1",
		SourcePath:         "/media/f1/d.jpg",
		SourceRelativePath: "d.jpg",
		OutputPath:         "/target/photo/d.jpg",
		OutputContainer:    "/target/photo",
		NodeType:           "move-node",
		ArtifactType:       "primary",
	}}); err != nil {
		t.Fatalf("ReplaceByWorkflowRunID(wr-2) error = %v", err)
	}

	latestRunID, err := repo.GetLatestWorkflowRunIDByFolderID(ctx, "folder-1")
	if err != nil {
		t.Fatalf("GetLatestWorkflowRunIDByFolderID() error = %v", err)
	}
	if latestRunID != "wr-2" {
		t.Fatalf("latest workflow_run_id = %q, want wr-2", latestRunID)
	}
}

func TestOutputMappingRepository_NotFound(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewOutputMappingRepository(database)
	ctx := context.Background()

	_, err := repo.GetLatestWorkflowRunIDByFolderID(ctx, "missing-folder")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetLatestWorkflowRunIDByFolderID(missing) error = %v, want ErrNotFound", err)
	}
}
