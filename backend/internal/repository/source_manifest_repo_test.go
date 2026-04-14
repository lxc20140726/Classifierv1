package repository

import (
	"context"
	"sync"
	"testing"
)

func TestSourceManifestRepository_BatchesAndQueries(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewSourceManifestRepository(database)
	ctx := context.Background()

	batch1 := []*FolderSourceManifest{
		{ID: "m-1", FolderID: "folder-1", BatchID: "batch-1", SourcePath: "/media/f1/a.jpg", RelativePath: "a.jpg", FileName: "a.jpg", SizeBytes: 10},
		{ID: "m-2", FolderID: "folder-1", BatchID: "batch-1", SourcePath: "/media/f1/b.jpg", RelativePath: "b.jpg", FileName: "b.jpg", SizeBytes: 20},
	}
	if err := repo.CreateBatch(ctx, "folder-1", "batch-1", batch1); err != nil {
		t.Fatalf("CreateBatch(batch-1) error = %v", err)
	}

	latest, err := repo.ListLatestByFolderID(ctx, "folder-1")
	if err != nil {
		t.Fatalf("ListLatestByFolderID() error = %v", err)
	}
	if len(latest) != 2 {
		t.Fatalf("len(latest) = %d, want 2", len(latest))
	}
	if latest[0].RelativePath != "a.jpg" || latest[1].RelativePath != "b.jpg" {
		t.Fatalf("latest relative paths = %q,%q, want a.jpg,b.jpg", latest[0].RelativePath, latest[1].RelativePath)
	}

	// 同 batch 重建应先删后建，保持幂等。
	if err := repo.CreateBatch(ctx, "folder-1", "batch-1", []*FolderSourceManifest{{
		ID: "m-1b", FolderID: "folder-1", BatchID: "batch-1", SourcePath: "/media/f1/c.jpg", RelativePath: "c.jpg", FileName: "c.jpg", SizeBytes: 30,
	}}); err != nil {
		t.Fatalf("CreateBatch(batch-1 replace) error = %v", err)
	}

	latest, err = repo.ListLatestByFolderID(ctx, "folder-1")
	if err != nil {
		t.Fatalf("ListLatestByFolderID() after replace error = %v", err)
	}
	if len(latest) != 1 || latest[0].RelativePath != "c.jpg" {
		t.Fatalf("after replace len/path = %d/%q, want 1/c.jpg", len(latest), latest[0].RelativePath)
	}

	if err := repo.CreateBatch(ctx, "folder-1", "batch-2", []*FolderSourceManifest{{
		ID: "m-3", FolderID: "folder-1", BatchID: "batch-2", SourcePath: "/media/f1/d.jpg", RelativePath: "d.jpg", FileName: "d.jpg", SizeBytes: 40,
	}}); err != nil {
		t.Fatalf("CreateBatch(batch-2) error = %v", err)
	}

	latest, err = repo.ListLatestByFolderID(ctx, "folder-1")
	if err != nil {
		t.Fatalf("ListLatestByFolderID() after batch-2 error = %v", err)
	}
	if len(latest) != 1 || latest[0].BatchID != "batch-2" {
		t.Fatalf("latest batch = %q, want batch-2", latest[0].BatchID)
	}

	if err := repo.CreateBatchForWorkflowRun(ctx, "wr-1", "folder-1", "batch-run-1", []*FolderSourceManifest{{
		ID: "m-run-1", FolderID: "folder-1", BatchID: "batch-run-1", WorkflowRunID: "wr-1", SourcePath: "/media/f1/e.jpg", RelativePath: "e.jpg", FileName: "e.jpg", SizeBytes: 50,
	}}); err != nil {
		t.Fatalf("CreateBatchForWorkflowRun() error = %v", err)
	}

	runItems, err := repo.ListByWorkflowRunAndFolderID(ctx, "wr-1", "folder-1")
	if err != nil {
		t.Fatalf("ListByWorkflowRunAndFolderID() error = %v", err)
	}
	if len(runItems) != 1 || runItems[0].RelativePath != "e.jpg" {
		t.Fatalf("run items len/path = %d/%q, want 1/e.jpg", len(runItems), runItems[0].RelativePath)
	}

	exists, err := repo.ExistsByWorkflowRunAndFolderID(ctx, "wr-1", "folder-1")
	if err != nil {
		t.Fatalf("ExistsByWorkflowRunAndFolderID() error = %v", err)
	}
	if !exists {
		t.Fatalf("exists = false, want true")
	}

	notExists, err := repo.ExistsByWorkflowRunAndFolderID(ctx, "wr-x", "folder-1")
	if err != nil {
		t.Fatalf("ExistsByWorkflowRunAndFolderID(missing) error = %v", err)
	}
	if notExists {
		t.Fatalf("notExists = true, want false")
	}

	allItems, err := repo.ListByFolderID(ctx, "folder-1")
	if err != nil {
		t.Fatalf("ListByFolderID() error = %v", err)
	}
	if len(allItems) < 3 {
		t.Fatalf("len(allItems) = %d, want >= 3", len(allItems))
	}
}

func TestSourceManifestRepository_ConcurrentBatchWrites(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewSourceManifestRepository(database)
	ctx := context.Background()

	wg := sync.WaitGroup{}
	for i := 0; i < 6; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			batchID := "batch-concurrent-" + string(rune('a'+i))
			if err := repo.CreateBatchForWorkflowRun(ctx, "wr-concurrent", "folder-concurrent", batchID, []*FolderSourceManifest{{
				ID:            "m-concurrent-" + batchID,
				FolderID:      "folder-concurrent",
				BatchID:       batchID,
				WorkflowRunID: "wr-concurrent",
				SourcePath:    "/media/concurrent/" + batchID + ".jpg",
				RelativePath:  batchID + ".jpg",
				FileName:      batchID + ".jpg",
				SizeBytes:     1,
			}}); err != nil {
				t.Errorf("CreateBatchForWorkflowRun(%s) error = %v", batchID, err)
			}
		}()
	}
	wg.Wait()

	exists, err := repo.ExistsByWorkflowRunAndFolderID(ctx, "wr-concurrent", "folder-concurrent")
	if err != nil {
		t.Fatalf("ExistsByWorkflowRunAndFolderID(concurrent) error = %v", err)
	}
	if !exists {
		t.Fatalf("exists = false, want true")
	}

	runItems, err := repo.ListByWorkflowRunAndFolderID(ctx, "wr-concurrent", "folder-concurrent")
	if err != nil {
		t.Fatalf("ListByWorkflowRunAndFolderID(concurrent) error = %v", err)
	}
	if len(runItems) != 6 {
		t.Fatalf("len(runItems) = %d, want 6", len(runItems))
	}
}
