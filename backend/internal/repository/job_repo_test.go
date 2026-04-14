package repository

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestJobRepository_CRUDFilterAndStatusTransition(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewJobRepository(database)
	ctx := context.Background()

	jobs := []*Job{
		{ID: "job-1", Type: "workflow", WorkflowDefID: "wf-1", SourceDir: "/src-a", Status: "pending", FolderIDs: `["f1"]`},
		{ID: "job-2", Type: "workflow", WorkflowDefID: "wf-2", SourceDir: "/src-b", Status: "running", FolderIDs: `["f2"]`},
		{ID: "job-3", Type: "scan", SourceDir: "/src-c", Status: "failed", FolderIDs: `[]`},
	}
	for _, item := range jobs {
		if err := repo.Create(ctx, item); err != nil {
			t.Fatalf("Create(%s) error = %v", item.ID, err)
		}
	}

	list, total, err := repo.List(ctx, JobListFilter{Status: "running", Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("List(running) error = %v", err)
	}
	if total != 1 || len(list) != 1 || list[0].ID != "job-2" {
		t.Fatalf("List(running) total/len/id = %d/%d/%q, want 1/1/job-2", total, len(list), list[0].ID)
	}

	pageList, pageTotal, err := repo.List(ctx, JobListFilter{Page: 2, Limit: 2})
	if err != nil {
		t.Fatalf("List(page=2,limit=2) error = %v", err)
	}
	if pageTotal != 3 || len(pageList) != 1 {
		t.Fatalf("List(page=2,limit=2) total/len = %d/%d, want 3/1", pageTotal, len(pageList))
	}

	if err := repo.UpdateTotal(ctx, "job-2", 9); err != nil {
		t.Fatalf("UpdateTotal() error = %v", err)
	}
	if err := repo.IncrementProgress(ctx, "job-2", 2, 1); err != nil {
		t.Fatalf("IncrementProgress() error = %v", err)
	}
	if err := repo.UpdateStatus(ctx, "job-2", "running", ""); err != nil {
		t.Fatalf("UpdateStatus(running) error = %v", err)
	}
	if err := repo.UpdateStatus(ctx, "job-2", "failed", "step failed"); err != nil {
		t.Fatalf("UpdateStatus(failed) error = %v", err)
	}

	updated, err := repo.GetByID(ctx, "job-2")
	if err != nil {
		t.Fatalf("GetByID(job-2) error = %v", err)
	}
	if updated.Total != 9 || updated.Done != 2 || updated.Failed != 1 {
		t.Fatalf("updated total/done/failed = %d/%d/%d, want 9/2/1", updated.Total, updated.Done, updated.Failed)
	}
	if updated.Status != "failed" || updated.Error != "step failed" {
		t.Fatalf("updated status/error = %q/%q, want failed/step failed", updated.Status, updated.Error)
	}
	if updated.StartedAt == nil || updated.FinishedAt == nil {
		t.Fatalf("started_at/finished_at = %#v/%#v, want non-nil", updated.StartedAt, updated.FinishedAt)
	}
}

func TestJobRepository_ConcurrentIncrementProgress(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewJobRepository(database)
	ctx := context.Background()

	if err := repo.Create(ctx, &Job{ID: "job-concurrent", Type: "workflow", Status: "running", FolderIDs: `[]`}); err != nil {
		t.Fatalf("Create(job-concurrent) error = %v", err)
	}

	const workers = 16
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			if err := repo.IncrementProgress(ctx, "job-concurrent", 1, 0); err != nil {
				t.Errorf("IncrementProgress() error = %v", err)
			}
		}()
	}
	wg.Wait()

	item, err := repo.GetByID(ctx, "job-concurrent")
	if err != nil {
		t.Fatalf("GetByID(job-concurrent) error = %v", err)
	}
	if item.Done != workers || item.Failed != 0 {
		t.Fatalf("done/failed = %d/%d, want %d/0", item.Done, item.Failed, workers)
	}
}

func TestJobRepository_NotFoundBranches(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewJobRepository(database)
	ctx := context.Background()

	_, err := repo.GetByID(ctx, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByID(missing) error = %v, want ErrNotFound", err)
	}

	if err := repo.UpdateTotal(ctx, "missing", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateTotal(missing) error = %v, want ErrNotFound", err)
	}
	if err := repo.UpdateStatus(ctx, "missing", "running", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateStatus(missing) error = %v, want ErrNotFound", err)
	}
	if err := repo.IncrementProgress(ctx, "missing", 1, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("IncrementProgress(missing) error = %v, want ErrNotFound", err)
	}
}
