package repository

import (
	"context"
	"errors"
	"testing"
)

func TestWorkflowRunRepository_CRUDFilterAndTransitions(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewWorkflowRunRepository(database)
	ctx := context.Background()

	items := []*WorkflowRun{
		{ID: "wr-1", JobID: "job-1", FolderID: "f-1", WorkflowDefID: "wf-1", Status: "pending"},
		{ID: "wr-2", JobID: "job-1", FolderID: "f-2", WorkflowDefID: "wf-1", Status: "running"},
		{ID: "wr-3", JobID: "job-2", FolderID: "f-1", WorkflowDefID: "wf-2", Status: "failed"},
	}
	for _, item := range items {
		if err := repo.Create(ctx, item); err != nil {
			t.Fatalf("Create(%s) error = %v", item.ID, err)
		}
	}

	list, total, err := repo.List(ctx, WorkflowRunListFilter{JobID: "job-1", Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("List(job=job-1) error = %v", err)
	}
	if total != 2 || len(list) != 2 {
		t.Fatalf("List(job=job-1) total/len = %d/%d, want 2/2", total, len(list))
	}

	paged, pagedTotal, err := repo.List(ctx, WorkflowRunListFilter{FolderID: "f-1", Page: 2, Limit: 1})
	if err != nil {
		t.Fatalf("List(folder=f-1,page=2,limit=1) error = %v", err)
	}
	if pagedTotal != 2 || len(paged) != 1 {
		t.Fatalf("List(folder=f-1,page=2,limit=1) total/len = %d/%d, want 2/1", pagedTotal, len(paged))
	}

	if err := repo.UpdateStatus(ctx, "wr-1", "running", "node-1"); err != nil {
		t.Fatalf("UpdateStatus(running) error = %v", err)
	}
	if err := repo.UpdateStatus(ctx, "wr-1", "succeeded", "node-2"); err != nil {
		t.Fatalf("UpdateStatus(succeeded) error = %v", err)
	}
	updated, err := repo.GetByID(ctx, "wr-1")
	if err != nil {
		t.Fatalf("GetByID(wr-1) error = %v", err)
	}
	if updated.Status != "succeeded" || updated.ResumeNodeID != "node-2" || updated.LastNodeID != "node-2" {
		t.Fatalf("updated status/resume/last = %q/%q/%q, want succeeded/node-2/node-2", updated.Status, updated.ResumeNodeID, updated.LastNodeID)
	}
	if updated.StartedAt == nil || updated.FinishedAt == nil {
		t.Fatalf("started_at/finished_at = %#v/%#v, want non-nil", updated.StartedAt, updated.FinishedAt)
	}

	if err := repo.UpdateStatus(ctx, "wr-2", "running", "node-p"); err != nil {
		t.Fatalf("UpdateStatus(wr-2 running) error = %v", err)
	}
	if err := repo.UpdateStatus(ctx, "wr-2", "partial", "node-p"); err != nil {
		t.Fatalf("UpdateStatus(partial) error = %v", err)
	}
	partial, err := repo.GetByID(ctx, "wr-2")
	if err != nil {
		t.Fatalf("GetByID(wr-2 partial) error = %v", err)
	}
	if partial.Status != "partial" || partial.FinishedAt == nil {
		t.Fatalf("partial status/finished_at = %q/%#v, want partial/non-nil", partial.Status, partial.FinishedAt)
	}

	if err := repo.UpdateFailure(ctx, "wr-2", "node-f", "node crashed"); err != nil {
		t.Fatalf("UpdateFailure() error = %v", err)
	}
	failed, err := repo.GetByID(ctx, "wr-2")
	if err != nil {
		t.Fatalf("GetByID(wr-2) error = %v", err)
	}
	if failed.Status != "failed" || failed.ResumeNodeID != "node-f" || failed.Error != "node crashed" {
		t.Fatalf("failed status/resume/error = %q/%q/%q, want failed/node-f/node crashed", failed.Status, failed.ResumeNodeID, failed.Error)
	}

	if err := repo.UpdateBlocks(ctx, "wr-3", 3); err != nil {
		t.Fatalf("UpdateBlocks(+3) error = %v", err)
	}
	if err := repo.UpdateBlocks(ctx, "wr-3", -10); err != nil {
		t.Fatalf("UpdateBlocks(-10) error = %v", err)
	}
	blocked, err := repo.GetByID(ctx, "wr-3")
	if err != nil {
		t.Fatalf("GetByID(wr-3) error = %v", err)
	}
	if blocked.ExternalBlocks != 0 {
		t.Fatalf("external_blocks = %d, want 0", blocked.ExternalBlocks)
	}
}

func TestWorkflowRunRepository_NotFoundBranches(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewWorkflowRunRepository(database)
	ctx := context.Background()

	_, err := repo.GetByID(ctx, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByID(missing) error = %v, want ErrNotFound", err)
	}
	if err := repo.UpdateStatus(ctx, "missing", "running", "node"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateStatus(missing) error = %v, want ErrNotFound", err)
	}
	if err := repo.UpdateFailure(ctx, "missing", "node", "err"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateFailure(missing) error = %v, want ErrNotFound", err)
	}
	if err := repo.UpdateBlocks(ctx, "missing", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateBlocks(missing) error = %v, want ErrNotFound", err)
	}
}
