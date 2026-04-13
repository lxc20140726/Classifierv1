package repository

import (
	"context"
	"testing"
	"time"
)

func TestScheduledWorkflowRepositoryCRUD(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewScheduledWorkflowRepository(database)
	ctx := context.Background()
	now := time.Now().UTC().Truncate(time.Second)

	item := &ScheduledWorkflow{
		ID:            "sched-1",
		Name:          "每小时整理",
		JobType:       "workflow",
		WorkflowDefID: "wf-1",
		FolderIDs:     `["folder-1","folder-2"]`,
		SourceDirs:    `[]`,
		CronSpec:      "0 * * * *",
		Enabled:       true,
		LastRunAt:     &now,
	}
	if err := repo.Create(ctx, item); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	got, err := repo.GetByID(ctx, item.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if got.Name != item.Name || got.WorkflowDefID != item.WorkflowDefID || got.JobType != item.JobType || got.CronSpec != item.CronSpec || !got.Enabled {
		t.Fatalf("GetByID() = %#v, want matching fields", got)
	}

	items, total, err := repo.List(ctx, ScheduledWorkflowListFilter{Page: 1, Limit: 20})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("List() total/items = %d/%d, want 1/1", total, len(items))
	}

	enabledItems, err := repo.ListEnabled(ctx)
	if err != nil {
		t.Fatalf("ListEnabled() error = %v", err)
	}
	if len(enabledItems) != 1 {
		t.Fatalf("ListEnabled() len = %d, want 1", len(enabledItems))
	}

	item.Name = "每天整理"
	item.JobType = "scan"
	item.WorkflowDefID = ""
	item.FolderIDs = `[]`
	item.SourceDirs = `["/scan-a"]`
	item.CronSpec = "0 0 * * *"
	item.Enabled = false
	if err := repo.Update(ctx, item); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	updated, err := repo.GetByID(ctx, item.ID)
	if err != nil {
		t.Fatalf("GetByID(updated) error = %v", err)
	}
	if updated.Name != item.Name || updated.CronSpec != item.CronSpec || updated.JobType != item.JobType || updated.Enabled {
		t.Fatalf("updated item = %#v, want updated fields", updated)
	}

	later := now.Add(2 * time.Hour)
	if err := repo.UpdateLastRunAt(ctx, item.ID, later); err != nil {
		t.Fatalf("UpdateLastRunAt() error = %v", err)
	}
	updated, err = repo.GetByID(ctx, item.ID)
	if err != nil {
		t.Fatalf("GetByID(last run) error = %v", err)
	}
	if updated.LastRunAt == nil || updated.LastRunAt.IsZero() {
		t.Fatalf("LastRunAt = %#v, want non-nil", updated.LastRunAt)
	}

	if err := repo.Delete(ctx, item.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	if _, err := repo.GetByID(ctx, item.ID); err == nil {
		t.Fatalf("GetByID() after delete error = nil, want not found")
	}
}
