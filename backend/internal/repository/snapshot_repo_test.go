package repository

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

func TestSnapshotRepositoryCreateGetAndList(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewSnapshotRepository(database)
	ctx := context.Background()

	fixtures := []*Snapshot{
		{ID: "s1", JobID: "j1", FolderID: "f1", OperationType: "rename", Before: json.RawMessage(`{"a":1}`), Status: "pending"},
		{ID: "s2", JobID: "j1", FolderID: "f2", OperationType: "move", Before: json.RawMessage(`{"b":2}`), Status: "pending"},
		{ID: "s3", JobID: "j2", FolderID: "f1", OperationType: "compress", Before: json.RawMessage(`{"c":3}`), Status: "done"},
	}

	for _, fixture := range fixtures {
		if err := repo.Create(ctx, fixture); err != nil {
			t.Fatalf("Create(%s) error = %v", fixture.ID, err)
		}
	}

	got, err := repo.GetByID(ctx, "s1")
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if string(got.Before) != `{"a":1}` {
		t.Fatalf("GetByID().Before = %s, want %s", string(got.Before), `{"a":1}`)
	}

	if got.CreatedAt.IsZero() {
		t.Fatalf("expected created_at to be set")
	}

	byFolder, err := repo.ListByFolderID(ctx, "f1")
	if err != nil {
		t.Fatalf("ListByFolderID() error = %v", err)
	}

	if len(byFolder) != 2 {
		t.Fatalf("ListByFolderID() len = %d, want 2", len(byFolder))
	}

	byJob, err := repo.ListByJobID(ctx, "j1")
	if err != nil {
		t.Fatalf("ListByJobID() error = %v", err)
	}

	if len(byJob) != 2 {
		t.Fatalf("ListByJobID() len = %d, want 2", len(byJob))
	}
}

func TestSnapshotRepositoryCommitAfterAndUpdateStatus(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewSnapshotRepository(database)
	ctx := context.Background()

	snapshot := &Snapshot{
		ID:            "s-update",
		JobID:         "job-1",
		FolderID:      "folder-1",
		OperationType: "rename",
		Before:        json.RawMessage(`{"before":true}`),
		Status:        "pending",
	}

	if err := repo.Create(ctx, snapshot); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	after := json.RawMessage(`{"after":true}`)
	if err := repo.CommitAfter(ctx, snapshot.ID, after); err != nil {
		t.Fatalf("CommitAfter() error = %v", err)
	}

	if err := repo.UpdateStatus(ctx, snapshot.ID, "done"); err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}

	got, err := repo.GetByID(ctx, snapshot.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if string(got.After) != string(after) {
		t.Fatalf("after = %s, want %s", string(got.After), string(after))
	}

	if got.Status != "done" {
		t.Fatalf("status = %q, want done", got.Status)
	}
}

func TestSnapshotRepositoryNotFound(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewSnapshotRepository(database)
	ctx := context.Background()

	_, err := repo.GetByID(ctx, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByID(missing) error = %v, want ErrNotFound", err)
	}

	tests := []struct {
		name string
		fn   func() error
	}{
		{name: "CommitAfter missing", fn: func() error { return repo.CommitAfter(ctx, "missing", json.RawMessage(`{"x":1}`)) }},
		{name: "UpdateStatus missing", fn: func() error { return repo.UpdateStatus(ctx, "missing", "done") }},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.fn()
			if !errors.Is(err, ErrNotFound) {
				t.Fatalf("error = %v, want ErrNotFound", err)
			}
		})
	}
}
