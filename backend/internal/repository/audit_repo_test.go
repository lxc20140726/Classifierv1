package repository

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestAuditRepositoryWriteGetAndList(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewAuditRepository(database)
	ctx := context.Background()

	fixtures := []*AuditLog{
		{
			ID:            "a1",
			JobID:         "job-1",
			WorkflowRunID: "wr-1",
			NodeRunID:     "nr-1",
			NodeID:        "node-1",
			NodeType:      "move-node",
			FolderID:      "folder-1",
			FolderPath:    "/media/a",
			Action:        "rename",
			Level:         "info",
			Detail:        json.RawMessage(`{"old":"a","new":"b"}`),
			Result:        "success",
			DurationMs:    11,
		},
		{
			ID:            "a2",
			JobID:         "job-2",
			WorkflowRunID: "wr-2",
			NodeRunID:     "nr-2",
			NodeID:        "node-2",
			NodeType:      "compress-node",
			FolderID:      "folder-2",
			FolderPath:    "/media/b",
			Action:        "move",
			Level:         "error",
			Result:        "failed",
			ErrorMsg:      "permission denied",
			DurationMs:    22,
		},
		{
			ID:         "a3",
			FolderPath: "/media/c",
			Action:     "rename",
			Level:      "info",
			Result:     "success",
			DurationMs: 9,
		},
	}

	for _, fixture := range fixtures {
		if err := repo.Write(ctx, fixture); err != nil {
			t.Fatalf("Write(%s) error = %v", fixture.ID, err)
		}
	}

	got, err := repo.GetByID(ctx, "a1")
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.Action != "rename" || got.Result != "success" {
		t.Fatalf("GetByID() action/result = %q/%q, want rename/success", got.Action, got.Result)
	}
	if got.WorkflowRunID != "wr-1" || got.NodeRunID != "nr-1" || got.NodeID != "node-1" || got.NodeType != "move-node" {
		t.Fatalf("GetByID() refs = run:%q nodeRun:%q node:%q type:%q", got.WorkflowRunID, got.NodeRunID, got.NodeID, got.NodeType)
	}

	if string(got.Detail) != `{"old":"a","new":"b"}` {
		t.Fatalf("GetByID() detail = %s, want expected JSON", string(got.Detail))
	}

	if got.CreatedAt.IsZero() {
		t.Fatalf("expected created_at to be set")
	}

	tests := []struct {
		name      string
		filter    AuditListFilter
		wantTotal int
		wantLen   int
	}{
		{name: "all", filter: AuditListFilter{Page: 1, Limit: 10}, wantTotal: 3, wantLen: 3},
		{name: "filter action", filter: AuditListFilter{Action: "rename", Page: 1, Limit: 10}, wantTotal: 2, wantLen: 2},
		{name: "filter result", filter: AuditListFilter{Result: "failed", Page: 1, Limit: 10}, wantTotal: 1, wantLen: 1},
		{name: "filter workflow run", filter: AuditListFilter{WorkflowRunID: "wr-1", Page: 1, Limit: 10}, wantTotal: 1, wantLen: 1},
		{name: "filter node run", filter: AuditListFilter{NodeRunID: "nr-2", Page: 1, Limit: 10}, wantTotal: 1, wantLen: 1},
		{name: "filter node id", filter: AuditListFilter{NodeID: "node-2", Page: 1, Limit: 10}, wantTotal: 1, wantLen: 1},
		{name: "filter node type", filter: AuditListFilter{NodeType: "move-node", Page: 1, Limit: 10}, wantTotal: 1, wantLen: 1},
		{name: "filter folder", filter: AuditListFilter{FolderID: "folder-1", Page: 1, Limit: 10}, wantTotal: 1, wantLen: 1},
		{name: "filter folder path keyword", filter: AuditListFilter{FolderPathKeyword: "/media/b", Page: 1, Limit: 10}, wantTotal: 1, wantLen: 1},
		{name: "pagination", filter: AuditListFilter{Page: 2, Limit: 2}, wantTotal: 3, wantLen: 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			items, total, err := repo.List(ctx, tc.filter)
			if err != nil {
				t.Fatalf("List() error = %v", err)
			}

			if total != tc.wantTotal {
				t.Fatalf("List() total = %d, want %d", total, tc.wantTotal)
			}

			if len(items) != tc.wantLen {
				t.Fatalf("List() len = %d, want %d", len(items), tc.wantLen)
			}
		})
	}
}

func TestAuditRepositoryListTimeRange(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewAuditRepository(database)
	ctx := context.Background()

	if err := repo.Write(ctx, &AuditLog{ID: "t1", FolderPath: "/a", Action: "rename", Level: "info", Result: "success"}); err != nil {
		t.Fatalf("Write(t1) error = %v", err)
	}

	if err := repo.Write(ctx, &AuditLog{ID: "t2", FolderPath: "/b", Action: "move", Level: "info", Result: "success"}); err != nil {
		t.Fatalf("Write(t2) error = %v", err)
	}

	mid := time.Now().UTC().Add(-5 * time.Minute)
	if _, err := database.ExecContext(ctx, "UPDATE audit_logs SET created_at = ? WHERE id = ?", mid.Add(-1*time.Minute).Format("2006-01-02 15:04:05"), "t1"); err != nil {
		t.Fatalf("update created_at t1 error = %v", err)
	}

	if _, err := database.ExecContext(ctx, "UPDATE audit_logs SET created_at = ? WHERE id = ?", mid.Add(1*time.Minute).Format("2006-01-02 15:04:05"), "t2"); err != nil {
		t.Fatalf("update created_at t2 error = %v", err)
	}

	items, total, err := repo.List(ctx, AuditListFilter{From: mid, Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("List(From) error = %v", err)
	}

	if total != 1 || len(items) != 1 {
		t.Fatalf("List(From) total/len = %d/%d, want 1/1", total, len(items))
	}

	if items[0].ID != "t2" {
		t.Fatalf("List(From)[0].ID = %q, want t2", items[0].ID)
	}

	items, total, err = repo.List(ctx, AuditListFilter{To: mid, Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("List(To) error = %v", err)
	}

	if total != 1 || len(items) != 1 {
		t.Fatalf("List(To) total/len = %d/%d, want 1/1", total, len(items))
	}

	if items[0].ID != "t1" {
		t.Fatalf("List(To)[0].ID = %q, want t1", items[0].ID)
	}
}

func TestAuditRepositoryNotFound(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewAuditRepository(database)

	_, err := repo.GetByID(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByID(missing) error = %v, want ErrNotFound", err)
	}
}
