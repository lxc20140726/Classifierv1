package repository

import (
	"context"
	"errors"
	"testing"
)

func TestFolderRepositoryUpsertAndGetters(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewFolderRepository(database)
	ctx := context.Background()

	folder := &Folder{
		ID:             "folder-1",
		Path:           "/media/a",
		Name:           "a",
		Category:       "photo",
		CategorySource: "auto",
		Status:         "pending",
		ImageCount:     10,
		VideoCount:     1,
		OtherFileCount: 2,
		HasOtherFiles:  true,
		TotalFiles:     11,
		TotalSize:      1024,
		MarkedForMove:  true,
		CoverImagePath: "/covers/a.jpg",
	}

	if err := repo.Upsert(ctx, folder); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	updated := *folder
	updated.Name = "renamed"
	updated.Status = "done"
	updated.MarkedForMove = false
	if err := repo.Upsert(ctx, &updated); err != nil {
		t.Fatalf("Upsert(updated) error = %v", err)
	}

	byID, err := repo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if byID.Name != updated.Name {
		t.Fatalf("GetByID().Name = %q, want %q", byID.Name, updated.Name)
	}

	if byID.Status != updated.Status {
		t.Fatalf("GetByID().Status = %q, want %q", byID.Status, updated.Status)
	}

	if byID.MarkedForMove != updated.MarkedForMove {
		t.Fatalf("GetByID().MarkedForMove = %v, want %v", byID.MarkedForMove, updated.MarkedForMove)
	}
	if byID.OtherFileCount != updated.OtherFileCount || byID.HasOtherFiles != updated.HasOtherFiles {
		t.Fatalf("GetByID() other stats = %d/%v, want %d/%v", byID.OtherFileCount, byID.HasOtherFiles, updated.OtherFileCount, updated.HasOtherFiles)
	}
	if byID.CoverImagePath != updated.CoverImagePath {
		t.Fatalf("GetByID().CoverImagePath = %q, want %q", byID.CoverImagePath, updated.CoverImagePath)
	}

	if byID.ScannedAt.IsZero() || byID.UpdatedAt.IsZero() {
		t.Fatalf("expected non-zero timestamps, got scanned_at=%v updated_at=%v", byID.ScannedAt, byID.UpdatedAt)
	}

	byPath, err := repo.GetByPath(ctx, folder.Path)
	if err != nil {
		t.Fatalf("GetByPath() error = %v", err)
	}

	if byPath.ID != folder.ID {
		t.Fatalf("GetByPath().ID = %q, want %q", byPath.ID, folder.ID)
	}
}

func TestFolderRepositoryList(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewFolderRepository(database)
	ctx := context.Background()

	fixtures := []*Folder{
		{ID: "f1", Path: "/media/photo-a", Name: "photo-a", Category: "photo", CategorySource: "auto", Status: "pending"},
		{ID: "f2", Path: "/media/video-a", Name: "video-a", Category: "video", CategorySource: "auto", Status: "done"},
		{ID: "f3", Path: "/media/photo-b", Name: "photo-b", Category: "photo", CategorySource: "manual", Status: "done"},
	}

	for _, fixture := range fixtures {
		if err := repo.Upsert(ctx, fixture); err != nil {
			t.Fatalf("Upsert(%s) error = %v", fixture.ID, err)
		}
	}

	tests := []struct {
		name      string
		filter    FolderListFilter
		wantTotal int
		wantLen   int
	}{
		{
			name:      "no filter returns all",
			filter:    FolderListFilter{Page: 1, Limit: 10},
			wantTotal: 3,
			wantLen:   3,
		},
		{
			name:      "filter by status",
			filter:    FolderListFilter{Status: "done", Page: 1, Limit: 10},
			wantTotal: 2,
			wantLen:   2,
		},
		{
			name:      "filter by category",
			filter:    FolderListFilter{Category: "photo", Page: 1, Limit: 10},
			wantTotal: 2,
			wantLen:   2,
		},
		{
			name:      "query filter",
			filter:    FolderListFilter{Q: "video", Page: 1, Limit: 10},
			wantTotal: 1,
			wantLen:   1,
		},
		{
			name:      "pagination",
			filter:    FolderListFilter{Page: 2, Limit: 2},
			wantTotal: 3,
			wantLen:   1,
		},
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

func TestFolderRepositoryListByPathPrefixMatchesMixedSeparators(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewFolderRepository(database)
	ctx := context.Background()

	fixtures := []*Folder{
		{ID: "root", Path: `E:\TEST\sample\yourpersonalwaifu`, Name: "yourpersonalwaifu", Category: "mixed", CategorySource: "auto", Status: "pending"},
		{ID: "images", Path: "E:/TEST/sample/yourpersonalwaifu/Images", Name: "Images", Category: "photo", CategorySource: "workflow", Status: "pending"},
		{ID: "videos", Path: "E:/TEST/sample/yourpersonalwaifu/Videos", Name: "Videos", Category: "video", CategorySource: "workflow", Status: "pending"},
		{ID: "sibling", Path: "E:/TEST/sample/yourpersonalwaifu-other", Name: "yourpersonalwaifu-other", Category: "other", CategorySource: "auto", Status: "pending"},
	}
	for _, fixture := range fixtures {
		if err := repo.Upsert(ctx, fixture); err != nil {
			t.Fatalf("Upsert(%s) error = %v", fixture.ID, err)
		}
	}

	current, err := repo.GetCurrentByPath(ctx, "E:/TEST/sample/yourpersonalwaifu")
	if err != nil {
		t.Fatalf("GetCurrentByPath() error = %v", err)
	}
	if current.ID != "root" {
		t.Fatalf("GetCurrentByPath().ID = %q, want root", current.ID)
	}

	items, err := repo.ListByPathPrefix(ctx, `E:\TEST\sample\yourpersonalwaifu`)
	if err != nil {
		t.Fatalf("ListByPathPrefix() error = %v", err)
	}

	gotIDs := make([]string, 0, len(items))
	for _, item := range items {
		gotIDs = append(gotIDs, item.ID)
	}
	wantIDs := []string{"root", "images", "videos"}
	if len(gotIDs) != len(wantIDs) {
		t.Fatalf("ListByPathPrefix() IDs = %#v, want %#v", gotIDs, wantIDs)
	}
	for i, wantID := range wantIDs {
		if gotIDs[i] != wantID {
			t.Fatalf("ListByPathPrefix() IDs = %#v, want %#v", gotIDs, wantIDs)
		}
	}
}

func TestFolderRepositoryListSortsByUpdatedAtAndSize(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewFolderRepository(database)
	ctx := context.Background()

	fixtures := []*Folder{
		{ID: "f-small", Path: "/media/small", Name: "small", Category: "photo", CategorySource: "auto", Status: "pending", TotalSize: 100},
		{ID: "f-large", Path: "/media/large", Name: "large", Category: "photo", CategorySource: "auto", Status: "pending", TotalSize: 500},
		{ID: "f-medium", Path: "/media/medium", Name: "medium", Category: "photo", CategorySource: "auto", Status: "pending", TotalSize: 300},
	}

	for _, fixture := range fixtures {
		if err := repo.Upsert(ctx, fixture); err != nil {
			t.Fatalf("Upsert(%s) error = %v", fixture.ID, err)
		}
	}

	for id, updatedAt := range map[string]string{
		"f-small":  "2026-01-01 00:00:01",
		"f-large":  "2026-01-01 00:00:03",
		"f-medium": "2026-01-01 00:00:02",
	} {
		if _, err := database.ExecContext(ctx, "UPDATE folders SET updated_at = ? WHERE id = ?", updatedAt, id); err != nil {
			t.Fatalf("update folders.updated_at(%s) error = %v", id, err)
		}
	}

	tests := []struct {
		name      string
		filter    FolderListFilter
		wantOrder []string
	}{
		{
			name:      "updated_at desc by default",
			filter:    FolderListFilter{Page: 1, Limit: 10},
			wantOrder: []string{"f-large", "f-medium", "f-small"},
		},
		{
			name:      "updated_at asc",
			filter:    FolderListFilter{Page: 1, Limit: 10, SortBy: "updated_at", SortOrder: "asc"},
			wantOrder: []string{"f-small", "f-medium", "f-large"},
		},
		{
			name:      "total_size desc",
			filter:    FolderListFilter{Page: 1, Limit: 10, SortBy: "total_size", SortOrder: "desc"},
			wantOrder: []string{"f-large", "f-medium", "f-small"},
		},
		{
			name:      "total_size asc",
			filter:    FolderListFilter{Page: 1, Limit: 10, SortBy: "total_size", SortOrder: "asc"},
			wantOrder: []string{"f-small", "f-medium", "f-large"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			items, _, err := repo.List(ctx, tc.filter)
			if err != nil {
				t.Fatalf("List() error = %v", err)
			}
			if len(items) != len(tc.wantOrder) {
				t.Fatalf("len(items) = %d, want %d", len(items), len(tc.wantOrder))
			}
			for i, wantID := range tc.wantOrder {
				if items[i].ID != wantID {
					t.Fatalf("items[%d].ID = %q, want %q", i, items[i].ID, wantID)
				}
			}
		})
	}
}

func TestFolderRepositoryListWorkflowSummariesByFolderIDs(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewFolderRepository(database)
	workflowRunRepo := NewWorkflowRunRepository(database)
	nodeRunRepo := NewNodeRunRepository(database)
	ctx := context.Background()

	folders := []*Folder{
		{ID: "f-not-run", Path: "/media/not-run", Name: "not-run", Category: "other", CategorySource: "auto", Status: "pending"},
		{ID: "f-classify", Path: "/media/classify", Name: "classify", Category: "other", CategorySource: "auto", Status: "pending"},
		{ID: "f-process", Path: "/media/process", Name: "process", Category: "other", CategorySource: "auto", Status: "pending"},
		{ID: "f-process-router", Path: "/media/process-router", Name: "process-router", Category: "other", CategorySource: "auto", Status: "pending"},
		{ID: "f-failed", Path: "/media/failed", Name: "failed", Category: "other", CategorySource: "auto", Status: "pending"},
		{ID: "f-wait", Path: "/media/wait", Name: "wait", Category: "other", CategorySource: "auto", Status: "pending"},
		{ID: "f-rolled", Path: "/media/rolled", Name: "rolled", Category: "other", CategorySource: "auto", Status: "pending"},
	}
	for _, folder := range folders {
		if err := repo.Upsert(ctx, folder); err != nil {
			t.Fatalf("Upsert(%s) error = %v", folder.ID, err)
		}
	}

	mustCreateWorkflowRun := func(run *WorkflowRun) {
		t.Helper()
		if err := workflowRunRepo.Create(ctx, run); err != nil {
			t.Fatalf("workflowRunRepo.Create(%s) error = %v", run.ID, err)
		}
	}
	mustCreateNodeRun := func(run *NodeRun) {
		t.Helper()
		if err := nodeRunRepo.Create(ctx, run); err != nil {
			t.Fatalf("nodeRunRepo.Create(%s) error = %v", run.ID, err)
		}
	}
	mustSetWorkflowRunUpdatedAt := func(runID, at string) {
		t.Helper()
		if _, err := database.ExecContext(ctx, "UPDATE workflow_runs SET updated_at = ? WHERE id = ?", at, runID); err != nil {
			t.Fatalf("update workflow_runs.updated_at(%s) error = %v", runID, err)
		}
	}

	mustCreateWorkflowRun(&WorkflowRun{ID: "wr-classify", JobID: "job-classify", FolderID: "f-classify", WorkflowDefID: "def", Status: "succeeded"})
	mustCreateNodeRun(&NodeRun{ID: "nr-classify-writer", WorkflowRunID: "wr-classify", NodeID: "writer", NodeType: "classification-writer", Sequence: 1, Status: "succeeded"})
	mustSetWorkflowRunUpdatedAt("wr-classify", "2026-01-01 00:00:01")

	if _, err := database.ExecContext(ctx, `INSERT INTO snapshots (
id, job_id, folder_id, operation_type, before_state, after_state, detail, status, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"snap-classify-derived",
		"job-classify-derived",
		"f-classify",
		"classify",
		`{"category":"other"}`,
		`{"category":"photo"}`,
		`{"node_type":"classification-writer"}`,
		"committed",
		"2026-01-01 00:00:07",
	); err != nil {
		t.Fatalf("insert classify snapshot error = %v", err)
	}
	mustCreateWorkflowRun(&WorkflowRun{ID: "wr-classify-derived", JobID: "job-classify-derived", WorkflowDefID: "def", Status: "succeeded"})
	mustCreateNodeRun(&NodeRun{ID: "nr-classify-derived-writer", WorkflowRunID: "wr-classify-derived", NodeID: "writer", NodeType: "classification-writer", Sequence: 1, Status: "succeeded"})
	mustSetWorkflowRunUpdatedAt("wr-classify-derived", "2026-01-01 00:00:07")

	mustCreateWorkflowRun(&WorkflowRun{ID: "wr-process", JobID: "job-process", FolderID: "f-process", WorkflowDefID: "def", Status: "succeeded"})
	mustCreateNodeRun(&NodeRun{ID: "nr-process-move", WorkflowRunID: "wr-process", NodeID: "move", NodeType: "move-node", Sequence: 1, Status: "succeeded"})
	mustSetWorkflowRunUpdatedAt("wr-process", "2026-01-01 00:00:02")

	mustCreateWorkflowRun(&WorkflowRun{ID: "wr-process-router", JobID: "job-process-router", FolderID: "f-process-router", WorkflowDefID: "def", Status: "succeeded"})
	mustCreateNodeRun(&NodeRun{ID: "nr-process-router-reader", WorkflowRunID: "wr-process-router", NodeID: "reader", NodeType: "db-subtree-reader", Sequence: 1, Status: "succeeded"})
	mustCreateNodeRun(&NodeRun{ID: "nr-process-router-category", WorkflowRunID: "wr-process-router", NodeID: "router", NodeType: "category-router", Sequence: 2, Status: "succeeded"})
	mustCreateNodeRun(&NodeRun{ID: "nr-process-router-move", WorkflowRunID: "wr-process-router", NodeID: "move", NodeType: "move-node", Sequence: 3, Status: "succeeded"})
	mustSetWorkflowRunUpdatedAt("wr-process-router", "2026-01-01 00:00:09")

	mustCreateWorkflowRun(&WorkflowRun{ID: "wr-process-derived", JobID: "job-process-derived", WorkflowDefID: "def", Status: "succeeded"})
	mustCreateNodeRun(&NodeRun{ID: "nr-process-derived-move", WorkflowRunID: "wr-process-derived", NodeID: "move", NodeType: "move-node", Sequence: 1, Status: "succeeded"})
	if _, err := database.ExecContext(ctx, `INSERT INTO processing_review_items (
id, workflow_run_id, job_id, folder_id, status, before_json, after_json, step_results_json, diff_json, error, created_at, updated_at, reviewed_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		"review-process-derived",
		"wr-process-derived",
		"job-process-derived",
		"f-process",
		"approved",
		`{"path":"/media/process"}`,
		`{"path":"/media/process"}`,
		`[]`,
		`{}`,
		"",
		"2026-01-01 00:00:08",
		"2026-01-01 00:00:08",
		"2026-01-01 00:00:08",
	); err != nil {
		t.Fatalf("insert processing review error = %v", err)
	}
	mustSetWorkflowRunUpdatedAt("wr-process-derived", "2026-01-01 00:00:08")

	mustCreateWorkflowRun(&WorkflowRun{ID: "wr-failed-old", JobID: "job-failed-old", FolderID: "f-failed", WorkflowDefID: "def", Status: "succeeded"})
	mustCreateNodeRun(&NodeRun{ID: "nr-failed-old-move", WorkflowRunID: "wr-failed-old", NodeID: "move", NodeType: "move-node", Sequence: 1, Status: "succeeded"})
	mustSetWorkflowRunUpdatedAt("wr-failed-old", "2026-01-01 00:00:03")
	mustCreateWorkflowRun(&WorkflowRun{ID: "wr-failed-new", JobID: "job-failed-new", FolderID: "f-failed", WorkflowDefID: "def", Status: "failed"})
	mustCreateNodeRun(&NodeRun{ID: "nr-failed-new-move", WorkflowRunID: "wr-failed-new", NodeID: "move", NodeType: "move-node", Sequence: 1, Status: "failed"})
	mustSetWorkflowRunUpdatedAt("wr-failed-new", "2026-01-01 00:00:04")

	mustCreateWorkflowRun(&WorkflowRun{ID: "wr-wait", JobID: "job-wait", FolderID: "f-wait", WorkflowDefID: "def", Status: "waiting_input"})
	mustCreateNodeRun(&NodeRun{ID: "nr-wait-keyword", WorkflowRunID: "wr-wait", NodeID: "kw", NodeType: "name-keyword-classifier", Sequence: 1, Status: "waiting_input"})
	mustSetWorkflowRunUpdatedAt("wr-wait", "2026-01-01 00:00:05")

	mustCreateWorkflowRun(&WorkflowRun{ID: "wr-rolled", JobID: "job-rolled", FolderID: "f-rolled", WorkflowDefID: "def", Status: "rolled_back"})
	mustCreateNodeRun(&NodeRun{ID: "nr-rolled-move", WorkflowRunID: "wr-rolled", NodeID: "move", NodeType: "move-node", Sequence: 1, Status: "succeeded"})
	mustSetWorkflowRunUpdatedAt("wr-rolled", "2026-01-01 00:00:06")

	summaries, err := repo.ListWorkflowSummariesByFolderIDs(ctx, []string{
		"f-not-run",
		"f-classify",
		"f-process",
		"f-process-router",
		"f-failed",
		"f-wait",
		"f-rolled",
		"f-not-run",
		"",
	})
	if err != nil {
		t.Fatalf("ListWorkflowSummariesByFolderIDs() error = %v", err)
	}

	if got := summaries["f-not-run"].Classification.Status; got != "not_run" {
		t.Fatalf("f-not-run classification = %q, want not_run", got)
	}
	if got := summaries["f-not-run"].Processing.Status; got != "not_run" {
		t.Fatalf("f-not-run processing = %q, want not_run", got)
	}

	if got := summaries["f-classify"].Classification.Status; got != "succeeded" {
		t.Fatalf("f-classify classification = %q, want succeeded", got)
	}
	if got := summaries["f-classify"].Classification.WorkflowRunID; got != "wr-classify-derived" {
		t.Fatalf("f-classify classification workflow_run_id = %q, want wr-classify-derived", got)
	}
	if got := summaries["f-classify"].Processing.Status; got != "not_run" {
		t.Fatalf("f-classify processing = %q, want not_run", got)
	}

	if got := summaries["f-process"].Processing.Status; got != "succeeded" {
		t.Fatalf("f-process processing = %q, want succeeded", got)
	}
	if got := summaries["f-process"].Processing.WorkflowRunID; got != "wr-process-derived" {
		t.Fatalf("f-process processing workflow_run_id = %q, want wr-process-derived", got)
	}
	if got := summaries["f-process"].Classification.Status; got != "not_run" {
		t.Fatalf("f-process classification = %q, want not_run", got)
	}

	if got := summaries["f-process-router"].Processing.Status; got != "succeeded" {
		t.Fatalf("f-process-router processing = %q, want succeeded", got)
	}
	if got := summaries["f-process-router"].Classification.Status; got != "not_run" {
		t.Fatalf("f-process-router classification = %q, want not_run", got)
	}

	if got := summaries["f-failed"].Processing.Status; got != "failed" {
		t.Fatalf("f-failed processing = %q, want failed", got)
	}
	if got := summaries["f-wait"].Classification.Status; got != "waiting_input" {
		t.Fatalf("f-wait classification = %q, want waiting_input", got)
	}
	if got := summaries["f-rolled"].Processing.Status; got != "rolled_back" {
		t.Fatalf("f-rolled processing = %q, want rolled_back", got)
	}
}

func TestFolderRepositoryUpdatesAndDelete(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewFolderRepository(database)
	ctx := context.Background()

	folder := &Folder{
		ID:             "folder-update",
		Path:           "/media/update",
		Name:           "update",
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
	}

	if err := repo.Upsert(ctx, folder); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	if err := repo.UpdateCategory(ctx, folder.ID, "video", "manual"); err != nil {
		t.Fatalf("UpdateCategory() error = %v", err)
	}

	if err := repo.UpdateStatus(ctx, folder.ID, "done"); err != nil {
		t.Fatalf("UpdateStatus() error = %v", err)
	}

	if err := repo.UpdatePath(ctx, folder.ID, "/media/new-path", "/media", "new-path"); err != nil {
		t.Fatalf("UpdatePath() error = %v", err)
	}

	if err := repo.UpdateCoverImagePath(ctx, folder.ID, "/covers/new-path.jpg"); err != nil {
		t.Fatalf("UpdateCoverImagePath() error = %v", err)
	}

	got, err := repo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if got.Category != "video" || got.CategorySource != "manual" {
		t.Fatalf("category/source = %q/%q, want video/manual", got.Category, got.CategorySource)
	}

	if got.Status != "done" {
		t.Fatalf("status = %q, want done", got.Status)
	}

	if got.Path != "/media/new-path" {
		t.Fatalf("path = %q, want /media/new-path", got.Path)
	}
	if got.Name != "new-path" {
		t.Fatalf("name = %q, want new-path", got.Name)
	}

	if got.CoverImagePath != "/covers/new-path.jpg" {
		t.Fatalf("cover_image_path = %q, want /covers/new-path.jpg", got.CoverImagePath)
	}

	if err := repo.Delete(ctx, folder.ID); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}

	_, err = repo.GetByID(ctx, folder.ID)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByID(after delete) error = %v, want ErrNotFound", err)
	}
}

func TestFolderRepositoryNotFoundMutations(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewFolderRepository(database)
	ctx := context.Background()

	tests := []struct {
		name string
		fn   func() error
	}{
		{name: "UpdateCategory missing", fn: func() error { return repo.UpdateCategory(ctx, "missing", "photo", "auto") }},
		{name: "UpdateStatus missing", fn: func() error { return repo.UpdateStatus(ctx, "missing", "done") }},
		{name: "UpdatePath missing", fn: func() error { return repo.UpdatePath(ctx, "missing", "/new", "/media", "new") }},
		{name: "UpdateCoverImagePath missing", fn: func() error { return repo.UpdateCoverImagePath(ctx, "missing", "/cover.jpg") }},
		{name: "Delete missing", fn: func() error { return repo.Delete(ctx, "missing") }},
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

func TestFolderRepositoryPathObservationsAndHistory(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewFolderRepository(database)
	ctx := context.Background()

	folder := &Folder{
		ID:             "folder-history",
		Path:           "/media/original",
		SourceDir:      "/media",
		RelativePath:   "original",
		Name:           "original",
		Category:       "photo",
		CategorySource: "auto",
		Status:         "pending",
	}
	if err := repo.Upsert(ctx, folder); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	if err := repo.UpdatePath(ctx, folder.ID, "/archive/original", "/archive", "original"); err != nil {
		t.Fatalf("UpdatePath() error = %v", err)
	}

	current, err := repo.GetCurrentByPath(ctx, "/archive/original")
	if err != nil {
		t.Fatalf("GetCurrentByPath(new) error = %v", err)
	}
	if current.ID != folder.ID {
		t.Fatalf("GetCurrentByPath(new).ID = %q, want %q", current.ID, folder.ID)
	}
	if current.Name != "original" {
		t.Fatalf("GetCurrentByPath(new).Name = %q, want original", current.Name)
	}

	relativeReader, ok := repo.(interface {
		ListByRelativePath(ctx context.Context, relativePath string) ([]*Folder, error)
	})
	if !ok {
		t.Fatalf("folder repository does not expose ListByRelativePath")
	}

	byRelativePath, err := relativeReader.ListByRelativePath(ctx, "original")
	if err != nil {
		t.Fatalf("ListByRelativePath() error = %v", err)
	}
	if len(byRelativePath) != 1 || byRelativePath[0].ID != folder.ID {
		t.Fatalf("ListByRelativePath() = %#v, want only %q", byRelativePath, folder.ID)
	}

	historical, err := repo.GetByHistoricalPath(ctx, "/media/original")
	if err != nil {
		t.Fatalf("GetByHistoricalPath(old) error = %v", err)
	}
	if historical.ID != folder.ID {
		t.Fatalf("GetByHistoricalPath(old).ID = %q, want %q", historical.ID, folder.ID)
	}

	if _, err := repo.GetCurrentByPath(ctx, "/media/original"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetCurrentByPath(old) error = %v, want ErrNotFound", err)
	}
}

func TestFolderRepositoryUpdatePathReassignsCurrentSourceRelativeObservation(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewFolderRepository(database)
	ctx := context.Background()

	first := &Folder{
		ID:             "folder-first",
		Path:           "/library/first",
		SourceDir:      "/library",
		RelativePath:   "first",
		Name:           "first",
		Category:       "photo",
		CategorySource: "auto",
		Status:         "pending",
	}
	if err := repo.Upsert(ctx, first); err != nil {
		t.Fatalf("Upsert(first) error = %v", err)
	}

	second := &Folder{
		ID:             "folder-second",
		Path:           "/archive/second",
		SourceDir:      "/archive",
		RelativePath:   "second",
		Name:           "second",
		Category:       "photo",
		CategorySource: "auto",
		Status:         "pending",
	}
	if err := repo.Upsert(ctx, second); err != nil {
		t.Fatalf("Upsert(second) error = %v", err)
	}

	if err := repo.UpdatePath(ctx, second.ID, "/media/other-name", "/library", "first"); err != nil {
		t.Fatalf("UpdatePath() error = %v", err)
	}

	got, err := repo.GetCurrentBySourceAndRelativePath(ctx, "/library", "first")
	if err != nil {
		t.Fatalf("GetCurrentBySourceAndRelativePath() error = %v", err)
	}
	if got.ID != second.ID {
		t.Fatalf("GetCurrentBySourceAndRelativePath().ID = %q, want %q", got.ID, second.ID)
	}

	resolved, matchType, err := repo.ResolveScanTarget(ctx, "/does-not-exist", "/library", "first")
	if err != nil {
		t.Fatalf("ResolveScanTarget() error = %v", err)
	}
	if resolved == nil || resolved.ID != second.ID || matchType != FolderScanMatchTypeSourceRelativeMatch {
		t.Fatalf("ResolveScanTarget() = (%v, %q, %q), want (%q, %q)", resolved != nil, resolved.ID, matchType, second.ID, FolderScanMatchTypeSourceRelativeMatch)
	}
}

func TestFolderRepositoryResolveScanTarget(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewFolderRepository(database)
	ctx := context.Background()

	folder := &Folder{
		ID:             "folder-resolve",
		Path:           "/library/a",
		SourceDir:      "/library",
		RelativePath:   "a",
		Name:           "a",
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
	}
	if err := repo.Upsert(ctx, folder); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	got, matchType, err := repo.ResolveScanTarget(ctx, "/library/a", "/library", "a")
	if err != nil {
		t.Fatalf("ResolveScanTarget(current) error = %v", err)
	}
	if got.ID != folder.ID || matchType != FolderScanMatchTypeCurrentPathMatch {
		t.Fatalf("ResolveScanTarget(current) = (%v, %q), want (%q, %q)", got != nil, matchType, folder.ID, FolderScanMatchTypeCurrentPathMatch)
	}

	if err := repo.UpdatePath(ctx, folder.ID, "/archive/a", "/archive", "a"); err != nil {
		t.Fatalf("UpdatePath() error = %v", err)
	}

	got, matchType, err = repo.ResolveScanTarget(ctx, "/library/a", "/library", "zzz")
	if err != nil {
		t.Fatalf("ResolveScanTarget(history) error = %v", err)
	}
	if got.ID != folder.ID || matchType != FolderScanMatchTypeHistoricalPathMatch {
		t.Fatalf("ResolveScanTarget(history) = (%v, %q), want (%q, %q)", got != nil, matchType, folder.ID, FolderScanMatchTypeHistoricalPathMatch)
	}

	got, matchType, err = repo.ResolveScanTarget(ctx, "/library/new-name", "/archive", "a")
	if err != nil {
		t.Fatalf("ResolveScanTarget(source+relative) error = %v", err)
	}
	if got.ID != folder.ID || matchType != FolderScanMatchTypeSourceRelativeMatch {
		t.Fatalf("ResolveScanTarget(source+relative) = (%v, %q), want (%q, %q)", got != nil, matchType, folder.ID, FolderScanMatchTypeSourceRelativeMatch)
	}

	got, matchType, err = repo.ResolveScanTarget(ctx, "/library/brand-new", "/library", "brand-new")
	if err != nil {
		t.Fatalf("ResolveScanTarget(new) error = %v", err)
	}
	if got != nil || matchType != FolderScanMatchTypeNewDiscovery {
		t.Fatalf("ResolveScanTarget(new) = (%v, %q), want (nil, %q)", got, matchType, FolderScanMatchTypeNewDiscovery)
	}
}

func TestFolderRepositoryResolveScanTargetPrefersSourceRelativeOverHistoricalPath(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewFolderRepository(database)
	ctx := context.Background()

	historical := &Folder{
		ID:             "folder-historical",
		Path:           "/archive/a",
		SourceDir:      "/archive",
		RelativePath:   "a",
		Name:           "a",
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
	}
	if err := repo.Upsert(ctx, historical); err != nil {
		t.Fatalf("Upsert(historical) error = %v", err)
	}
	if err := repo.UpdatePath(ctx, historical.ID, "/archive/a-v2", "/archive", "a-v2"); err != nil {
		t.Fatalf("UpdatePath(historical) error = %v", err)
	}

	sourceRelative := &Folder{
		ID:             "folder-source-relative",
		Path:           "/library/current-a",
		SourceDir:      "/library",
		RelativePath:   "a",
		Name:           "a",
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
	}
	if err := repo.Upsert(ctx, sourceRelative); err != nil {
		t.Fatalf("Upsert(sourceRelative) error = %v", err)
	}

	got, matchType, err := repo.ResolveScanTarget(ctx, "/archive/a", "/library", "a")
	if err != nil {
		t.Fatalf("ResolveScanTarget() error = %v", err)
	}
	if got == nil {
		t.Fatalf("ResolveScanTarget() returned nil folder")
	}
	if got.ID != sourceRelative.ID {
		t.Fatalf("ResolveScanTarget().ID = %q, want %q", got.ID, sourceRelative.ID)
	}
	if matchType != FolderScanMatchTypeSourceRelativeMatch {
		t.Fatalf("ResolveScanTarget() matchType = %q, want %q", matchType, FolderScanMatchTypeSourceRelativeMatch)
	}
}

func TestFolderRepositoryListPathObservationsByFolderID(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewFolderRepository(database)
	ctx := context.Background()

	folder := &Folder{
		ID:             "folder-observations",
		Path:           "/media/a",
		SourceDir:      "/media",
		RelativePath:   "a",
		Name:           "a",
		Category:       "photo",
		CategorySource: "auto",
		Status:         "pending",
	}
	if err := repo.Upsert(ctx, folder); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}
	if err := repo.UpdatePath(ctx, folder.ID, "/archive/a", "/archive", "a"); err != nil {
		t.Fatalf("UpdatePath(archive) error = %v", err)
	}
	if err := repo.UpdatePath(ctx, folder.ID, "/target/a", "/target", "a"); err != nil {
		t.Fatalf("UpdatePath(target) error = %v", err)
	}

	reader, ok := repo.(interface {
		ListPathObservationsByFolderID(ctx context.Context, folderID string) ([]*FolderPathObservation, error)
	})
	if !ok {
		t.Fatalf("folder repository does not expose ListPathObservationsByFolderID")
	}

	items, err := reader.ListPathObservationsByFolderID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("ListPathObservationsByFolderID() error = %v", err)
	}
	if len(items) != 3 {
		t.Fatalf("len(items) = %d, want 3", len(items))
	}

	paths := map[string]bool{}
	currentCount := 0
	for _, item := range items {
		paths[item.Path] = true
		if item.IsCurrent {
			currentCount++
			if item.Path != "/target/a" {
				t.Fatalf("current observation path = %q, want /target/a", item.Path)
			}
		}
	}
	for _, expectedPath := range []string{"/media/a", "/archive/a", "/target/a"} {
		if !paths[expectedPath] {
			t.Fatalf("expected path %q not found in observations %#v", expectedPath, items)
		}
	}
	if currentCount != 1 {
		t.Fatalf("current observation count = %d, want 1", currentCount)
	}
}
