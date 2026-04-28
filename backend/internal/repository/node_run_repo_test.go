package repository

import (
	"context"
	"errors"
	"testing"
)

func TestNodeRunRepository_CRUDFilterAndTransitions(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewNodeRunRepository(database).(*SQLiteNodeRunRepository)
	ctx := context.Background()

	items := []*NodeRun{
		{ID: "nr-1", WorkflowRunID: "wr-1", NodeID: "n-a", NodeType: "rename-node", Sequence: 1, Status: "pending"},
		{ID: "nr-2", WorkflowRunID: "wr-1", NodeID: "n-a", NodeType: "rename-node", Sequence: 2, Status: "waiting_input"},
		{ID: "nr-3", WorkflowRunID: "wr-1", NodeID: "n-b", NodeType: "move-node", Sequence: 3, Status: "pending"},
		{ID: "nr-4", WorkflowRunID: "wr-2", NodeID: "n-a", NodeType: "move-node", Sequence: 1, Status: "pending"},
	}
	for _, item := range items {
		if err := repo.Create(ctx, item); err != nil {
			t.Fatalf("Create(%s) error = %v", item.ID, err)
		}
	}

	list, total, err := repo.List(ctx, NodeRunListFilter{WorkflowRunID: "wr-1", Page: 1, Limit: 2})
	if err != nil {
		t.Fatalf("List(wr-1) error = %v", err)
	}
	if total != 3 || len(list) != 2 {
		t.Fatalf("List(wr-1) total/len = %d/%d, want 3/2", total, len(list))
	}
	if list[0].ID != "nr-1" || list[1].ID != "nr-2" {
		t.Fatalf("List(wr-1) order = %q,%q, want nr-1,nr-2", list[0].ID, list[1].ID)
	}

	empty, emptyTotal, err := repo.List(ctx, NodeRunListFilter{WorkflowRunID: "", Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("List(empty workflow_run_id) error = %v", err)
	}
	if emptyTotal != 0 || len(empty) != 0 {
		t.Fatalf("List(empty workflow_run_id) total/len = %d/%d, want 0/0", emptyTotal, len(empty))
	}

	latest, err := repo.GetLatestByNodeID(ctx, "wr-1", "n-a")
	if err != nil {
		t.Fatalf("GetLatestByNodeID() error = %v", err)
	}
	if latest.ID != "nr-2" {
		t.Fatalf("latest id = %q, want nr-2", latest.ID)
	}

	waiting, err := repo.GetWaitingInputByWorkflowRunID(ctx, "wr-1")
	if err != nil {
		t.Fatalf("GetWaitingInputByWorkflowRunID() error = %v", err)
	}
	if waiting.ID != "nr-2" {
		t.Fatalf("waiting id = %q, want nr-2", waiting.ID)
	}

	if err := repo.UpdateStart(ctx, "nr-1", `{"k":"v"}`); err != nil {
		t.Fatalf("UpdateStart() error = %v", err)
	}
	if err := repo.UpdateResumeData(ctx, "nr-1", `{"category":"photo"}`); err != nil {
		t.Fatalf("UpdateResumeData() error = %v", err)
	}
	if err := repo.SetResumeToken(ctx, "nr-1", "token-1"); err != nil {
		t.Fatalf("SetResumeToken() error = %v", err)
	}
	if err := repo.UpdateFinish(ctx, "nr-1", "succeeded", `{"ok":true}`, ""); err != nil {
		t.Fatalf("UpdateFinish() error = %v", err)
	}
	if err := repo.UpdateProgress(ctx, "nr-1", NodeRunProgress{
		Percent:    45,
		Done:       9,
		Total:      20,
		Stage:      "writing",
		Message:    "已写入 9/20",
		SourcePath: "/source/a.jpg",
		TargetPath: "/target/a.cbz",
	}); err != nil {
		t.Fatalf("UpdateProgress() error = %v", err)
	}
	updated, err := repo.GetByID(ctx, "nr-1")
	if err != nil {
		t.Fatalf("GetByID(nr-1) error = %v", err)
	}
	if updated.Status != "succeeded" || updated.ResumeData == "" || updated.ResumeToken != "token-1" {
		t.Fatalf("updated status/resume_data/resume_token = %q/%q/%q", updated.Status, updated.ResumeData, updated.ResumeToken)
	}
	if updated.StartedAt == nil || updated.FinishedAt == nil {
		t.Fatalf("started_at/finished_at = %#v/%#v, want non-nil", updated.StartedAt, updated.FinishedAt)
	}
	if updated.ProgressPercent == nil || *updated.ProgressPercent != 45 {
		t.Fatalf("progress_percent = %#v, want 45", updated.ProgressPercent)
	}
	if updated.ProgressDone == nil || *updated.ProgressDone != 9 {
		t.Fatalf("progress_done = %#v, want 9", updated.ProgressDone)
	}
	if updated.ProgressTotal == nil || *updated.ProgressTotal != 20 {
		t.Fatalf("progress_total = %#v, want 20", updated.ProgressTotal)
	}
	if updated.ProgressStage == nil || *updated.ProgressStage != "writing" {
		t.Fatalf("progress_stage = %#v, want writing", updated.ProgressStage)
	}
	if updated.ProgressUpdatedAt == nil {
		t.Fatalf("progress_updated_at = nil, want non-nil")
	}

	if err := repo.UpdateStart(ctx, "nr-2", `{"wait":true}`); err != nil {
		t.Fatalf("UpdateStart(nr-2) error = %v", err)
	}
	if err := repo.UpdateFinish(ctx, "nr-2", "waiting_input", `{"pending":true}`, "等待确认"); err != nil {
		t.Fatalf("UpdateFinish(waiting_input) error = %v", err)
	}
	waitingAfterFinish, err := repo.GetByID(ctx, "nr-2")
	if err != nil {
		t.Fatalf("GetByID(nr-2 waiting) error = %v", err)
	}
	if waitingAfterFinish.FinishedAt != nil {
		t.Fatalf("waiting_input finished_at = %#v, want nil before resume", waitingAfterFinish.FinishedAt)
	}
	if err := repo.FinishWaitingInput(ctx, "nr-2"); err != nil {
		t.Fatalf("FinishWaitingInput() error = %v", err)
	}
	waitingAfterResume, err := repo.GetByID(ctx, "nr-2")
	if err != nil {
		t.Fatalf("GetByID(nr-2 resumed) error = %v", err)
	}
	if waitingAfterResume.FinishedAt == nil {
		t.Fatalf("waiting_input finished_at = nil, want non-nil after resume")
	}
}

func TestNodeRunRepository_NotFoundBranches(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewNodeRunRepository(database).(*SQLiteNodeRunRepository)
	ctx := context.Background()

	_, err := repo.GetByID(ctx, "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetByID(missing) error = %v, want ErrNotFound", err)
	}
	_, err = repo.GetLatestByNodeID(ctx, "wr-1", "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetLatestByNodeID(missing) error = %v, want ErrNotFound", err)
	}
	_, err = repo.GetWaitingInputByWorkflowRunID(ctx, "wr-1")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetWaitingInputByWorkflowRunID(missing) error = %v, want ErrNotFound", err)
	}
	if err := repo.UpdateStart(ctx, "missing", `{}`); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateStart(missing) error = %v, want ErrNotFound", err)
	}
	if err := repo.UpdateFinish(ctx, "missing", "failed", `{}`, "boom"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateFinish(missing) error = %v, want ErrNotFound", err)
	}
	if err := repo.FinishWaitingInput(ctx, "missing"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("FinishWaitingInput(missing) error = %v, want ErrNotFound", err)
	}
	if err := repo.UpdateResumeData(ctx, "missing", `{}`); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateResumeData(missing) error = %v, want ErrNotFound", err)
	}
	if err := repo.SetResumeToken(ctx, "missing", "token"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("SetResumeToken(missing) error = %v, want ErrNotFound", err)
	}
	if err := repo.UpdateProgress(ctx, "missing", NodeRunProgress{Percent: 1, Done: 1, Total: 1}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("UpdateProgress(missing) error = %v, want ErrNotFound", err)
	}
}
