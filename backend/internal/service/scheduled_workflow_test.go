package service

import (
	"context"
	"reflect"
	"testing"

	"github.com/liqiye/classifier/internal/repository"
)

type stubScheduledWorkflowRunner struct {
	called []StartWorkflowJobInput
}

func (s *stubScheduledWorkflowRunner) StartJob(_ context.Context, input StartWorkflowJobInput) (string, error) {
	s.called = append(s.called, input)
	return "job-1", nil
}

type stubScheduledScanRunner struct {
	called [][]string
}

func (s *stubScheduledScanRunner) StartScheduledJob(_ context.Context, sourceDirs []string) (string, bool, error) {
	s.called = append(s.called, append([]string(nil), sourceDirs...))
	return "scan-job-1", true, nil
}

func TestScheduledWorkflowServiceRunNow(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	repo := repository.NewScheduledWorkflowRepository(database)
	runner := &stubScheduledWorkflowRunner{}
	scanRunner := &stubScheduledScanRunner{}
	svc := NewScheduledWorkflowService(repo, runner, scanRunner)

	created, err := svc.Create(context.Background(), "workflow", "每小时整理", "wf-1", "0 * * * *", true, []string{"folder-2", "folder-1", "folder-1"}, nil)
	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	jobID, err := svc.RunNow(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("RunNow() error = %v", err)
	}
	if jobID != "job-1" {
		t.Fatalf("jobID = %q, want job-1", jobID)
	}
	if len(runner.called) != 1 {
		t.Fatalf("runner called = %d, want 1", len(runner.called))
	}
	if runner.called[0].WorkflowDefID != "wf-1" {
		t.Fatalf("WorkflowDefID = %q, want wf-1", runner.called[0].WorkflowDefID)
	}
	loaded, err := repo.GetByID(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if loaded.LastRunAt == nil || loaded.LastRunAt.IsZero() {
		t.Fatalf("LastRunAt = %#v, want non-zero", loaded.LastRunAt)
	}
}

func TestScheduledWorkflowServiceRunNowScan(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	repo := repository.NewScheduledWorkflowRepository(database)
	runner := &stubScheduledWorkflowRunner{}
	scanRunner := &stubScheduledScanRunner{}
	svc := NewScheduledWorkflowService(repo, runner, scanRunner)

	created, err := svc.Create(context.Background(), "scan", "定时扫描", "", "*/15 * * * *", true, nil, []string{"/scan-b", "/scan-a", "/scan-a"})
	if err != nil {
		t.Fatalf("Create(scan) error = %v", err)
	}

	jobID, err := svc.RunNow(context.Background(), created.ID)
	if err != nil {
		t.Fatalf("RunNow(scan) error = %v", err)
	}
	if jobID != "scan-job-1" {
		t.Fatalf("jobID = %q, want scan-job-1", jobID)
	}
	if len(scanRunner.called) != 1 {
		t.Fatalf("scan runner called = %d, want 1", len(scanRunner.called))
	}
	if !reflect.DeepEqual(scanRunner.called[0], []string{"/scan-a", "/scan-b"}) {
		t.Fatalf("source dirs = %#v, want normalized dirs", scanRunner.called[0])
	}
	if len(runner.called) != 0 {
		t.Fatalf("workflow runner called = %d, want 0", len(runner.called))
	}
}
