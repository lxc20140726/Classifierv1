package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/sse"
)

type progressEventExecutor struct{}

func (e *progressEventExecutor) Type() string {
	return "progress-event-executor"
}

func (e *progressEventExecutor) Schema() NodeSchema {
	return NodeSchema{Type: e.Type(), Label: "Progress Event Executor", Description: "Emit one progress update for tests"}
}

func (e *progressEventExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	if input.ProgressFn != nil {
		input.ProgressFn(NodeProgressUpdate{
			Percent:    37,
			Done:       37,
			Total:      100,
			Stage:      "writing",
			Message:    "已处理 37/100",
			SourcePath: "/source/album/a.jpg",
			TargetPath: "/target/album.cbz",
		})
	}
	return NodeExecutionOutput{Status: ExecutionSuccess, Outputs: map[string]TypedValue{}}, nil
}

func (e *progressEventExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *progressEventExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func TestWorkflowRunNodeProgressEventContainsNodeRunIDAndSequence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)
	broker := sse.NewBroker()
	events := broker.Subscribe()
	defer broker.Unsubscribe(events)

	folder := &repository.Folder{
		ID:             "folder-progress-event",
		Path:           "/source/album",
		Name:           "album",
		SourceDir:      "/source",
		RelativePath:   "album",
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "n1", Type: "trigger", Enabled: true},
			{ID: "n2", Type: "progress-event-executor", Enabled: true},
		},
		Edges: []repository.WorkflowGraphEdge{{ID: "e1", Source: "n1", Target: "n2"}},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{
		ID:        "wf-progress-event",
		Name:      "wf-progress-event",
		GraphJSON: string(graphJSON),
		IsActive:  true,
		Version:   1,
	}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	svc := NewWorkflowRunnerService(
		jobRepo,
		folderRepo,
		repository.NewSnapshotRepository(database),
		workflowDefRepo,
		workflowRunRepo,
		nodeRunRepo,
		nodeSnapshotRepo,
		fs.NewMockAdapter(),
		broker,
		nil,
	)
	svc.RegisterExecutor(&progressEventExecutor{})

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}
	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}

	timeout := time.After(3 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatal("timed out waiting workflow_run.node_progress")
		case event := <-events:
			if event.Type != "workflow_run.node_progress" {
				continue
			}
			var payload struct {
				WorkflowRunID string `json:"workflow_run_id"`
				NodeRunID     string `json:"node_run_id"`
				NodeID        string `json:"node_id"`
				Sequence      int    `json:"sequence"`
				Percent       int    `json:"percent"`
			}
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				t.Fatalf("json.Unmarshal(workflow_run.node_progress) error = %v", err)
			}
			if payload.WorkflowRunID == "" || payload.NodeID == "" {
				t.Fatalf("unexpected payload: %+v", payload)
			}
			if payload.NodeRunID == "" {
				t.Fatalf("node_run_id is empty: %+v", payload)
			}
			if payload.Sequence <= 0 {
				t.Fatalf("sequence = %d, want > 0", payload.Sequence)
			}
			if payload.Percent < 0 || payload.Percent > 100 {
				t.Fatalf("percent = %d, want within [0,100]", payload.Percent)
			}
			return
		}
	}
}
