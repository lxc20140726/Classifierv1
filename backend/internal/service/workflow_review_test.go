package service

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/sse"
)

func TestWorkflowRunnerServiceProcessingReviewApproveFlow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	reviewRepo := repository.NewProcessingReviewRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	folder := &repository.Folder{
		ID:             "folder-review-approve",
		Path:           "/source/review-approve",
		SourceDir:      "/source",
		RelativePath:   "review-approve",
		Name:           "review-approve",
		Category:       "photo",
		CategorySource: "manual",
		Status:         "done",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	producer := &processingItemSourceExecutor{items: []ProcessingItem{{
		FolderID:   folder.ID,
		SourcePath: folder.Path,
		FolderName: folder.Name,
		Category:   folder.Category,
	}}}
	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "source", Type: producer.Type(), Enabled: true},
			{ID: "rename", Type: renameNodeExecutorType, Enabled: true, Config: map[string]any{"strategy": "template", "template": "{name}-ok"}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "source", SourcePort: "items"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e1", Source: "source", SourcePort: "items", Target: "rename", TargetPort: "items"},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}
	def := &repository.WorkflowDefinition{ID: "wf-review-approve", Name: "wf-review-approve", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, fs.NewMockAdapter(), nil, nil)
	svc.SetProcessingReviewRepository(reviewRepo)
	svc.RegisterExecutor(producer)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	run := waitWorkflowRunStatus(t, workflowRunRepo, jobID, "waiting_input")
	job, err := jobRepo.GetByID(ctx, jobID)
	if err != nil {
		t.Fatalf("jobRepo.GetByID() error = %v", err)
	}
	if job.Status != "waiting_input" {
		t.Fatalf("job status = %q, want waiting_input", job.Status)
	}
	waitingFolder, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() waiting_input error = %v", err)
	}
	if waitingFolder.Status != "pending" {
		t.Fatalf("folder status in waiting_input = %q, want pending", waitingFolder.Status)
	}

	reviews, err := svc.ListProcessingReviews(ctx, run.ID)
	if err != nil {
		t.Fatalf("ListProcessingReviews() error = %v", err)
	}
	if reviews.Summary.Total != 1 || reviews.Summary.Pending != 1 {
		t.Fatalf("review summary = %+v, want total=1 pending=1", reviews.Summary)
	}

	if err := svc.ApproveProcessingReview(ctx, run.ID, reviews.Items[0].ID); err != nil {
		t.Fatalf("ApproveProcessingReview() error = %v", err)
	}

	run = waitWorkflowRunIDStatus(t, workflowRunRepo, run.ID, "succeeded")
	if run.Status != "succeeded" {
		t.Fatalf("workflow run status = %q, want succeeded", run.Status)
	}
	job = waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}
	updatedFolder, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() error = %v", err)
	}
	if updatedFolder.Status != "done" {
		t.Fatalf("folder status = %q, want done", updatedFolder.Status)
	}
}

func TestWorkflowRunnerServiceProcessingReviewApproveIsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	reviewRepo := repository.NewProcessingReviewRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	folder := &repository.Folder{
		ID:             "folder-review-approve-idempotent",
		Path:           "/source/review-approve-idempotent",
		SourceDir:      "/source",
		RelativePath:   "review-approve-idempotent",
		Name:           "review-approve-idempotent",
		Category:       "photo",
		CategorySource: "manual",
		Status:         "done",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	producer := &processingItemSourceExecutor{items: []ProcessingItem{{
		FolderID:   folder.ID,
		SourcePath: folder.Path,
		FolderName: folder.Name,
		Category:   folder.Category,
	}}}
	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "source", Type: producer.Type(), Enabled: true},
			{ID: "rename", Type: renameNodeExecutorType, Enabled: true, Config: map[string]any{"strategy": "template", "template": "{name}-ok"}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "source", SourcePort: "items"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e1", Source: "source", SourcePort: "items", Target: "rename", TargetPort: "items"},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}
	def := &repository.WorkflowDefinition{
		ID:        "wf-review-approve-idempotent",
		Name:      "wf-review-approve-idempotent",
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
		nil,
		nil,
	)
	svc.SetProcessingReviewRepository(reviewRepo)
	svc.RegisterExecutor(producer)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	run := waitWorkflowRunStatus(t, workflowRunRepo, jobID, "waiting_input")
	reviews, err := svc.ListProcessingReviews(ctx, run.ID)
	if err != nil {
		t.Fatalf("ListProcessingReviews() error = %v", err)
	}
	if len(reviews.Items) != 1 {
		t.Fatalf("review items = %d, want 1", len(reviews.Items))
	}

	reviewID := reviews.Items[0].ID
	if err := svc.ApproveProcessingReview(ctx, run.ID, reviewID); err != nil {
		t.Fatalf("ApproveProcessingReview() first call error = %v", err)
	}
	if err := svc.ApproveProcessingReview(ctx, run.ID, reviewID); err != nil {
		t.Fatalf("ApproveProcessingReview() second call error = %v", err)
	}

	run = waitWorkflowRunIDStatus(t, workflowRunRepo, run.ID, "succeeded")
	if run.Status != "succeeded" {
		t.Fatalf("workflow run status = %q, want succeeded", run.Status)
	}
	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}
	review, err := reviewRepo.GetByID(ctx, reviewID)
	if err != nil {
		t.Fatalf("reviewRepo.GetByID() error = %v", err)
	}
	if review.Status != "approved" {
		t.Fatalf("review status = %q, want approved", review.Status)
	}
}

func TestWorkflowRunnerServiceWorkflowRunUpdatedEventOnReviewFlow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	reviewRepo := repository.NewProcessingReviewRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)
	broker := sse.NewBroker()
	runEvents := broker.Subscribe()
	defer broker.Unsubscribe(runEvents)

	folder := &repository.Folder{
		ID:             "folder-review-updated-event",
		Path:           "/source/review-updated-event",
		SourceDir:      "/source",
		RelativePath:   "review-updated-event",
		Name:           "review-updated-event",
		Category:       "photo",
		CategorySource: "manual",
		Status:         "pending",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	producer := &processingItemSourceExecutor{items: []ProcessingItem{{
		FolderID:   folder.ID,
		SourcePath: folder.Path,
		FolderName: folder.Name,
		Category:   folder.Category,
	}}}
	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "source", Type: producer.Type(), Enabled: true},
			{ID: "rename", Type: renameNodeExecutorType, Enabled: true, Config: map[string]any{"strategy": "template", "template": "{name}-ok"}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "source", SourcePort: "items"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e1", Source: "source", SourcePort: "items", Target: "rename", TargetPort: "items"},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}
	def := &repository.WorkflowDefinition{ID: "wf-review-updated-event", Name: "wf-review-updated-event", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, fs.NewMockAdapter(), broker, nil)
	svc.SetProcessingReviewRepository(reviewRepo)
	svc.RegisterExecutor(producer)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	run := waitWorkflowRunStatus(t, workflowRunRepo, jobID, "waiting_input")
	waitWorkflowRunUpdatedStatus(t, runEvents, run.ID, "waiting_input", jobID, def.ID)

	reviews, err := svc.ListProcessingReviews(ctx, run.ID)
	if err != nil {
		t.Fatalf("ListProcessingReviews() error = %v", err)
	}
	if len(reviews.Items) == 0 {
		t.Fatalf("review items empty, want at least 1")
	}

	if err := svc.ApproveProcessingReview(ctx, run.ID, reviews.Items[0].ID); err != nil {
		t.Fatalf("ApproveProcessingReview() error = %v", err)
	}

	waitWorkflowRunUpdatedStatus(t, runEvents, run.ID, "succeeded", jobID, def.ID)
}

func waitWorkflowRunUpdatedStatus(t *testing.T, events <-chan sse.Event, runID, status, jobID, workflowDefID string) {
	t.Helper()

	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatalf("timed out waiting workflow_run.updated status=%q run=%q", status, runID)
		case event := <-events:
			if event.Type != "workflow_run.updated" {
				continue
			}
			var payload struct {
				JobID         string  `json:"job_id"`
				WorkflowRunID string  `json:"workflow_run_id"`
				WorkflowDefID string  `json:"workflow_def_id"`
				Status        string  `json:"status"`
				LastNodeID    string  `json:"last_node_id"`
				ResumeNodeID  *string `json:"resume_node_id"`
				Error         string  `json:"error"`
			}
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				t.Fatalf("json.Unmarshal(workflow_run.updated) error = %v", err)
			}
			if payload.WorkflowRunID != runID || payload.Status != status {
				continue
			}
			if payload.JobID != jobID {
				t.Fatalf("workflow_run.updated job_id = %q, want %q", payload.JobID, jobID)
			}
			if payload.WorkflowDefID != workflowDefID {
				t.Fatalf("workflow_run.updated workflow_def_id = %q, want %q", payload.WorkflowDefID, workflowDefID)
			}
			if status == "running" && payload.ResumeNodeID != nil && *payload.ResumeNodeID != "" {
				t.Fatalf("running workflow_run.updated resume_node_id = %q, want empty", *payload.ResumeNodeID)
			}
			return
		}
	}
}

func waitFolderClassificationUpdatedStatus(t *testing.T, events <-chan sse.Event, runID, folderID, status string) {
	t.Helper()

	timeout := time.After(5 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatalf("timed out waiting folder.classification.updated status=%q run=%q folder=%q", status, runID, folderID)
		case event := <-events:
			if event.Type != "folder.classification.updated" {
				continue
			}
			var payload struct {
				FolderID             string `json:"folder_id"`
				WorkflowRunID        string `json:"workflow_run_id"`
				ClassificationStatus string `json:"classification_status"`
				Category             string `json:"category"`
				CategorySource       string `json:"category_source"`
				UpdatedAt            string `json:"updated_at"`
			}
			if err := json.Unmarshal(event.Data, &payload); err != nil {
				t.Fatalf("json.Unmarshal(folder.classification.updated) error = %v", err)
			}
			if payload.WorkflowRunID != runID || payload.FolderID != folderID || payload.ClassificationStatus != status {
				continue
			}
			if payload.Category == "" {
				t.Fatalf("folder.classification.updated category is empty")
			}
			if payload.CategorySource == "" {
				t.Fatalf("folder.classification.updated category_source is empty")
			}
			if payload.UpdatedAt == "" {
				t.Fatalf("folder.classification.updated updated_at is empty")
			}
			return
		}
	}
}

func TestPublishWorkflowRunUpdatedPublishesFolderClassificationEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	broker := sse.NewBroker()
	events := broker.Subscribe()
	defer broker.Unsubscribe(events)

	folder := &repository.Folder{
		ID:             "folder-live-publish",
		Path:           "/source/live-publish",
		SourceDir:      "/source",
		RelativePath:   "live-publish",
		Name:           "live-publish",
		Category:       "photo",
		CategorySource: "workflow",
		Status:         "pending",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}
	job := &repository.Job{
		ID:        "job-live-publish",
		Type:      "workflow",
		Status:    "running",
		FolderIDs: `["folder-live-publish"]`,
		Total:     1,
	}
	if err := jobRepo.Create(ctx, job); err != nil {
		t.Fatalf("jobRepo.Create() error = %v", err)
	}
	run := &repository.WorkflowRun{
		ID:            "run-live-publish",
		JobID:         job.ID,
		FolderID:      folder.ID,
		WorkflowDefID: "wf-live-publish",
		Status:        "waiting_input",
		LastNodeID:    "node-review",
	}
	if err := workflowRunRepo.Create(ctx, run); err != nil {
		t.Fatalf("workflowRunRepo.Create() error = %v", err)
	}

	svc := NewWorkflowRunnerService(
		jobRepo,
		folderRepo,
		repository.NewSnapshotRepository(database),
		repository.NewWorkflowDefinitionRepository(database),
		workflowRunRepo,
		repository.NewNodeRunRepository(database),
		repository.NewNodeSnapshotRepository(database),
		fs.NewMockAdapter(),
		broker,
		nil,
	)

	svc.publishWorkflowRunUpdated(ctx, run.ID)
	waitFolderClassificationUpdatedStatus(t, events, run.ID, folder.ID, "waiting_input")
}

func TestWorkflowRunnerPrepareProcessingReviews_AutoAggregateWithoutStepResultEdges(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	reviewRepo := repository.NewProcessingReviewRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	folder := &repository.Folder{
		ID:             "folder-review-auto",
		Path:           "/source/review-auto",
		SourceDir:      "/source",
		RelativePath:   "review-auto",
		Name:           "review-auto",
		Category:       "photo",
		CategorySource: "manual",
		Status:         "pending",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	job := &repository.Job{
		ID:            "job-review-auto",
		Type:          "workflow",
		WorkflowDefID: "wf-review-auto",
		Status:        "running",
		FolderIDs:     `["folder-review-auto"]`,
		Total:         1,
	}
	if err := jobRepo.Create(ctx, job); err != nil {
		t.Fatalf("jobRepo.Create() error = %v", err)
	}

	runStartedAt := time.Now()
	run := &repository.WorkflowRun{
		ID:            "run-review-auto",
		JobID:         job.ID,
		FolderID:      folder.ID,
		WorkflowDefID: job.WorkflowDefID,
		Status:        "running",
		StartedAt:     &runStartedAt,
	}
	if err := workflowRunRepo.Create(ctx, run); err != nil {
		t.Fatalf("workflowRunRepo.Create() error = %v", err)
	}

	makeOutputJSON := func(outputs map[string]TypedValue) string {
		encoded, err := typedValueMapToJSON(outputs, NewTypeRegistry())
		if err != nil {
			t.Fatalf("typedValueMapToJSON() error = %v", err)
		}
		raw, err := json.Marshal(encoded)
		if err != nil {
			t.Fatalf("json.Marshal(outputs) error = %v", err)
		}
		return string(raw)
	}
	makeInputJSON := func(nodeType, nodeLabel string, config map[string]any, inputs map[string]any) string {
		raw, err := json.Marshal(map[string]any{
			"node": map[string]any{
				"type":   nodeType,
				"label":  nodeLabel,
				"config": config,
			},
			"inputs": inputs,
		})
		if err != nil {
			t.Fatalf("json.Marshal(inputs) error = %v", err)
		}
		return string(raw)
	}

	renamedName := "review-auto-renamed"
	movedPath := "/target/review-auto-renamed"
	compressDir := "/archives"
	thumbDir := "/thumbs"

	nodeRuns := []*repository.NodeRun{
		{
			ID:            "nr-rename",
			WorkflowRunID: run.ID,
			NodeID:        "rename",
			NodeType:      "rename-node",
			Sequence:      1,
			Status:        "succeeded",
			InputJSON: makeInputJSON("rename-node", "重命名", map[string]any{
				"strategy": "template",
				"template": "{name}-renamed",
			}, map[string]any{
				"items": []ProcessingItem{{FolderID: folder.ID, SourcePath: folder.Path, FolderName: folder.Name}},
			}),
			OutputJSON: makeOutputJSON(map[string]TypedValue{
				"items":        {Type: PortTypeProcessingItemList, Value: []ProcessingItem{{FolderID: folder.ID, SourcePath: folder.Path, FolderName: folder.Name, TargetName: renamedName}}},
				"step_results": {Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{{SourcePath: folder.Path, TargetPath: renamedName, NodeType: "rename-node", Status: "renamed"}}},
			}),
		},
		{
			ID:            "nr-move",
			WorkflowRunID: run.ID,
			NodeID:        "move",
			NodeType:      "move-node",
			Sequence:      2,
			Status:        "succeeded",
			InputJSON: makeInputJSON("move-node", "移动", map[string]any{"target_dir": "/target"}, map[string]any{
				"items": []ProcessingItem{{FolderID: folder.ID, SourcePath: folder.Path, FolderName: folder.Name, TargetName: renamedName}},
			}),
			OutputJSON: makeOutputJSON(map[string]TypedValue{
				"items":        {Type: PortTypeProcessingItemList, Value: []ProcessingItem{{FolderID: folder.ID, SourcePath: movedPath, FolderName: folder.Name, TargetName: renamedName}}},
				"step_results": {Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{{SourcePath: folder.Path, TargetPath: movedPath, NodeType: "move-node", Status: "moved"}}},
			}),
		},
		{
			ID:            "nr-compress",
			WorkflowRunID: run.ID,
			NodeID:        "compress",
			NodeType:      "compress-node",
			Sequence:      3,
			Status:        "succeeded",
			InputJSON: makeInputJSON("compress-node", "压缩", map[string]any{"target_dir": compressDir, "format": "cbz"}, map[string]any{
				"items": []ProcessingItem{{FolderID: folder.ID, SourcePath: movedPath, FolderName: folder.Name, TargetName: renamedName}},
			}),
			OutputJSON: makeOutputJSON(map[string]TypedValue{
				"items":        {Type: PortTypeProcessingItemList, Value: []ProcessingItem{{FolderID: folder.ID, SourcePath: movedPath, FolderName: folder.Name, TargetName: renamedName}}},
				"step_results": {Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{{SourcePath: movedPath, TargetPath: compressDir + "/" + renamedName + ".cbz", NodeType: "compress-node", Status: "succeeded"}}},
			}),
		},
		{
			ID:            "nr-thumbnail",
			WorkflowRunID: run.ID,
			NodeID:        "thumbnail",
			NodeType:      "thumbnail-node",
			Sequence:      4,
			Status:        "succeeded",
			InputJSON: makeInputJSON("thumbnail-node", "缩略图", map[string]any{"output_dir": thumbDir}, map[string]any{
				"items": []ProcessingItem{{FolderID: folder.ID, SourcePath: movedPath, FolderName: folder.Name, TargetName: renamedName}},
			}),
			OutputJSON: makeOutputJSON(map[string]TypedValue{
				"items":        {Type: PortTypeProcessingItemList, Value: []ProcessingItem{{FolderID: folder.ID, SourcePath: movedPath, FolderName: folder.Name, TargetName: renamedName}}},
				"step_results": {Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{{SourcePath: movedPath, TargetPath: thumbDir + "/" + renamedName + ".jpg", NodeType: "thumbnail-node", Status: "succeeded"}}},
			}),
		},
	}
	for _, item := range nodeRuns {
		if err := nodeRunRepo.Create(ctx, item); err != nil {
			t.Fatalf("nodeRunRepo.Create(%s) error = %v", item.ID, err)
		}
	}
	storedNodeRuns, _, err := nodeRunRepo.List(ctx, repository.NodeRunListFilter{WorkflowRunID: run.ID, Page: 1, Limit: 20})
	if err != nil {
		t.Fatalf("nodeRunRepo.List() error = %v", err)
	}
	for _, nodeRun := range storedNodeRuns {
		typedOutputs, typed, parseErr := parseTypedNodeOutputs(nodeRun.OutputJSON)
		if parseErr != nil || !typed {
			t.Fatalf("parseTypedNodeOutputs(%s) error = %v typed=%v", nodeRun.NodeID, parseErr, typed)
		}
		if len(processingStepResultsFromNodeRun(nodeRun, typedOutputs)) == 0 {
			t.Fatalf("processingStepResultsFromNodeRun(%s) returned empty", nodeRun.NodeID)
		}
	}

	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, fs.NewMockAdapter(), nil, nil)
	svc.SetProcessingReviewRepository(reviewRepo)

	prepared, err := svc.prepareProcessingReviews(ctx, run.ID, folder)
	if err != nil {
		t.Fatalf("prepareProcessingReviews() error = %v", err)
	}
	if !prepared {
		t.Fatalf("prepareProcessingReviews() = false, want true")
	}

	reviews, err := svc.ListProcessingReviews(ctx, run.ID)
	if err != nil {
		t.Fatalf("ListProcessingReviews() error = %v", err)
	}
	if reviews.Summary.Total != 1 {
		t.Fatalf("review total = %d, want 1", reviews.Summary.Total)
	}
	if len(reviews.Items) != 1 {
		t.Fatalf("review items len = %d, want 1", len(reviews.Items))
	}

	var steps []ProcessingStepResult
	if err := json.Unmarshal(reviews.Items[0].StepResultsJSON, &steps); err != nil {
		t.Fatalf("json.Unmarshal(step_results) error = %v", err)
	}
	if len(steps) != 4 {
		t.Fatalf("step_results len = %d, want 4", len(steps))
	}
	typeSet := map[string]struct{}{}
	for _, step := range steps {
		typeSet[step.NodeType] = struct{}{}
	}
	for _, nodeType := range []string{"rename-node", "move-node", "compress-node", "thumbnail-node"} {
		if _, ok := typeSet[nodeType]; !ok {
			t.Fatalf("missing step result for node type %q", nodeType)
		}
	}
}

func TestBuildProcessingReviewAfterPrefersAggregatedFolderPathOverArtifactTarget(t *testing.T) {
	t.Parallel()

	folderPath := filepath.Join("/library", "A")
	agg := &processingFolderAggregate{
		BeforePath: folderPath,
		LastPath:   folderPath,
		AfterPath:  "",
		SourcePaths: map[string]struct{}{
			folderPath: {},
		},
		TargetPaths: map[string]struct{}{
			filepath.Join("/output", "A.cbz"): {},
		},
		StepResults: []ProcessingStepResult{
			{
				SourcePath: folderPath,
				TargetPath: filepath.Join("/output", "A.cbz"),
				NodeType:   compressNodeExecutorType,
				Status:     "succeeded",
			},
		},
	}

	after := buildProcessingReviewAfter(nil, agg)
	if got := after["path"]; got != folderPath {
		t.Fatalf("after[path] = %v, want %q", got, folderPath)
	}
	if got := after["name"]; got != "A" {
		t.Fatalf("after[name] = %v, want %q", got, "A")
	}

	artifacts, ok := after["artifact_paths"].([]string)
	if !ok {
		t.Fatalf("after[artifact_paths] type = %T, want []string", after["artifact_paths"])
	}
	if len(artifacts) != 1 || artifacts[0] != filepath.Join("/output", "A.cbz") {
		t.Fatalf("after[artifact_paths] = %#v, want [%q]", artifacts, filepath.Join("/output", "A.cbz"))
	}
}

func TestSummarizeStepsKeepsSourceAndTargetPath(t *testing.T) {
	t.Parallel()

	sourcePath := filepath.Join("/source", "A")
	resolvedTarget := resolveProcessingStepTargetPath(sourcePath, "A-renamed")
	results := []ProcessingStepResult{
		{
			SourcePath: sourcePath,
			TargetPath: "A-renamed",
			NodeType:   renameNodeExecutorType,
			NodeLabel:  "rename",
			Status:     "renamed",
		},
		{
			SourcePath: sourcePath,
			TargetPath: "A-renamed",
			NodeType:   renameNodeExecutorType,
			NodeLabel:  "rename",
			Status:     "renamed",
		},
	}

	steps := summarizeSteps(results)
	if len(steps) != 1 {
		t.Fatalf("len(steps) = %d, want 1", len(steps))
	}
	if got := steps[0]["source_path"]; got != sourcePath {
		t.Fatalf("source_path = %q, want %q", got, sourcePath)
	}
	if got := steps[0]["target_path"]; got != resolvedTarget {
		t.Fatalf("target_path = %q, want %q", got, resolvedTarget)
	}
}

func TestBuildProcessingReviewBeforeAfterPreferAggregatedPaths(t *testing.T) {
	t.Parallel()

	agg := &processingFolderAggregate{
		BeforePath: filepath.Join("/source", "A"),
		AfterPath:  filepath.Join("/target", "A"),
		LastPath:   filepath.Join("/target", "A"),
		SourcePaths: map[string]struct{}{
			filepath.Join("/source", "A"): {},
		},
		TargetPaths: map[string]struct{}{
			filepath.Join("/target", "A"): {},
		},
	}
	folder := &repository.Folder{
		Path: filepath.Join("/db", "already-moved", "A"),
		Name: "A",
	}

	before := buildProcessingReviewBefore(folder, agg)
	after := buildProcessingReviewAfter(folder, agg)
	if got := before["path"]; got != agg.BeforePath {
		t.Fatalf("before[path] = %v, want %q", got, agg.BeforePath)
	}
	if got := after["path"]; got != agg.AfterPath {
		t.Fatalf("after[path] = %v, want %q", got, agg.AfterPath)
	}
}

func TestBuildProcessingReviewDiffPathChangedUsesNormalizedPaths(t *testing.T) {
	t.Parallel()

	diff := buildProcessingReviewDiff(
		map[string]any{"path": `F:\media\album`},
		map[string]any{"path": `f:/media/album`},
		nil,
	)
	if changed, _ := diff["path_changed"].(bool); changed {
		t.Fatalf("path_changed = true, want false for normalized-equal paths")
	}
}

func TestWorkflowRunnerServiceApproveAllPendingProcessingReviews(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	reviewRepo := repository.NewProcessingReviewRepository(database)
	auditRepo := repository.NewAuditRepository(database)

	folder := &repository.Folder{
		ID:             "folder-review-batch-approve",
		Path:           "/source/review-batch-approve",
		SourceDir:      "/source",
		RelativePath:   "review-batch-approve",
		Name:           "review-batch-approve",
		Category:       "photo",
		CategorySource: "workflow",
		Status:         "pending",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	job := &repository.Job{
		ID:            "job-review-batch-approve",
		Type:          "workflow",
		WorkflowDefID: "wf-review-batch-approve",
		Status:        "waiting_input",
		FolderIDs:     `["folder-review-batch-approve"]`,
		Total:         1,
	}
	if err := jobRepo.Create(ctx, job); err != nil {
		t.Fatalf("jobRepo.Create() error = %v", err)
	}

	run := &repository.WorkflowRun{
		ID:            "run-review-batch-approve",
		JobID:         job.ID,
		FolderID:      folder.ID,
		WorkflowDefID: job.WorkflowDefID,
		Status:        "waiting_input",
	}
	if err := workflowRunRepo.Create(ctx, run); err != nil {
		t.Fatalf("workflowRunRepo.Create() error = %v", err)
	}

	review1 := &repository.ProcessingReviewItem{
		ID:            "review-batch-approve-1",
		WorkflowRunID: run.ID,
		JobID:         job.ID,
		FolderID:      folder.ID,
		Status:        "pending",
	}
	if err := reviewRepo.Create(ctx, review1); err != nil {
		t.Fatalf("reviewRepo.Create(review1) error = %v", err)
	}
	review2 := &repository.ProcessingReviewItem{
		ID:            "review-batch-approve-2",
		WorkflowRunID: run.ID,
		JobID:         job.ID,
		FolderID:      folder.ID,
		Status:        "pending",
	}
	if err := reviewRepo.Create(ctx, review2); err != nil {
		t.Fatalf("reviewRepo.Create(review2) error = %v", err)
	}
	reviewedAt := time.Now().Add(-time.Hour)
	review3 := &repository.ProcessingReviewItem{
		ID:            "review-batch-approve-3",
		WorkflowRunID: run.ID,
		JobID:         job.ID,
		FolderID:      folder.ID,
		Status:        "approved",
		Error:         "keep",
		ReviewedAt:    &reviewedAt,
	}
	if err := reviewRepo.Create(ctx, review3); err != nil {
		t.Fatalf("reviewRepo.Create(review3) error = %v", err)
	}

	svc := NewWorkflowRunnerService(
		jobRepo,
		folderRepo,
		repository.NewSnapshotRepository(database),
		repository.NewWorkflowDefinitionRepository(database),
		workflowRunRepo,
		repository.NewNodeRunRepository(database),
		repository.NewNodeSnapshotRepository(database),
		fs.NewMockAdapter(),
		nil,
		NewAuditService(auditRepo),
	)
	svc.SetProcessingReviewRepository(reviewRepo)

	approved, err := svc.ApproveAllPendingProcessingReviews(ctx, run.ID)
	if err != nil {
		t.Fatalf("ApproveAllPendingProcessingReviews() error = %v", err)
	}
	if approved != 2 {
		t.Fatalf("approved = %d, want 2", approved)
	}

	approvedAgain, err := svc.ApproveAllPendingProcessingReviews(ctx, run.ID)
	if err != nil {
		t.Fatalf("ApproveAllPendingProcessingReviews() second call error = %v", err)
	}
	if approvedAgain != 0 {
		t.Fatalf("approved second call = %d, want 0", approvedAgain)
	}

	reviews, err := reviewRepo.ListByWorkflowRunID(ctx, run.ID)
	if err != nil {
		t.Fatalf("reviewRepo.ListByWorkflowRunID() error = %v", err)
	}
	if got := countReviewStatus(reviews, "approved"); got != 3 {
		t.Fatalf("approved count = %d, want 3", got)
	}
	if got := countReviewStatus(reviews, "pending"); got != 0 {
		t.Fatalf("pending count = %d, want 0", got)
	}
	kept, err := reviewRepo.GetByID(ctx, review3.ID)
	if err != nil {
		t.Fatalf("reviewRepo.GetByID(review3) error = %v", err)
	}
	if kept.Error != "keep" {
		t.Fatalf("already-approved review error = %q, want keep", kept.Error)
	}

	updatedRun, err := workflowRunRepo.GetByID(ctx, run.ID)
	if err != nil {
		t.Fatalf("workflowRunRepo.GetByID() error = %v", err)
	}
	if updatedRun.Status != "succeeded" {
		t.Fatalf("workflow run status = %q, want succeeded", updatedRun.Status)
	}

	updatedJob, err := jobRepo.GetByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("jobRepo.GetByID() error = %v", err)
	}
	if updatedJob.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", updatedJob.Status)
	}

	logs, _, err := auditRepo.List(ctx, repository.AuditListFilter{JobID: job.ID, Action: "workflow.processing.review_approved", Page: 1, Limit: 50})
	if err != nil {
		t.Fatalf("auditRepo.List() error = %v", err)
	}
	if len(logs) != 2 {
		t.Fatalf("approved audit logs = %d, want 2", len(logs))
	}
}

func TestWorkflowRunnerServiceRollbackAllPendingProcessingReviews(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	reviewRepo := repository.NewProcessingReviewRepository(database)
	auditRepo := repository.NewAuditRepository(database)

	folder1 := &repository.Folder{
		ID:             "folder-review-batch-rollback-1",
		Path:           "/source/review-batch-rollback-1",
		SourceDir:      "/source",
		RelativePath:   "review-batch-rollback-1",
		Name:           "review-batch-rollback-1",
		Category:       "photo",
		CategorySource: "workflow",
		Status:         "done",
	}
	if err := folderRepo.Upsert(ctx, folder1); err != nil {
		t.Fatalf("folderRepo.Upsert(folder1) error = %v", err)
	}
	folder2 := &repository.Folder{
		ID:             "folder-review-batch-rollback-2",
		Path:           "/source/review-batch-rollback-2",
		SourceDir:      "/source",
		RelativePath:   "review-batch-rollback-2",
		Name:           "review-batch-rollback-2",
		Category:       "video",
		CategorySource: "workflow",
		Status:         "done",
	}
	if err := folderRepo.Upsert(ctx, folder2); err != nil {
		t.Fatalf("folderRepo.Upsert(folder2) error = %v", err)
	}

	job := &repository.Job{
		ID:            "job-review-batch-rollback",
		Type:          "workflow",
		WorkflowDefID: "wf-review-batch-rollback",
		Status:        "waiting_input",
		FolderIDs:     `["folder-review-batch-rollback-1","folder-review-batch-rollback-2"]`,
		Total:         1,
	}
	if err := jobRepo.Create(ctx, job); err != nil {
		t.Fatalf("jobRepo.Create() error = %v", err)
	}

	run := &repository.WorkflowRun{
		ID:            "run-review-batch-rollback",
		JobID:         job.ID,
		FolderID:      folder1.ID,
		WorkflowDefID: job.WorkflowDefID,
		Status:        "waiting_input",
	}
	if err := workflowRunRepo.Create(ctx, run); err != nil {
		t.Fatalf("workflowRunRepo.Create() error = %v", err)
	}

	review1 := &repository.ProcessingReviewItem{
		ID:            "review-batch-rollback-1",
		WorkflowRunID: run.ID,
		JobID:         job.ID,
		FolderID:      folder1.ID,
		Status:        "pending",
	}
	if err := reviewRepo.Create(ctx, review1); err != nil {
		t.Fatalf("reviewRepo.Create(review1) error = %v", err)
	}
	review2 := &repository.ProcessingReviewItem{
		ID:            "review-batch-rollback-2",
		WorkflowRunID: run.ID,
		JobID:         job.ID,
		FolderID:      folder2.ID,
		Status:        "pending",
	}
	if err := reviewRepo.Create(ctx, review2); err != nil {
		t.Fatalf("reviewRepo.Create(review2) error = %v", err)
	}
	reviewedAt := time.Now().Add(-time.Hour)
	review3 := &repository.ProcessingReviewItem{
		ID:            "review-batch-rollback-3",
		WorkflowRunID: run.ID,
		JobID:         job.ID,
		FolderID:      folder2.ID,
		Status:        "rolled_back",
		Error:         "keep",
		ReviewedAt:    &reviewedAt,
	}
	if err := reviewRepo.Create(ctx, review3); err != nil {
		t.Fatalf("reviewRepo.Create(review3) error = %v", err)
	}

	svc := NewWorkflowRunnerService(
		jobRepo,
		folderRepo,
		repository.NewSnapshotRepository(database),
		repository.NewWorkflowDefinitionRepository(database),
		workflowRunRepo,
		repository.NewNodeRunRepository(database),
		repository.NewNodeSnapshotRepository(database),
		fs.NewMockAdapter(),
		nil,
		NewAuditService(auditRepo),
	)
	svc.SetProcessingReviewRepository(reviewRepo)

	rolledBack, err := svc.RollbackAllPendingProcessingReviews(ctx, run.ID)
	if err != nil {
		t.Fatalf("RollbackAllPendingProcessingReviews() error = %v", err)
	}
	if rolledBack != 2 {
		t.Fatalf("rolledBack = %d, want 2", rolledBack)
	}

	rolledBackAgain, err := svc.RollbackAllPendingProcessingReviews(ctx, run.ID)
	if err != nil {
		t.Fatalf("RollbackAllPendingProcessingReviews() second call error = %v", err)
	}
	if rolledBackAgain != 0 {
		t.Fatalf("rolledBack second call = %d, want 0", rolledBackAgain)
	}

	reviews, err := reviewRepo.ListByWorkflowRunID(ctx, run.ID)
	if err != nil {
		t.Fatalf("reviewRepo.ListByWorkflowRunID() error = %v", err)
	}
	if got := countReviewStatus(reviews, "rolled_back"); got != 3 {
		t.Fatalf("rolled_back count = %d, want 3", got)
	}
	if got := countReviewStatus(reviews, "pending"); got != 0 {
		t.Fatalf("pending count = %d, want 0", got)
	}
	kept, err := reviewRepo.GetByID(ctx, review3.ID)
	if err != nil {
		t.Fatalf("reviewRepo.GetByID(review3) error = %v", err)
	}
	if kept.Error != "keep" {
		t.Fatalf("already-rolled-back review error = %q, want keep", kept.Error)
	}

	updatedRun, err := workflowRunRepo.GetByID(ctx, run.ID)
	if err != nil {
		t.Fatalf("workflowRunRepo.GetByID() error = %v", err)
	}
	if updatedRun.Status != "rolled_back" {
		t.Fatalf("workflow run status = %q, want rolled_back", updatedRun.Status)
	}

	updatedJob, err := jobRepo.GetByID(ctx, job.ID)
	if err != nil {
		t.Fatalf("jobRepo.GetByID() error = %v", err)
	}
	if updatedJob.Status != "rolled_back" {
		t.Fatalf("job status = %q, want rolled_back", updatedJob.Status)
	}

	updatedFolder1, err := folderRepo.GetByID(ctx, folder1.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID(folder1) error = %v", err)
	}
	if updatedFolder1.Status != "pending" {
		t.Fatalf("folder1 status = %q, want pending", updatedFolder1.Status)
	}
	updatedFolder2, err := folderRepo.GetByID(ctx, folder2.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID(folder2) error = %v", err)
	}
	if updatedFolder2.Status != "pending" {
		t.Fatalf("folder2 status = %q, want pending", updatedFolder2.Status)
	}

	logs, _, err := auditRepo.List(ctx, repository.AuditListFilter{JobID: job.ID, Action: "workflow.processing.review_rolled_back", Page: 1, Limit: 50})
	if err != nil {
		t.Fatalf("auditRepo.List() error = %v", err)
	}
	if len(logs) != 2 {
		t.Fatalf("rolled_back audit logs = %d, want 2", len(logs))
	}
}
