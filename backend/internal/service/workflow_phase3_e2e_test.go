package service

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

func TestPhase3WorkflowE2E_BatchSourceDirClassification(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	adapter := fs.NewMockAdapter()
	adapter.AddDir("/source", []fs.DirEntry{{Name: "漫画合集", IsDir: true}, {Name: "cbz-only", IsDir: true}, {Name: "video-pack", IsDir: true}})
	adapter.AddDir("/source/漫画合集", []fs.DirEntry{{Name: "001.jpg", IsDir: false, Size: 100}})
	adapter.AddDir("/source/cbz-only", []fs.DirEntry{{Name: "vol1.cbz", IsDir: false, Size: 100}})
	adapter.AddDir("/source/video-pack", []fs.DirEntry{{Name: "ep1.mp4", IsDir: false, Size: 200}, {Name: "ep2.mkv", IsDir: false, Size: 220}})

	svc, jobRepo, folderRepo, workflowDefRepo, workflowRunRepo, nodeRunRepo, _ := newPhase3WorkflowTestEnv(t, adapter)

	def := createPhase3WorkflowDef(t, workflowDefRepo, "wf-batch-source", repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "picker", Type: "folder-picker", Enabled: true, Config: map[string]any{"paths": []any{"/source"}}},
			{ID: "scanner", Type: folderTreeScannerExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"source_dir": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "picker", SourcePort: "path"}},
			}},
			{ID: "kw", Type: "name-keyword-classifier", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"trees": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scanner", OutputPortIndex: 0}},
			}},
			{ID: "ft", Type: "file-tree-classifier", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"trees": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scanner", OutputPortIndex: 0}},
			}},
			{ID: "ext", Type: "ext-ratio-classifier", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"trees": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scanner", OutputPortIndex: 0}},
			}},
			{ID: "cc", Type: "confidence-check", Enabled: true, Config: map[string]any{"threshold": 0.75}, Inputs: map[string]repository.NodeInputSpec{
				"signals": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "ext", OutputPortIndex: 0}},
			}},
			{ID: "agg", Type: "signal-aggregator", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"trees":       {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scanner", OutputPortIndex: 0}},
				"signal_kw":   {LinkSource: &repository.NodeLinkSource{SourceNodeID: "kw", OutputPortIndex: 0}},
				"signal_ft":   {LinkSource: &repository.NodeLinkSource{SourceNodeID: "ft", OutputPortIndex: 0}},
				"signal_high": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "cc", OutputPortIndex: 0}},
			}},
			{ID: "writer", Type: "classification-writer", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"entries": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "agg", SourcePort: "entries"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e0", Source: "picker", SourcePort: "path", Target: "scanner", TargetPort: "source_dir"},
			{ID: "e1", Source: "scanner", SourcePort: "tree", Target: "kw", TargetPort: "trees"},
			{ID: "e2", Source: "scanner", SourcePort: "tree", Target: "ft", TargetPort: "trees"},
			{ID: "e3", Source: "scanner", SourcePort: "tree", Target: "ext", TargetPort: "trees"},
			{ID: "e4", Source: "ext", SourcePort: "signal", Target: "cc", TargetPort: "signals"},
			{ID: "e7", Source: "scanner", SourcePort: "tree", Target: "agg", TargetPort: "trees"},
			{ID: "e8", Source: "kw", SourcePort: "signal", Target: "agg", TargetPort: "signal_kw"},
			{ID: "e9", Source: "ft", SourcePort: "signal", Target: "agg", TargetPort: "signal_ft"},
			{ID: "e10", Source: "cc", SourcePort: "high", Target: "agg", TargetPort: "signal_high"},
			{ID: "e11", Source: "agg", SourcePort: "entries", Target: "writer", TargetPort: "entries"},
		},
	})

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		run := waitWorkflowRunByJob(t, workflowRunRepo, jobID)
		nodeRuns, _, listErr := nodeRunRepo.List(ctx, repository.NodeRunListFilter{WorkflowRunID: run.ID, Page: 1, Limit: 50})
		if listErr != nil {
			t.Fatalf("job status = %q, want succeeded; failed listing node runs: %v", job.Status, listErr)
		}
		t.Fatalf("job status = %q, want succeeded (workflow_run_status=%q resume_node_id=%q node_runs=%v)", job.Status, run.Status, run.ResumeNodeID, compactNodeRuns(nodeRuns))
	}

	check := []struct {
		path string
		want string
	}{
		{path: "/source/漫画合集", want: "manga"},
		{path: "/source/cbz-only", want: "manga"},
		{path: "/source/video-pack", want: "video"},
	}
	for _, tc := range check {
		folder, getErr := folderRepo.GetByPath(ctx, tc.path)
		if getErr != nil {
			t.Fatalf("folderRepo.GetByPath(%q) error = %v", tc.path, getErr)
		}
		if folder.Category != tc.want {
			t.Fatalf("folder %q category = %q, want %q", tc.path, folder.Category, tc.want)
		}
	}

	run := waitWorkflowRunByJob(t, workflowRunRepo, jobID)
	nodeRuns, _, err := nodeRunRepo.List(ctx, repository.NodeRunListFilter{WorkflowRunID: run.ID, Page: 1, Limit: 50})
	if err != nil {
		t.Fatalf("nodeRunRepo.List() error = %v", err)
	}
	if got := nodeRunStatusByID(nodeRuns, "writer"); got != "succeeded" {
		t.Fatalf("writer node status = %q, want succeeded", got)
	}
}

func TestPhase3WorkflowE2E_ComplexNestedMixedClassification(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	adapter := fs.NewMockAdapter()
	adapter.AddDir("/source", []fs.DirEntry{
		{Name: "剧集库", IsDir: true},
		{Name: "旅行图集", IsDir: true},
		{Name: "活动记录", IsDir: true},
	})
	adapter.AddDir("/source/剧集库", []fs.DirEntry{{Name: "国剧", IsDir: true}})
	adapter.AddDir("/source/剧集库/国剧", []fs.DirEntry{{Name: "第01季", IsDir: true}})
	adapter.AddDir("/source/剧集库/国剧/第01季", []fs.DirEntry{
		{Name: "ep01.mp4", IsDir: false, Size: 300},
		{Name: "ep02.mkv", IsDir: false, Size: 280},
	})
	adapter.AddDir("/source/旅行图集", []fs.DirEntry{{Name: "2024", IsDir: true}})
	adapter.AddDir("/source/旅行图集/2024", []fs.DirEntry{{Name: "日本", IsDir: true}})
	adapter.AddDir("/source/旅行图集/2024/日本", []fs.DirEntry{{Name: "Day01", IsDir: true}})
	adapter.AddDir("/source/旅行图集/2024/日本/Day01", []fs.DirEntry{
		{Name: "001.jpg", IsDir: false, Size: 110},
		{Name: "002.png", IsDir: false, Size: 120},
	})
	adapter.AddDir("/source/活动记录", []fs.DirEntry{{Name: "婚礼", IsDir: true}})
	adapter.AddDir("/source/活动记录/婚礼", []fs.DirEntry{
		{Name: "原片视频", IsDir: true},
		{Name: "相册照片", IsDir: true},
		{Name: "混合精选", IsDir: true},
	})
	adapter.AddDir("/source/活动记录/婚礼/原片视频", []fs.DirEntry{{Name: "clip01.mp4", IsDir: false, Size: 500}})
	adapter.AddDir("/source/活动记录/婚礼/相册照片", []fs.DirEntry{
		{Name: "p1.jpg", IsDir: false, Size: 90},
		{Name: "p2.jpeg", IsDir: false, Size: 100},
	})
	adapter.AddDir("/source/活动记录/婚礼/混合精选", []fs.DirEntry{
		{Name: "highlight.mp4", IsDir: false, Size: 260},
		{Name: "cover.jpg", IsDir: false, Size: 80},
	})

	svc, jobRepo, folderRepo, workflowDefRepo, workflowRunRepo, nodeRunRepo, _ := newPhase3WorkflowTestEnv(t, adapter)

	def := createPhase3WorkflowDef(t, workflowDefRepo, "wf-complex-nested-classification", repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "picker", Type: "folder-picker", Enabled: true, Config: map[string]any{"paths": []any{"/source"}}},
			{ID: "scanner", Type: folderTreeScannerExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"source_dir": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "picker", SourcePort: "path"}},
			}},
			{ID: "kw", Type: "name-keyword-classifier", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"trees": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scanner", SourcePort: "tree"}},
			}},
			{ID: "ft", Type: "file-tree-classifier", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"trees": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scanner", SourcePort: "tree"}},
			}},
			{ID: "ext", Type: "ext-ratio-classifier", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"trees": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scanner", SourcePort: "tree"}},
			}},
			{ID: "subtree", Type: subtreeAggregatorExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"trees":      {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scanner", SourcePort: "tree"}},
				"signal_kw":  {LinkSource: &repository.NodeLinkSource{SourceNodeID: "kw", SourcePort: "signal"}},
				"signal_ft":  {LinkSource: &repository.NodeLinkSource{SourceNodeID: "ft", SourcePort: "signal"}},
				"signal_ext": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "ext", SourcePort: "signal"}},
			}},
			{ID: "writer", Type: "classification-writer", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"entries": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "subtree", SourcePort: "entry"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e0", Source: "picker", SourcePort: "path", Target: "scanner", TargetPort: "source_dir"},
			{ID: "e1", Source: "scanner", SourcePort: "tree", Target: "kw", TargetPort: "trees"},
			{ID: "e2", Source: "scanner", SourcePort: "tree", Target: "ft", TargetPort: "trees"},
			{ID: "e3", Source: "scanner", SourcePort: "tree", Target: "ext", TargetPort: "trees"},
			{ID: "e4", Source: "scanner", SourcePort: "tree", Target: "subtree", TargetPort: "trees"},
			{ID: "e5", Source: "kw", SourcePort: "signal", Target: "subtree", TargetPort: "signal_kw"},
			{ID: "e6", Source: "ft", SourcePort: "signal", Target: "subtree", TargetPort: "signal_ft"},
			{ID: "e7", Source: "ext", SourcePort: "signal", Target: "subtree", TargetPort: "signal_ext"},
			{ID: "e8", Source: "subtree", SourcePort: "entry", Target: "writer", TargetPort: "entries"},
		},
	})

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		run := waitWorkflowRunByJob(t, workflowRunRepo, jobID)
		nodeRuns, _, listErr := nodeRunRepo.List(ctx, repository.NodeRunListFilter{WorkflowRunID: run.ID, Page: 1, Limit: 80})
		if listErr != nil {
			t.Fatalf("job status = %q, want succeeded; failed listing node runs: %v", job.Status, listErr)
		}
		t.Fatalf("job status = %q, want succeeded (workflow_run_status=%q resume_node_id=%q node_runs=%v)", job.Status, run.Status, run.ResumeNodeID, compactNodeRuns(nodeRuns))
	}

	checks := []struct {
		path string
		want string
	}{
		{path: "/source/活动记录", want: "mixed"},
		{path: "/source/活动记录/婚礼", want: "mixed"},
		{path: "/source/活动记录/婚礼/原片视频", want: "video"},
		{path: "/source/活动记录/婚礼/相册照片", want: "photo"},
		{path: "/source/活动记录/婚礼/混合精选", want: "mixed"},
	}
	for _, tc := range checks {
		folder, getErr := folderRepo.GetByPath(ctx, tc.path)
		if getErr != nil {
			t.Fatalf("folderRepo.GetByPath(%q) error = %v", tc.path, getErr)
		}
		if folder.Category != tc.want {
			t.Fatalf("folder %q category = %q, want %q", tc.path, folder.Category, tc.want)
		}
	}
}

func TestPhase3WorkflowE2E_MoveRollback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	adapter := fs.NewMockAdapter()
	adapter.AddDir("/source", []fs.DirEntry{{Name: "rollback-me", IsDir: true}})
	adapter.AddDir("/source/rollback-me", []fs.DirEntry{{Name: "001.jpg", IsDir: false, Size: 100}})

	svc, jobRepo, folderRepo, workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo := newPhase3WorkflowTestEnv(t, adapter)

	folder := &repository.Folder{ID: "folder-move-rollback", Path: "/source/rollback-me", Name: "rollback-me", Category: "photo", CategorySource: "workflow", Status: "pending"}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	producer := &processingItemSourceExecutor{items: []ProcessingItem{{FolderID: folder.ID, SourcePath: folder.Path, FolderName: folder.Name, Category: folder.Category}}}
	def := createPhase3WorkflowDef(t, workflowDefRepo, "wf-move-rollback", repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "source", Type: producer.Type(), Enabled: true},
			{ID: "move", Type: phase4MoveNodeExecutorType, Enabled: true, Config: map[string]any{"target_dir": "/target"}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "source", SourcePort: "items"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{{ID: "e1", Source: "source", SourcePort: "items", Target: "move", TargetPort: "items"}},
	})
	svc.RegisterExecutor(producer)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}

	run := waitWorkflowRunByJob(t, workflowRunRepo, jobID)
	movedFolder, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() after move error = %v", err)
	}
	if movedFolder.Path != "/target/rollback-me" {
		t.Fatalf("folder path after move = %q, want /target/rollback-me", movedFolder.Path)
	}

	nodeRuns, _, err := nodeRunRepo.List(ctx, repository.NodeRunListFilter{WorkflowRunID: run.ID, Page: 1, Limit: 20})
	if err != nil {
		t.Fatalf("nodeRunRepo.List() error = %v", err)
	}
	snaps, err := nodeSnapshotRepo.ListByNodeRunID(ctx, nodeRuns[0].ID)
	if err != nil {
		t.Fatalf("nodeSnapshotRepo.ListByNodeRunID() error = %v", err)
	}
	if len(snaps) != 2 {
		t.Fatalf("snapshot count = %d, want 2", len(snaps))
	}

	if err := svc.RollbackWorkflowRun(ctx, run.ID); err != nil {
		t.Fatalf("RollbackWorkflowRun() error = %v", err)
	}
	rolledBackFolder, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() after rollback error = %v", err)
	}
	if rolledBackFolder.Path != "/source/rollback-me" {
		t.Fatalf("folder path after rollback = %q, want /source/rollback-me", rolledBackFolder.Path)
	}
}

func newPhase3WorkflowTestEnv(t *testing.T, adapter *fs.MockAdapter) (*WorkflowRunnerService, repository.JobRepository, repository.FolderRepository, repository.WorkflowDefinitionRepository, repository.WorkflowRunRepository, repository.NodeRunRepository, repository.NodeSnapshotRepository) {
	t.Helper()

	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)
	snapshotRepo := repository.NewSnapshotRepository(database)

	svc := NewWorkflowRunnerService(jobRepo, folderRepo, snapshotRepo, workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, adapter, nil, nil)
	svc.RegisterExecutor(newFolderPickerNodeExecutor(adapter, folderRepo))
	svc.RegisterExecutor(NewFolderTreeScannerExecutor(adapter))
	svc.RegisterExecutor(NewNameKeywordClassifierExecutor())
	svc.RegisterExecutor(NewFileTreeClassifierExecutor())
	svc.RegisterExecutor(NewConfidenceCheckExecutor())
	svc.RegisterExecutor(newSignalAggregatorExecutor())
	svc.RegisterExecutor(newSubtreeAggregatorExecutor(folderRepo, snapshotRepo, nil))
	svc.RegisterExecutor(newClassificationWriterExecutor(folderRepo, snapshotRepo))

	return svc, jobRepo, folderRepo, workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo
}

func createPhase3WorkflowDef(t *testing.T, repo repository.WorkflowDefinitionRepository, id string, graph repository.WorkflowGraph) *repository.WorkflowDefinition {
	t.Helper()

	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{ID: id, Name: id, GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := repo.Create(context.Background(), def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	return def
}

// TestEngineV2_AC_CLASS2_KeywordPriorityOverExtRatio verifies that the
// name-keyword-classifier (confidence 1.0) overrides ext-ratio-classifier when
// classifying a folder whose name contains a keyword trigger.
func TestEngineV2_AC_CLASS2_KeywordPriorityOverExtRatio(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	adapter := fs.NewMockAdapter()
	// Folder "漫画合集" has only video files → ext-ratio would say "video" (conf 0.85)
	// but the name keyword rule fires "manga" at conf 1.0.
	adapter.AddDir("/source", []fs.DirEntry{{Name: "漫画合集", IsDir: true}})
	adapter.AddDir("/source/漫画合集", []fs.DirEntry{
		{Name: "ep1.mp4", IsDir: false, Size: 200},
		{Name: "ep2.mkv", IsDir: false, Size: 220},
	})

	svc, jobRepo, folderRepo, workflowDefRepo, workflowRunRepo, nodeRunRepo, _ := newPhase3WorkflowTestEnv(t, adapter)

	def := createPhase3WorkflowDef(t, workflowDefRepo, "wf-keyword-priority", repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "picker", Type: "folder-picker", Enabled: true, Config: map[string]any{"paths": []any{"/source"}}},
			{ID: "scanner", Type: folderTreeScannerExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"source_dir": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "picker", SourcePort: "path"}},
			}},
			// keyword classifier: fires "manga" at confidence 1.0 because name contains "漫画"
			{ID: "kw", Type: "name-keyword-classifier", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"trees": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scanner", OutputPortIndex: 0}},
			}},
			// ext-ratio classifier: sees video files, emits "video" at confidence 0.85
			{ID: "ext", Type: "ext-ratio-classifier", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"trees": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scanner", OutputPortIndex: 0}},
			}},
			// aggregator receives both signals and picks highest-confidence winner
			{ID: "agg", Type: "signal-aggregator", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"trees":      {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scanner", OutputPortIndex: 0}},
				"signal_kw":  {LinkSource: &repository.NodeLinkSource{SourceNodeID: "kw", OutputPortIndex: 0}},
				"signal_ext": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "ext", OutputPortIndex: 0}},
			}},
			{ID: "writer", Type: "classification-writer", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"entries": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "agg", SourcePort: "entries"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e0", Source: "picker", SourcePort: "path", Target: "scanner", TargetPort: "source_dir"},
			{ID: "e1", Source: "scanner", SourcePort: "tree", Target: "kw", TargetPort: "trees"},
			{ID: "e2", Source: "scanner", SourcePort: "tree", Target: "ext", TargetPort: "trees"},
			{ID: "e3", Source: "scanner", SourcePort: "tree", Target: "agg", TargetPort: "trees"},
			{ID: "e4", Source: "kw", SourcePort: "signal", Target: "agg", TargetPort: "signal_kw"},
			{ID: "e5", Source: "ext", SourcePort: "signal", Target: "agg", TargetPort: "signal_ext"},
			{ID: "e6", Source: "agg", SourcePort: "entries", Target: "writer", TargetPort: "entries"},
		},
	})

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		run := waitWorkflowRunByJob(t, workflowRunRepo, jobID)
		nodeRuns, _, listErr := nodeRunRepo.List(ctx, repository.NodeRunListFilter{WorkflowRunID: run.ID, Page: 1, Limit: 50})
		if listErr != nil {
			t.Fatalf("job status = %q, want succeeded; failed listing node runs: %v", job.Status, listErr)
		}
		t.Fatalf("job status = %q, want succeeded (workflow_run_status=%q resume_node_id=%q node_runs=%v)", job.Status, run.Status, run.ResumeNodeID, compactNodeRuns(nodeRuns))
	}

	folder, err := folderRepo.GetByPath(ctx, "/source/漫画合集")
	if err != nil {
		t.Fatalf("folderRepo.GetByPath() error = %v", err)
	}
	// keyword (manga, 1.0) must win over ext-ratio (video, 0.85)
	if folder.Category != "manga" {
		t.Fatalf("folder category = %q, want manga (keyword priority over ext-ratio)", folder.Category)
	}
}

// TestEngineV2_AC_COMPAT2_LegacyScanAndMoveNodeRegression verifies that the
// legacy folder-scan path and v2 move-node workflow both complete successfully.
func TestEngineV2_AC_COMPAT2_LegacyScanAndMoveNodeRegression(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	adapter := fs.NewMockAdapter()
	adapter.AddDir("/source", []fs.DirEntry{{Name: "legacy-folder", IsDir: true}})
	adapter.AddDir("/source/legacy-folder", []fs.DirEntry{{Name: "001.jpg", IsDir: false, Size: 100}})

	svc, jobRepo, folderRepo, workflowDefRepo, workflowRunRepo, _, _ := newPhase3WorkflowTestEnv(t, adapter)

	// v2 move flow: processing-item source + move-node.
	folder := &repository.Folder{ID: "folder-legacy-compat", Path: "/source/legacy-folder", Name: "legacy-folder", Category: "photo", CategorySource: "manual", Status: "pending"}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	def := createPhase3WorkflowDef(t, workflowDefRepo, "wf-legacy-compat", repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "source", Type: "processing-item-source", Enabled: true},
			{ID: "move", Type: phase4MoveNodeExecutorType, Enabled: true, Config: map[string]any{"target_dir": "/target"}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "source", SourcePort: "items"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{{ID: "e1", Source: "source", SourcePort: "items", Target: "move", TargetPort: "items"}},
	})
	svc.RegisterExecutor(&processingItemSourceExecutor{items: []ProcessingItem{{FolderID: folder.ID, SourcePath: folder.Path, FolderName: folder.Name, Category: folder.Category}}})

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}
	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("v2 move flow status = %q, want succeeded", job.Status)
	}

	run := waitWorkflowRunByJob(t, workflowRunRepo, jobID)
	if run.Status != "succeeded" {
		t.Fatalf("v2 move workflow-run status = %q, want succeeded", run.Status)
	}

	updated, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() error = %v", err)
	}
	if updated.Path != "/target/legacy-folder" {
		t.Fatalf("folder path = %q, want /target/legacy-folder", updated.Path)
	}
}

func waitWorkflowRunByJob(t *testing.T, repo repository.WorkflowRunRepository, jobID string) *repository.WorkflowRun {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runs, _, err := repo.List(context.Background(), repository.WorkflowRunListFilter{JobID: jobID, Page: 1, Limit: 10})
		if err != nil {
			t.Fatalf("workflowRunRepo.List() error = %v", err)
		}
		if len(runs) > 0 {
			return runs[0]
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timeout waiting workflow run for job %q", jobID)
	return nil
}

func waitWorkflowRunStatus(t *testing.T, repo repository.WorkflowRunRepository, jobID, status string) *repository.WorkflowRun {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		run := waitWorkflowRunByJob(t, repo, jobID)
		if run.Status == status {
			return run
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timeout waiting workflow run for job %q to reach status %q", jobID, status)
	return nil
}

func waitWorkflowRunIDStatus(t *testing.T, repo repository.WorkflowRunRepository, runID, status string) *repository.WorkflowRun {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		run, err := repo.GetByID(context.Background(), runID)
		if err != nil {
			t.Fatalf("workflowRunRepo.GetByID() error = %v", err)
		}
		if run.Status == status {
			return run
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timeout waiting workflow run %q to reach status %q", runID, status)
	return nil
}

func nodeRunStatusByID(nodeRuns []*repository.NodeRun, nodeID string) string {
	for _, nodeRun := range nodeRuns {
		if nodeRun.NodeID == nodeID {
			return nodeRun.Status
		}
	}

	return ""
}

func compactNodeRuns(nodeRuns []*repository.NodeRun) []string {
	items := make([]string, 0, len(nodeRuns))
	for _, nodeRun := range nodeRuns {
		items = append(items, nodeRun.NodeID+":"+nodeRun.Status+":"+nodeRun.Error)
	}

	return items
}
