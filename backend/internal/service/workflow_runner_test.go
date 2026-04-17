package service

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

func TestExtRatioClassifierClassifiesNestedVideoTree(t *testing.T) {
	t.Parallel()

	executor := &extRatioClassifierNodeExecutor{}
	tree := FolderTree{
		Path: "/source/compilation",
		Name: "compilation",
		Subdirs: []FolderTree{{
			Path: "/source/compilation/5k porn",
			Name: "5k porn",
			Files: []FileEntry{{
				Name: "5kporn.20.02.19.skye.blue.5k.mp4",
				Ext:  ".mp4",
			}},
		}},
	}

	output, err := executor.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{
			"trees": []FolderTree{tree},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	signals, ok := output.Outputs["signal"].Value.([]ClassificationSignal)
	if !ok {
		t.Fatalf("signals type = %T, want []ClassificationSignal", output.Outputs["signal"].Value)
	}
	if len(signals) != 1 {
		t.Fatalf("len(signals) = %d, want 1", len(signals))
	}
	if signals[0].Category != "video" {
		t.Fatalf("signal category = %q, want video", signals[0].Category)
	}
	if signals[0].Confidence != 0.85 {
		t.Fatalf("signal confidence = %v, want 0.85", signals[0].Confidence)
	}
}

type stubCustomExecutor struct{}

func (e *stubCustomExecutor) Type() string {
	return "custom-test"
}

func (e *stubCustomExecutor) Schema() NodeSchema {
	return NodeSchema{Type: e.Type(), Label: "Custom Test", Description: "Custom test executor"}
}

func (e *stubCustomExecutor) Execute(_ context.Context, _ NodeExecutionInput) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{Outputs: map[string]TypedValue{"ok": {Type: PortTypeBoolean, Value: true}}, Status: ExecutionSuccess}, nil
}

func (e *stubCustomExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *stubCustomExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

type produceInputExecutor struct{}

func (e *produceInputExecutor) Type() string {
	return "produce-input"
}

func (e *produceInputExecutor) Schema() NodeSchema {
	return NodeSchema{Type: e.Type(), Label: "Produce Input", Description: "Produce a fixed output for tests", Outputs: []PortDef{{Name: "out", Type: PortTypeString}}}
}

func (e *produceInputExecutor) Execute(_ context.Context, _ NodeExecutionInput) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{Outputs: map[string]TypedValue{"out": {Type: PortTypeString, Value: "hello-port"}}, Status: ExecutionSuccess}, nil
}

func (e *produceInputExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *produceInputExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

type consumeInputExecutor struct {
	seen string
}

type slowParallelExecutor struct {
	active    int32
	maxActive int32
	mu        sync.Mutex
	visited   []string
}

type auditOutputExecutor struct {
	nodeType string
	outputs  map[string]TypedValue
}

type namedPortProducerExecutor struct{}

type namedPortConsumerExecutor struct {
	seen string
}

type requiredInputProbeExecutor struct {
	nodeType  string
	portName  string
	lazy      bool
	executed  int32
	lastValue any
}

type resumeDataMergeExecutor struct {
	lastResume map[string]any
}

type processingItemSourceExecutor struct {
	items []ProcessingItem
}

type classifiedEntrySourceExecutor struct {
	nodeType string
	entry    ClassifiedEntry
}

type processingItemListProducerExecutor struct {
	nodeType string
	items    []ProcessingItem
}

type emptyRequiredOutputExecutor struct{}

type branchSlowItemsExecutor struct {
	branch    string
	active    *int32
	maxActive *int32
}

type stubManifestBuilder struct {
	ensureCalls int
	lastRunID   string
	lastItems   []ProcessingItem
}

func (s *stubManifestBuilder) Build(context.Context, string) error {
	return nil
}

func (s *stubManifestBuilder) EnsureForWorkflowRun(_ context.Context, workflowRunID string, items []ProcessingItem) error {
	s.ensureCalls++
	s.lastRunID = workflowRunID
	s.lastItems = append([]ProcessingItem(nil), items...)
	return nil
}

type stubMappingBuilder struct {
	buildCalls int
	lastRunID  string
}

func (s *stubMappingBuilder) Build(_ context.Context, workflowRunID string) error {
	s.buildCalls++
	s.lastRunID = workflowRunID
	return nil
}

type stubOutputValidator struct {
	validateWorkflowRunCalls int
}

func (s *stubOutputValidator) ValidateWorkflowRun(context.Context, string) ([]*repository.FolderOutputCheck, error) {
	s.validateWorkflowRunCalls++
	return []*repository.FolderOutputCheck{}, nil
}

func (s *stubOutputValidator) ValidateFolder(context.Context, string) (*repository.FolderOutputCheck, error) {
	return nil, nil
}

func (s *stubOutputValidator) GetLatestDetail(context.Context, string) (*FolderOutputCheckDetail, error) {
	return nil, nil
}

func (s *stubOutputValidator) CanMarkDone(context.Context, string) (bool, error) {
	return false, nil
}

type stubCompletionUpdater struct {
	syncCalls        int
	markPendingCalls int
	lastFolderID     string
}

func (s *stubCompletionUpdater) Sync(context.Context, string, *repository.FolderOutputCheck) error {
	s.syncCalls++
	return nil
}

func (s *stubCompletionUpdater) MarkPending(_ context.Context, folderID string) error {
	s.markPendingCalls++
	s.lastFolderID = folderID
	return nil
}

func TestWorkflowRunSourceManifestItemsFallsBackToRunFolderID(t *testing.T) {
	t.Parallel()

	run := &repository.WorkflowRun{ID: "run-manifest-fallback", FolderID: "folder-root"}
	items := []ProcessingItem{
		{SourcePath: "/source/root/__photo/a.jpg", CurrentPath: "/source/root/__photo/a.jpg", Category: "photo"},
		{FolderID: "folder-child", SourcePath: "/source/root/video", Category: "video"},
	}

	got := workflowRunSourceManifestItems(run, items)
	if got[0].FolderID != "folder-root" {
		t.Fatalf("got[0].FolderID = %q, want folder-root", got[0].FolderID)
	}
	if got[1].FolderID != "folder-child" {
		t.Fatalf("got[1].FolderID = %q, want folder-child", got[1].FolderID)
	}
	if items[0].FolderID != "" {
		t.Fatalf("input item was mutated: FolderID = %q", items[0].FolderID)
	}
}

func (e *consumeInputExecutor) Type() string {
	return "consume-input"
}

func (e *consumeInputExecutor) Schema() NodeSchema {
	return NodeSchema{Type: e.Type(), Label: "Consume Input", Description: "Consume upstream output for tests", Inputs: []PortDef{{Name: "upstream", Type: PortTypeString, Required: true}}, Outputs: []PortDef{{Name: "echo", Type: PortTypeString}}}
}

func (e *consumeInputExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	raw, ok := input.Inputs["upstream"]
	if !ok {
		return NodeExecutionOutput{}, nil
	}
	if raw == nil {
		return NodeExecutionOutput{}, nil
	}
	text, _ := raw.Value.(string)
	e.seen = text
	return NodeExecutionOutput{Outputs: map[string]TypedValue{"echo": {Type: PortTypeString, Value: text}}, Status: ExecutionSuccess}, nil
}

func (e *consumeInputExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *consumeInputExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func (e *slowParallelExecutor) Type() string {
	return "slow-parallel"
}

func (e *slowParallelExecutor) Schema() NodeSchema {
	return NodeSchema{Type: e.Type(), Label: "Slow Parallel", Description: "Track folder-level parallelism"}
}

func (e *slowParallelExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	current := atomic.AddInt32(&e.active, 1)
	for {
		observed := atomic.LoadInt32(&e.maxActive)
		if current <= observed {
			break
		}
		if atomic.CompareAndSwapInt32(&e.maxActive, observed, current) {
			break
		}
	}

	folderID := ""
	if input.Folder != nil {
		folderID = input.Folder.ID
	}

	e.mu.Lock()
	e.visited = append(e.visited, folderID)
	e.mu.Unlock()

	time.Sleep(50 * time.Millisecond)
	atomic.AddInt32(&e.active, -1)
	return NodeExecutionOutput{Outputs: map[string]TypedValue{"folder_id": {Type: PortTypeString, Value: folderID}}, Status: ExecutionSuccess}, nil
}

func (e *slowParallelExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *slowParallelExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func (e *auditOutputExecutor) Type() string {
	return e.nodeType
}

func (e *auditOutputExecutor) Schema() NodeSchema {
	return NodeSchema{Type: e.Type(), Label: e.Type(), Description: "Emit outputs for audit tests"}
}

func (e *auditOutputExecutor) Execute(_ context.Context, _ NodeExecutionInput) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{Outputs: e.outputs, Status: ExecutionSuccess}, nil
}

func (e *auditOutputExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *auditOutputExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func (e *namedPortProducerExecutor) Type() string {
	return "named-port-producer"
}

func (e *namedPortProducerExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "Named Port Producer",
		Description: "Emit multi-port outputs for named-port compatibility tests",
		Outputs:     []PortDef{{Name: "first"}, {Name: "second"}},
	}
}

func (e *namedPortProducerExecutor) Execute(_ context.Context, _ NodeExecutionInput) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{Outputs: map[string]TypedValue{"first": {Type: PortTypeString, Value: "first-value"}, "second": {Type: PortTypeString, Value: "second-value"}}, Status: ExecutionSuccess}, nil
}

func (e *namedPortProducerExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *namedPortProducerExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func (e *namedPortConsumerExecutor) Type() string {
	return "named-port-consumer"
}

func (e *namedPortConsumerExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "Named Port Consumer",
		Description: "Consume upstream output through named source port",
		Inputs:      []PortDef{{Name: "upstream", Required: true}},
	}
}

func (e *namedPortConsumerExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	if input.Inputs["upstream"] != nil {
		if value, ok := input.Inputs["upstream"].Value.(string); ok {
			e.seen = value
		}
	}

	return NodeExecutionOutput{Outputs: map[string]TypedValue{"seen": {Type: PortTypeString, Value: e.seen}}, Status: ExecutionSuccess}, nil
}

func (e *namedPortConsumerExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *namedPortConsumerExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func (e *requiredInputProbeExecutor) Type() string {
	return e.nodeType
}

func (e *requiredInputProbeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       e.Type(),
		Description: "Probe skip behavior for required inputs",
		Inputs:      []PortDef{{Name: e.portName, Required: true, Lazy: e.lazy}},
	}
}

func (e *requiredInputProbeExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	atomic.AddInt32(&e.executed, 1)
	if input.Inputs[e.portName] != nil {
		e.lastValue = input.Inputs[e.portName].Value
	}
	return NodeExecutionOutput{Outputs: map[string]TypedValue{"value": {Type: PortTypeJSON, Value: e.lastValue}}, Status: ExecutionSuccess}, nil
}

func (e *requiredInputProbeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *requiredInputProbeExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func (e *resumeDataMergeExecutor) Type() string {
	return "resume-data-merge"
}

func (e *resumeDataMergeExecutor) Schema() NodeSchema {
	return NodeSchema{Type: e.Type(), Label: "Resume Data Merge", Description: "Verify DB-backed resume-data merge behavior"}
}

func (e *resumeDataMergeExecutor) Execute(_ context.Context, _ NodeExecutionInput) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{Status: ExecutionPending, PendingReason: "need resume data"}, nil
}

func (e *resumeDataMergeExecutor) Resume(_ context.Context, _ NodeExecutionInput, resumeData map[string]any) (NodeExecutionOutput, error) {
	e.lastResume = make(map[string]any, len(resumeData))
	for key, value := range resumeData {
		e.lastResume[key] = value
	}

	if _, ok := resumeData["category"]; !ok {
		return NodeExecutionOutput{Status: ExecutionPending, PendingReason: "category is required"}, nil
	}

	return NodeExecutionOutput{Outputs: map[string]TypedValue{"category": {Type: PortTypeString, Value: resumeData["category"]}}, Status: ExecutionSuccess}, nil
}

func (e *resumeDataMergeExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func (e *processingItemSourceExecutor) Type() string {
	return "processing-item-source"
}

func (e *processingItemSourceExecutor) Schema() NodeSchema {
	return NodeSchema{Type: e.Type(), Label: "Processing Item Source", Description: "Emit processing items for rollback tests", Outputs: []PortDef{{Name: "items", Type: PortTypeProcessingItemList}}}
}

func (e *processingItemSourceExecutor) Execute(_ context.Context, _ NodeExecutionInput) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{Outputs: map[string]TypedValue{"items": {Type: PortTypeProcessingItemList, Value: append([]ProcessingItem(nil), e.items...)}}, Status: ExecutionSuccess}, nil
}

func (e *processingItemSourceExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *processingItemSourceExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func (e *classifiedEntrySourceExecutor) Type() string {
	if e.nodeType != "" {
		return e.nodeType
	}
	return "classified-entry-source"
}

func (e *classifiedEntrySourceExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "Classified Entry Source",
		Description: "Emit one classified entry for processing-flow tests",
		Outputs: []PortDef{
			{Name: "entry", Type: PortTypeJSON, RequiredOutput: true},
		},
	}
}

func (e *classifiedEntrySourceExecutor) Execute(_ context.Context, _ NodeExecutionInput) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{
			"entry": {Type: PortTypeJSON, Value: e.entry},
		},
		Status: ExecutionSuccess,
	}, nil
}

func (e *classifiedEntrySourceExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *classifiedEntrySourceExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func (e *processingItemListProducerExecutor) Type() string {
	if e.nodeType != "" {
		return e.nodeType
	}
	return "processing-item-list-producer"
}

func (e *processingItemListProducerExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "Processing Item List Producer",
		Description: "Emit processing items for branch skip tests",
		Outputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, RequiredOutput: true},
		},
	}
}

func (e *processingItemListProducerExecutor) Execute(_ context.Context, _ NodeExecutionInput) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{
			"items": {Type: PortTypeProcessingItemList, Value: append([]ProcessingItem(nil), e.items...)},
		},
		Status: ExecutionSuccess,
	}, nil
}

func (e *processingItemListProducerExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *processingItemListProducerExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func (e *emptyRequiredOutputExecutor) Type() string {
	return "empty-required-output"
}

func (e *emptyRequiredOutputExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "Empty Required Output",
		Description: "Return empty list on required output",
		Outputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, RequiredOutput: true},
		},
	}
}

func (e *emptyRequiredOutputExecutor) Execute(_ context.Context, _ NodeExecutionInput) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{"items": {Type: PortTypeProcessingItemList, Value: []ProcessingItem{}}},
		Status:  ExecutionSuccess,
	}, nil
}

func (e *emptyRequiredOutputExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *emptyRequiredOutputExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func (e *branchSlowItemsExecutor) Type() string {
	return "slow-items-" + e.branch
}

func (e *branchSlowItemsExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       e.Type(),
		Description: "Emit one processing item after delay",
		Outputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, RequiredOutput: true},
		},
	}
}

func (e *branchSlowItemsExecutor) Execute(_ context.Context, _ NodeExecutionInput) (NodeExecutionOutput, error) {
	current := atomic.AddInt32(e.active, 1)
	for {
		observed := atomic.LoadInt32(e.maxActive)
		if current <= observed {
			break
		}
		if atomic.CompareAndSwapInt32(e.maxActive, observed, current) {
			break
		}
	}
	time.Sleep(60 * time.Millisecond)
	atomic.AddInt32(e.active, -1)

	item := ProcessingItem{
		SourcePath: "/source/" + e.branch,
		FolderName: e.branch,
		TargetName: e.branch,
		Category:   "other",
	}
	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{"items": {Type: PortTypeProcessingItemList, Value: []ProcessingItem{item}}},
		Status:  ExecutionSuccess,
	}, nil
}

func (e *branchSlowItemsExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, nil
}

func (e *branchSlowItemsExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func TestWorkflowRunnerServiceStartAndResume(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	adapter := fs.NewMockAdapter()
	adapter.AddDir("/source/album", []fs.DirEntry{{Name: "a.jpg", IsDir: false}})

	folder := &repository.Folder{
		ID:             "folder-1",
		Path:           "/source/album",
		Name:           "album",
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
			{ID: "n2", Type: "custom-test", Enabled: true},
		},
		Edges: []repository.WorkflowGraphEdge{{Source: "n1", Target: "n2"}},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{
		ID:        "wf-1",
		Name:      "wf-test",
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
		adapter,
		nil,
		nil,
	)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "failed" {
		t.Fatalf("job status = %q, want failed", job.Status)
	}

	runs, total, err := workflowRunRepo.List(ctx, repository.WorkflowRunListFilter{JobID: jobID, Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("workflowRunRepo.List() error = %v", err)
	}
	if total != 1 || len(runs) != 1 {
		t.Fatalf("workflow runs total/len = %d/%d, want 1/1", total, len(runs))
	}
	if runs[0].ResumeNodeID != "n2" {
		t.Fatalf("resume_node_id = %q, want n2", runs[0].ResumeNodeID)
	}

	svc.RegisterExecutor(&stubCustomExecutor{})
	if err := svc.ResumeWorkflowRun(ctx, runs[0].ID); err != nil {
		t.Fatalf("ResumeWorkflowRun() error = %v", err)
	}

	updatedRun, err := workflowRunRepo.GetByID(ctx, runs[0].ID)
	if err != nil {
		t.Fatalf("workflowRunRepo.GetByID() error = %v", err)
	}
	if updatedRun.Status != "succeeded" {
		t.Fatalf("workflow run status = %q, want succeeded", updatedRun.Status)
	}

	nodeRuns, _, err := nodeRunRepo.List(ctx, repository.NodeRunListFilter{WorkflowRunID: runs[0].ID, Page: 1, Limit: 50})
	if err != nil {
		t.Fatalf("nodeRunRepo.List() error = %v", err)
	}
	if len(nodeRuns) < 3 {
		t.Fatalf("node runs len = %d, want >= 3", len(nodeRuns))
	}

	last := nodeRuns[len(nodeRuns)-1]
	if last.NodeID != "n2" || last.Status != "succeeded" {
		t.Fatalf("last node run = node_id %q status %q, want n2/succeeded", last.NodeID, last.Status)
	}
}

func TestWorkflowRunnerServicePortInputPropagation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	adapter := fs.NewMockAdapter()
	adapter.AddDir("/source/album-port", []fs.DirEntry{{Name: "a.jpg", IsDir: false}})

	folder := &repository.Folder{
		ID:             "folder-port",
		Path:           "/source/album-port",
		Name:           "album-port",
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "n1", Type: "produce-input", Enabled: true},
			{ID: "n2", Type: "consume-input", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"upstream": {
					LinkSource: &repository.NodeLinkSource{SourceNodeID: "n1", OutputPortIndex: 0},
				},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{{Source: "n1", SourcePortIndex: 0, Target: "n2", TargetPortIndex: 0}},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{
		ID:        "wf-port",
		Name:      "wf-port",
		GraphJSON: string(graphJSON),
		IsActive:  true,
		Version:   1,
	}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	consume := &consumeInputExecutor{}
	svc := NewWorkflowRunnerService(
		jobRepo,
		folderRepo,
		repository.NewSnapshotRepository(database),
		workflowDefRepo,
		workflowRunRepo,
		nodeRunRepo,
		nodeSnapshotRepo,
		adapter,
		nil,
		nil,
	)
	svc.RegisterExecutor(&produceInputExecutor{})
	svc.RegisterExecutor(consume)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}

	if consume.seen != "hello-port" {
		t.Fatalf("consume input = %q, want hello-port", consume.seen)
	}
}

func TestWorkflowRunnerServiceRunsFoldersInParallel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	adapter := fs.NewMockAdapter()
	folders := []*repository.Folder{
		{ID: "folder-a", Path: "/source/folder-a", Name: "folder-a", Category: "other", CategorySource: "auto", Status: "pending"},
		{ID: "folder-b", Path: "/source/folder-b", Name: "folder-b", Category: "other", CategorySource: "auto", Status: "pending"},
	}
	for _, folder := range folders {
		if err := folderRepo.Upsert(ctx, folder); err != nil {
			t.Fatalf("folderRepo.Upsert(%s) error = %v", folder.ID, err)
		}
	}

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "n1", Type: "trigger", Enabled: true},
			{ID: "n2", Type: "slow-parallel", Enabled: true},
		},
		Edges: []repository.WorkflowGraphEdge{{Source: "n1", Target: "n2"}},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{ID: "wf-parallel", Name: "wf-parallel", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	executor := &slowParallelExecutor{}
	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, adapter, nil, nil)
	svc.RegisterExecutor(executor)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}
	if atomic.LoadInt32(&executor.maxActive) != 1 {
		t.Fatalf("maxActive = %d, want 1 in v2 single-run mode", atomic.LoadInt32(&executor.maxActive))
	}
	if len(executor.visited) != 1 {
		t.Fatalf("visited len = %d, want 1 in v2 single-run mode", len(executor.visited))
	}
}

func TestWorkflowRunnerServiceWritesAuditForMutatingNodes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)
	auditRepo := repository.NewAuditRepository(database)
	auditSvc := NewAuditService(auditRepo)

	folder := &repository.Folder{ID: "folder-audit", Path: "/source/folder-audit", Name: "folder-audit", Category: "other", CategorySource: "auto", Status: "pending"}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	testCases := []struct {
		name       string
		nodeType   string
		outputs    map[string]TypedValue
		action     string
		result     string
		folderPath string
	}{
		{
			name:       "move-node",
			nodeType:   "move-node",
			outputs:    map[string]TypedValue{"items": {Type: PortTypeProcessingItemList, Value: []ProcessingItem{{FolderID: folder.ID, SourcePath: "/target/folder-audit", FolderName: folder.Name}}}, "step_results": {Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{{SourcePath: folder.Path, TargetPath: "/target/folder-audit", NodeType: "move-node", Status: "moved"}}}},
			action:     "workflow.move-node",
			result:     "moved",
			folderPath: "/target/folder-audit",
		},
		{
			name:       "compress-node",
			nodeType:   "compress-node",
			outputs:    map[string]TypedValue{"items": {Type: PortTypeProcessingItemList, Value: []ProcessingItem{{FolderID: folder.ID, SourcePath: folder.Path, FolderName: folder.Name}}}, "step_results": {Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{{SourcePath: folder.Path, TargetPath: "/archives/folder-audit.cbz", NodeType: "compress-node", Status: "succeeded"}}}},
			action:     "workflow.compress-node",
			result:     "success",
			folderPath: "/archives/folder-audit.cbz",
		},
		{
			name:       "thumbnail-node",
			nodeType:   "thumbnail-node",
			outputs:    map[string]TypedValue{"items": {Type: PortTypeProcessingItemList, Value: []ProcessingItem{{FolderID: folder.ID, SourcePath: folder.Path, FolderName: folder.Name}}}, "step_results": {Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{{SourcePath: folder.Path, TargetPath: "/thumbs/folder-audit.jpg", NodeType: "thumbnail-node", Status: "succeeded"}}}},
			action:     "workflow.thumbnail-node",
			result:     "success",
			folderPath: "/thumbs/folder-audit.jpg",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			graph := repository.WorkflowGraph{
				Nodes: []repository.WorkflowGraphNode{
					{ID: "n1", Type: "trigger", Enabled: true},
					{ID: "n2", Type: tc.nodeType, Enabled: true},
				},
				Edges: []repository.WorkflowGraphEdge{{Source: "n1", Target: "n2"}},
			}
			graphJSON, err := json.Marshal(graph)
			if err != nil {
				t.Fatalf("json.Marshal(graph) error = %v", err)
			}

			def := &repository.WorkflowDefinition{ID: "wf-" + tc.nodeType, Name: tc.nodeType, GraphJSON: string(graphJSON), IsActive: true, Version: 1}
			if err := workflowDefRepo.Create(ctx, def); err != nil {
				t.Fatalf("workflowDefRepo.Create() error = %v", err)
			}

			svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, fs.NewMockAdapter(), nil, auditSvc)
			svc.RegisterExecutor(&auditOutputExecutor{nodeType: tc.nodeType, outputs: tc.outputs})

			jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
			if err != nil {
				t.Fatalf("StartJob() error = %v", err)
			}
			waitJobDone(t, jobRepo, jobID)

			logs, _, err := auditRepo.List(ctx, repository.AuditListFilter{JobID: jobID, Action: tc.action, Page: 1, Limit: 20})
			if err != nil {
				t.Fatalf("auditRepo.List() error = %v", err)
			}
			if len(logs) == 0 {
				t.Fatalf("audit logs len = 0, want at least 1")
			}
			if logs[0].Result != tc.result {
				t.Fatalf("audit result = %q, want %q", logs[0].Result, tc.result)
			}
			if logs[0].FolderID != folder.ID {
				t.Fatalf("audit folder_id = %q, want %q", logs[0].FolderID, folder.ID)
			}
			if logs[0].FolderPath != tc.folderPath {
				t.Fatalf("audit folder_path = %q, want %q", logs[0].FolderPath, tc.folderPath)
			}
		})
	}
}

func TestWorkflowRunnerServiceNamedSourcePortCompatibility(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "n1", Type: "named-port-producer", Enabled: true},
			{ID: "n2", Type: "named-port-consumer", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"upstream": {
					LinkSource: &repository.NodeLinkSource{SourceNodeID: "n1", SourcePort: "second", OutputPortIndex: 0},
				},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{{Source: "n1", SourcePort: "second", Target: "n2", TargetPort: "upstream"}},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{ID: "wf-named-port", Name: "wf-named-port", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	consumer := &namedPortConsumerExecutor{}
	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, fs.NewMockAdapter(), nil, nil)
	svc.RegisterExecutor(&namedPortProducerExecutor{})
	svc.RegisterExecutor(consumer)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}
	if consumer.seen != "second-value" {
		t.Fatalf("consumer input = %q, want second-value", consumer.seen)
	}
}

func TestWorkflowRunnerServiceStartJobBindsFolderPickerRootFolder(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)
	auditRepo := repository.NewAuditRepository(database)
	auditSvc := NewAuditService(auditRepo)

	folder := &repository.Folder{
		ID:             "folder-root-binding",
		Path:           "/source/root-binding",
		SourceDir:      "/source",
		RelativePath:   "root-binding",
		Name:           "root-binding",
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "picker", Type: folderPickerExecutorType, Enabled: true, Config: map[string]any{
				"source_mode":     "folders",
				"saved_folder_id": folder.ID,
			}},
			{ID: "writer", Type: "classification-writer", Enabled: true},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{ID: "wf-folder-root-binding", Name: "wf-folder-root-binding", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	adapter := fs.NewMockAdapter()
	adapter.AddDir(folder.Path, []fs.DirEntry{})

	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, adapter, nil, auditSvc)
	svc.RegisterExecutor(&auditOutputExecutor{nodeType: "classification-writer", outputs: map[string]TypedValue{}})

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}

	var folderIDs []string
	if err := json.Unmarshal([]byte(job.FolderIDs), &folderIDs); err != nil {
		t.Fatalf("json.Unmarshal(job.FolderIDs) error = %v", err)
	}
	if len(folderIDs) != 1 || folderIDs[0] != folder.ID {
		t.Fatalf("job folder_ids = %v, want [%q]", folderIDs, folder.ID)
	}

	run := waitWorkflowRunByJob(t, workflowRunRepo, jobID)
	if run.FolderID != folder.ID {
		t.Fatalf("workflow run folder_id = %q, want %q", run.FolderID, folder.ID)
	}

	summaries, err := folderRepo.ListWorkflowSummariesByFolderIDs(ctx, []string{folder.ID})
	if err != nil {
		t.Fatalf("folderRepo.ListWorkflowSummariesByFolderIDs() error = %v", err)
	}
	summary := summaries[folder.ID]
	if summary.Classification.Status != "succeeded" {
		t.Fatalf("classification status = %q, want succeeded", summary.Classification.Status)
	}
	if summary.Classification.WorkflowRunID != run.ID {
		t.Fatalf("classification workflow_run_id = %q, want %q", summary.Classification.WorkflowRunID, run.ID)
	}

	logs, _, err := auditRepo.List(ctx, repository.AuditListFilter{
		WorkflowRunID: run.ID,
		Action:        "workflow.run.complete",
		Page:          1,
		Limit:         20,
	})
	if err != nil {
		t.Fatalf("auditRepo.List() error = %v", err)
	}
	if len(logs) == 0 {
		t.Fatalf("audit logs len = 0, want at least 1")
	}
	if logs[0].FolderID != folder.ID {
		t.Fatalf("audit folder_id = %q, want %q", logs[0].FolderID, folder.ID)
	}
	if logs[0].FolderPath != folder.Path {
		t.Fatalf("audit folder_path = %q, want %q", logs[0].FolderPath, folder.Path)
	}
}

func TestWorkflowRunnerServiceLazyRequiredInputFailureSemantics(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	strict := &requiredInputProbeExecutor{nodeType: "required-strict", portName: "upstream", lazy: false}
	lazy := &requiredInputProbeExecutor{nodeType: "required-lazy", portName: "upstream", lazy: true}

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "trigger", Type: "trigger", Enabled: true},
			{ID: "strict", Type: strict.Type(), Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"upstream": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "trigger", OutputPortIndex: 0}},
			}},
			{ID: "lazy", Type: lazy.Type(), Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"upstream": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "trigger", OutputPortIndex: 0}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{Source: "trigger", SourcePortIndex: 0, Target: "strict", TargetPortIndex: 0},
			{Source: "trigger", SourcePortIndex: 0, Target: "lazy", TargetPortIndex: 0},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{ID: "wf-lazy-skip", Name: "wf-lazy-skip", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, fs.NewMockAdapter(), nil, nil)
	svc.RegisterExecutor(strict)
	svc.RegisterExecutor(lazy)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "failed" {
		t.Fatalf("job status = %q, want failed", job.Status)
	}

	run := waitWorkflowRunByJob(t, workflowRunRepo, jobID)
	nodeRuns, _, err := nodeRunRepo.List(ctx, repository.NodeRunListFilter{WorkflowRunID: run.ID, Page: 1, Limit: 20})
	if err != nil {
		t.Fatalf("nodeRunRepo.List() error = %v", err)
	}

	if got := nodeRunStatusByID(nodeRuns, "strict"); got != "failed" {
		t.Fatalf("strict node status = %q, want failed", got)
	}
	if atomic.LoadInt32(&strict.executed) != 0 {
		t.Fatalf("strict executed = %d, want 0", atomic.LoadInt32(&strict.executed))
	}
	if atomic.LoadInt32(&lazy.executed) != 1 {
		t.Fatalf("lazy executed = %d, want 1 because same-level nodes execute concurrently", atomic.LoadInt32(&lazy.executed))
	}
}

func TestWorkflowRunnerServiceResumeDataPersistence(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	resumeExecutor := &resumeDataMergeExecutor{}
	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{{ID: "resume", Type: resumeExecutor.Type(), Enabled: true}},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{ID: "wf-resume-persist", Name: "wf-resume-persist", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, fs.NewMockAdapter(), nil, nil)
	svc.RegisterExecutor(resumeExecutor)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	run := waitWorkflowRunStatus(t, workflowRunRepo, jobID, "waiting_input")
	if run.ResumeNodeID != "resume" {
		t.Fatalf("resume node id = %q, want resume", run.ResumeNodeID)
	}

	if err := svc.ResumeWorkflowRunWithData(ctx, run.ID, map[string]any{"note": "first-pass"}); err != nil {
		t.Fatalf("ResumeWorkflowRunWithData(first) error = %v", err)
	}

	run = waitWorkflowRunIDStatus(t, workflowRunRepo, run.ID, "waiting_input")
	waitingNodeRun, err := nodeRunRepo.GetWaitingInputByWorkflowRunID(ctx, run.ID)
	if err != nil {
		t.Fatalf("nodeRunRepo.GetWaitingInputByWorkflowRunID() error = %v", err)
	}

	var persisted map[string]any
	if err := json.Unmarshal([]byte(waitingNodeRun.ResumeData), &persisted); err != nil {
		t.Fatalf("json.Unmarshal(resume_data) error = %v", err)
	}
	if persisted["note"] != "first-pass" {
		t.Fatalf("persisted note = %#v, want first-pass", persisted["note"])
	}

	if err := svc.ResumeWorkflowRunWithData(ctx, run.ID, map[string]any{"category": "video"}); err != nil {
		t.Fatalf("ResumeWorkflowRunWithData(second) error = %v", err)
	}

	run = waitWorkflowRunIDStatus(t, workflowRunRepo, run.ID, "succeeded")
	if run.Status != "succeeded" {
		t.Fatalf("workflow run status = %q, want succeeded", run.Status)
	}

	if resumeExecutor.lastResume["note"] != "first-pass" {
		t.Fatalf("resume note = %#v, want first-pass", resumeExecutor.lastResume["note"])
	}
	if resumeExecutor.lastResume["category"] != "video" {
		t.Fatalf("resume category = %#v, want video", resumeExecutor.lastResume["category"])
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}
}

func TestWorkflowRunnerServicePhase4MoveRollback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	adapter := fs.NewMockAdapter()
	adapter.AddDir("/source", []fs.DirEntry{{Name: "album", IsDir: true}})
	adapter.AddDir("/source/album", []fs.DirEntry{{Name: "001.jpg", IsDir: false, Size: 10}})

	folder := &repository.Folder{ID: "folder-phase4-move-rb", Path: "/source/album", Name: "album", Category: "photo", CategorySource: "workflow", Status: "pending"}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	producer := &processingItemSourceExecutor{items: []ProcessingItem{{FolderID: folder.ID, SourcePath: folder.Path, FolderName: folder.Name, Category: folder.Category}}}
	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "source", Type: producer.Type(), Enabled: true},
			{ID: "move", Type: phase4MoveNodeExecutorType, Enabled: true, Config: map[string]any{"target_dir": "/target"}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "source", SourcePort: "items"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{{ID: "e1", Source: "source", SourcePort: "items", Target: "move", TargetPort: "items"}},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}
	def := &repository.WorkflowDefinition{ID: "wf-phase4-move-rb", Name: "wf-phase4-move-rb", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, adapter, nil, nil)
	svc.RegisterExecutor(producer)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}
	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}

	moved, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() after move error = %v", err)
	}
	if moved.Path != "/target/album" {
		t.Fatalf("folder path after move = %q, want /target/album", moved.Path)
	}
	if moved.Status != "done" {
		t.Fatalf("folder status after move = %q, want done", moved.Status)
	}

	run := waitWorkflowRunByJob(t, workflowRunRepo, jobID)
	if err := svc.RollbackWorkflowRun(ctx, run.ID); err != nil {
		t.Fatalf("RollbackWorkflowRun() error = %v", err)
	}

	rolledBack, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() after rollback error = %v", err)
	}
	if rolledBack.Path != "/source/album" {
		t.Fatalf("folder path after rollback = %q, want /source/album", rolledBack.Path)
	}
	if rolledBack.Status != "pending" {
		t.Fatalf("folder status after rollback = %q, want pending", rolledBack.Status)
	}

	existsTarget, err := adapter.Exists(ctx, "/target/album")
	if err != nil {
		t.Fatalf("adapter.Exists(target) error = %v", err)
	}
	if existsTarget {
		t.Fatalf("target path should not exist after rollback")
	}
	existsSource, err := adapter.Exists(ctx, "/source/album")
	if err != nil {
		t.Fatalf("adapter.Exists(source) error = %v", err)
	}
	if !existsSource {
		t.Fatalf("source path should exist after rollback")
	}
}

func TestWorkflowRunnerServiceRejectsEmptyRequiredOutput(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)
	auditRepo := repository.NewAuditRepository(database)
	auditSvc := NewAuditService(auditRepo)

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "n1", Type: "empty-required-output", Enabled: true},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}
	def := &repository.WorkflowDefinition{ID: "wf-empty-required-output", Name: "wf-empty-required-output", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, fs.NewMockAdapter(), nil, auditSvc)
	svc.RegisterExecutor(&emptyRequiredOutputExecutor{})

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "failed" {
		t.Fatalf("job status = %q, want failed", job.Status)
	}
	if strings.TrimSpace(job.Error) == "" {
		t.Fatalf("job error is empty")
	}

	run := waitWorkflowRunByJob(t, workflowRunRepo, jobID)
	if run.Status != "failed" {
		t.Fatalf("workflow run status = %q, want failed", run.Status)
	}
	if strings.TrimSpace(run.Error) == "" {
		t.Fatalf("workflow run error is empty")
	}
	nodeRuns, _, err := nodeRunRepo.List(ctx, repository.NodeRunListFilter{WorkflowRunID: run.ID, Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("nodeRunRepo.List() error = %v", err)
	}
	if len(nodeRuns) == 0 {
		t.Fatalf("node runs len = 0, want >= 1")
	}
	if nodeRuns[0].Status != "failed" {
		t.Fatalf("node status = %q, want failed", nodeRuns[0].Status)
	}
	if strings.TrimSpace(nodeRuns[0].Error) == "" {
		t.Fatalf("node error is empty")
	}

	if nodeRuns[0].Error != run.Error {
		t.Fatalf("node error = %q, want workflow run error %q", nodeRuns[0].Error, run.Error)
	}
	if !strings.Contains(job.Error, run.Error) {
		t.Fatalf("job error = %q, want contain workflow run error %q", job.Error, run.Error)
	}

	failedNodeAudits, _, err := auditRepo.List(ctx, repository.AuditListFilter{
		WorkflowRunID: run.ID,
		NodeRunID:     nodeRuns[0].ID,
		Result:        "failed",
		Page:          1,
		Limit:         20,
	})
	if err != nil {
		t.Fatalf("auditRepo.List(node failed) error = %v", err)
	}
	if len(failedNodeAudits) == 0 {
		t.Fatalf("node failed audit logs len = 0, want >= 1")
	}
	if failedNodeAudits[0].ErrorMsg != nodeRuns[0].Error {
		t.Fatalf("node failed audit error = %q, want %q", failedNodeAudits[0].ErrorMsg, nodeRuns[0].Error)
	}

	workflowFailedAudits, _, err := auditRepo.List(ctx, repository.AuditListFilter{
		WorkflowRunID: run.ID,
		Action:        "workflow.run.failed",
		Result:        "failed",
		Page:          1,
		Limit:         20,
	})
	if err != nil {
		t.Fatalf("auditRepo.List(workflow failed) error = %v", err)
	}
	if len(workflowFailedAudits) == 0 {
		t.Fatalf("workflow failed audit logs len = 0, want >= 1")
	}
	if workflowFailedAudits[0].ErrorMsg != run.Error {
		t.Fatalf("workflow failed audit error = %q, want %q", workflowFailedAudits[0].ErrorMsg, run.Error)
	}
}

func TestWorkflowRunnerServiceRunsSameLevelNodesConcurrentlyWithCollect(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	var active int32
	var maxActive int32
	branchA := &branchSlowItemsExecutor{branch: "a", active: &active, maxActive: &maxActive}
	branchB := &branchSlowItemsExecutor{branch: "b", active: &active, maxActive: &maxActive}

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "trigger", Type: "trigger", Enabled: true},
			{ID: "a", Type: branchA.Type(), Enabled: true},
			{ID: "b", Type: branchB.Type(), Enabled: true},
			{ID: "collect", Type: collectNodeExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items_1": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "a", SourcePort: "items"}},
				"items_2": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "b", SourcePort: "items"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e3", Source: "a", SourcePort: "items", Target: "collect", TargetPort: "items_1"},
			{ID: "e4", Source: "b", SourcePort: "items", Target: "collect", TargetPort: "items_2"},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}
	def := &repository.WorkflowDefinition{ID: "wf-level-concurrency", Name: "wf-level-concurrency", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, fs.NewMockAdapter(), nil, nil)
	svc.RegisterExecutor(branchA)
	svc.RegisterExecutor(branchB)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}
	if atomic.LoadInt32(&maxActive) < 2 {
		t.Fatalf("maxActive = %d, want >=2 for same-level parallelism", atomic.LoadInt32(&maxActive))
	}
}

// TestEngineV2_AC_PROC1_ProcessingChainRenameAndMove verifies the complete
// processing chain: category-router → rename-node → move-node runs end-to-end
// and the folder ends up at the correct renamed destination.
func TestEngineV2_AC_PROC1_ProcessingChainRenameAndMove(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	adapter := fs.NewMockAdapter()
	adapter.AddDir("/source", []fs.DirEntry{{Name: "Dune[2021]", IsDir: true}})
	adapter.AddDir("/source/Dune[2021]", []fs.DirEntry{{Name: "movie.mkv", IsDir: false, Size: 500}})

	folder := &repository.Folder{ID: "folder-proc1", Path: "/source/Dune[2021]", Name: "Dune[2021]", Category: "video", CategorySource: "workflow", Status: "pending"}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	producer := &processingItemSourceExecutor{items: []ProcessingItem{
		{FolderID: folder.ID, SourcePath: folder.Path, FolderName: folder.Name, TargetName: folder.Name, Category: "video"},
	}}

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "source", Type: producer.Type(), Enabled: true},
			{ID: "router", Type: categoryRouterExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "source", SourcePort: "items"}},
			}},
			{ID: "rename", Type: renameNodeExecutorType, Enabled: true,
				Config: map[string]any{"strategy": "regex_extract", "regex": `^(?P<title>.+?)\[(?P<year>\d{4})\]$`, "template": "{title} ({year})"},
				Inputs: map[string]repository.NodeInputSpec{
					"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "router", SourcePort: "video"}},
				},
			},
			{ID: "move", Type: phase4MoveNodeExecutorType, Enabled: true,
				Config: map[string]any{"target_dir": "/target"},
				Inputs: map[string]repository.NodeInputSpec{
					"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "rename", SourcePort: "items"}},
				},
			},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e1", Source: "source", SourcePort: "items", Target: "router", TargetPort: "items"},
			{ID: "e2", Source: "router", SourcePort: "video", Target: "rename", TargetPort: "items"},
			{ID: "e3", Source: "rename", SourcePort: "items", Target: "move", TargetPort: "items"},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}
	def := &repository.WorkflowDefinition{ID: "wf-proc1", Name: "wf-proc1", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, adapter, nil, nil)
	svc.RegisterExecutor(producer)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}
	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}

	// folder should now be at the renamed destination
	updatedFolder, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() error = %v", err)
	}
	if updatedFolder.Path != "/target/Dune (2021)" {
		t.Fatalf("folder path = %q, want /target/Dune (2021)", updatedFolder.Path)
	}

	dstExists, err := adapter.Exists(ctx, "/target/Dune (2021)")
	if err != nil {
		t.Fatalf("adapter.Exists(dst) error = %v", err)
	}
	if !dstExists {
		t.Fatalf("destination /target/Dune (2021) should exist after move")
	}
}

// TestEngineV2_AC_ROLL4_MultiNodeReverseRollback verifies that RollbackWorkflowRun
// reverses node executions in strict reverse-sequence order: two sequential move-node
// steps both get rolled back, with the later move reversed first.
func TestEngineV2_AC_ROLL4_MultiNodeReverseRollback(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	adapter := fs.NewMockAdapter()
	adapter.AddDir("/source", []fs.DirEntry{{Name: "album", IsDir: true}})
	adapter.AddDir("/source/album", []fs.DirEntry{{Name: "001.jpg", IsDir: false, Size: 10}})

	folder := &repository.Folder{ID: "folder-roll4", Path: "/source/album", Name: "album", Category: "photo", CategorySource: "workflow", Status: "pending"}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	// Graph: source → move1 (/source/album → /dst1/album) → rename → move2 (/dst1/album → /dst2/album)
	producer := &processingItemSourceExecutor{items: []ProcessingItem{
		{FolderID: folder.ID, SourcePath: folder.Path, FolderName: folder.Name, TargetName: folder.Name, Category: "photo"},
	}}

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "source", Type: producer.Type(), Enabled: true},
			{ID: "move1", Type: phase4MoveNodeExecutorType, Enabled: true,
				Config: map[string]any{"target_dir": "/dst1"},
				Inputs: map[string]repository.NodeInputSpec{
					"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "source", SourcePort: "items"}},
				},
			},
			{ID: "rename", Type: renameNodeExecutorType, Enabled: true,
				Config: map[string]any{"strategy": "template", "template": "{name}"},
				Inputs: map[string]repository.NodeInputSpec{
					"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "move1", SourcePort: "items"}},
				},
			},
			{ID: "move2", Type: phase4MoveNodeExecutorType, Enabled: true,
				Config: map[string]any{"target_dir": "/dst2"},
				Inputs: map[string]repository.NodeInputSpec{
					"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "rename", SourcePort: "items"}},
				},
			},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e1", Source: "source", SourcePort: "items", Target: "move1", TargetPort: "items"},
			{ID: "e2", Source: "move1", SourcePort: "items", Target: "rename", TargetPort: "items"},
			{ID: "e3", Source: "rename", SourcePort: "items", Target: "move2", TargetPort: "items"},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}
	def := &repository.WorkflowDefinition{ID: "wf-roll4", Name: "wf-roll4", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, adapter, nil, nil)
	svc.RegisterExecutor(producer)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}
	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}

	// after both moves: folder should be at /dst2/album
	movedFolder, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() after moves error = %v", err)
	}
	if movedFolder.Path != "/dst2/album" {
		t.Fatalf("folder path after moves = %q, want /dst2/album", movedFolder.Path)
	}

	run := waitWorkflowRunByJob(t, workflowRunRepo, jobID)
	if err := svc.RollbackWorkflowRun(ctx, run.ID); err != nil {
		t.Fatalf("RollbackWorkflowRun() error = %v", err)
	}

	// after rollback: folder should be back at /source/album
	rolledBack, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() after rollback error = %v", err)
	}
	if rolledBack.Path != "/source/album" {
		t.Fatalf("folder path after rollback = %q, want /source/album", rolledBack.Path)
	}

	// intermediate path /dst1/album should also be gone (moved back through by reverse rollback)
	dst1Exists, err := adapter.Exists(ctx, "/dst1/album")
	if err != nil {
		t.Fatalf("adapter.Exists(/dst1/album) error = %v", err)
	}
	if dst1Exists {
		t.Fatalf("/dst1/album should not exist after complete rollback")
	}

	dst2Exists, err := adapter.Exists(ctx, "/dst2/album")
	if err != nil {
		t.Fatalf("adapter.Exists(/dst2/album) error = %v", err)
	}
	if dst2Exists {
		t.Fatalf("/dst2/album should not exist after rollback")
	}

	srcExists, err := adapter.Exists(ctx, "/source/album")
	if err != nil {
		t.Fatalf("adapter.Exists(/source/album) error = %v", err)
	}
	if !srcExists {
		t.Fatalf("/source/album should exist after rollback")
	}
}

func TestWorkflowRunnerServiceSingleCategoryBranchesAutoSkip(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	adapter := fs.NewMockAdapter()
	adapter.AddDir("/source", []fs.DirEntry{{Name: "video-a", IsDir: true}})
	adapter.AddDir("/source/video-a", []fs.DirEntry{{Name: "episode-01.mkv", IsDir: false, Size: 123}})

	source := &processingItemListProducerExecutor{
		nodeType: "single-category-source",
		items: []ProcessingItem{
			{SourcePath: "/source/video-a", FolderName: "video-a", TargetName: "video-a", Category: "video"},
		},
	}

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "src", Type: source.Type(), Enabled: true},
			{ID: "router", Type: categoryRouterExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "src", SourcePort: "items"}},
			}},
			{ID: "rename-video", Type: renameNodeExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "router", SourcePort: "video"}},
			}},
			{ID: "rename-manga", Type: renameNodeExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "router", SourcePort: "manga"}},
			}},
			{ID: "rename-photo", Type: renameNodeExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "router", SourcePort: "photo"}},
			}},
			{ID: "rename-other", Type: renameNodeExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "router", SourcePort: "other"}},
			}},
			{ID: "rename-mixed", Type: renameNodeExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "router", SourcePort: "mixed_leaf"}},
			}},
			{ID: "collect", Type: collectNodeExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items_1": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "rename-video", SourcePort: "items"}},
				"items_2": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "rename-manga", SourcePort: "items"}},
				"items_3": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "rename-photo", SourcePort: "items"}},
				"items_4": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "rename-other", SourcePort: "items"}},
				"items_5": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "rename-mixed", SourcePort: "items"}},
			}},
			{ID: "move", Type: phase4MoveNodeExecutorType, Enabled: true, Config: map[string]any{
				"target_dir":      "/target",
				"move_unit":       "folder",
				"conflict_policy": "auto_rename",
			}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "collect", SourcePort: "items"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e-src-router", Source: "src", SourcePort: "items", Target: "router", TargetPort: "items"},
			{ID: "e-router-video", Source: "router", SourcePort: "video", Target: "rename-video", TargetPort: "items"},
			{ID: "e-router-manga", Source: "router", SourcePort: "manga", Target: "rename-manga", TargetPort: "items"},
			{ID: "e-router-photo", Source: "router", SourcePort: "photo", Target: "rename-photo", TargetPort: "items"},
			{ID: "e-router-other", Source: "router", SourcePort: "other", Target: "rename-other", TargetPort: "items"},
			{ID: "e-router-mixed", Source: "router", SourcePort: "mixed_leaf", Target: "rename-mixed", TargetPort: "items"},
			{ID: "e-rv-collect", Source: "rename-video", SourcePort: "items", Target: "collect", TargetPort: "items_1"},
			{ID: "e-rm-collect", Source: "rename-manga", SourcePort: "items", Target: "collect", TargetPort: "items_2"},
			{ID: "e-rp-collect", Source: "rename-photo", SourcePort: "items", Target: "collect", TargetPort: "items_3"},
			{ID: "e-ro-collect", Source: "rename-other", SourcePort: "items", Target: "collect", TargetPort: "items_4"},
			{ID: "e-rx-collect", Source: "rename-mixed", SourcePort: "items", Target: "collect", TargetPort: "items_5"},
			{ID: "e-collect-move", Source: "collect", SourcePort: "items", Target: "move", TargetPort: "items"},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}
	def := &repository.WorkflowDefinition{ID: "wf-single-category-skip", Name: "wf-single-category-skip", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	svc := NewWorkflowRunnerService(jobRepo, folderRepo, repository.NewSnapshotRepository(database), workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, adapter, nil, nil)
	svc.RegisterExecutor(source)

	jobID, err := svc.StartJob(ctx, StartWorkflowJobInput{WorkflowDefID: def.ID})
	if err != nil {
		t.Fatalf("StartJob() error = %v", err)
	}

	job := waitJobDone(t, jobRepo, jobID)
	if job.Status != "succeeded" {
		t.Fatalf("job status = %q, want succeeded", job.Status)
	}

	run := waitWorkflowRunByJob(t, workflowRunRepo, jobID)
	nodeRuns, _, err := nodeRunRepo.List(ctx, repository.NodeRunListFilter{WorkflowRunID: run.ID, Page: 1, Limit: 50})
	if err != nil {
		t.Fatalf("nodeRunRepo.List() error = %v", err)
	}

	if got := nodeRunStatusByID(nodeRuns, "rename-video"); got != "succeeded" {
		t.Fatalf("rename-video status = %q, want succeeded", got)
	}
	if got := nodeRunStatusByID(nodeRuns, "rename-manga"); got != "skipped" {
		t.Fatalf("rename-manga status = %q, want skipped", got)
	}
	if got := nodeRunStatusByID(nodeRuns, "rename-photo"); got != "skipped" {
		t.Fatalf("rename-photo status = %q, want skipped", got)
	}
	if got := nodeRunStatusByID(nodeRuns, "rename-other"); got != "skipped" {
		t.Fatalf("rename-other status = %q, want skipped", got)
	}
	if got := nodeRunStatusByID(nodeRuns, "rename-mixed"); got != "skipped" {
		t.Fatalf("rename-mixed status = %q, want skipped", got)
	}
	if got := nodeRunStatusByID(nodeRuns, "collect"); got != "succeeded" {
		t.Fatalf("collect status = %q, want succeeded", got)
	}
	if got := nodeRunStatusByID(nodeRuns, "move"); got != "succeeded" {
		t.Fatalf("move status = %q, want succeeded", got)
	}

	for _, nodeRun := range nodeRuns {
		switch nodeRun.NodeID {
		case "rename-manga", "rename-photo", "rename-other", "rename-mixed":
			if nodeRun.Error != "skip_reason=empty_input" {
				t.Fatalf("%s error = %q, want skip_reason=empty_input", nodeRun.NodeID, nodeRun.Error)
			}
		}
	}
}

func TestWorkflowRunnerServiceComplexMixedLeafFanOutWithoutMove(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)
	snapshotRepo := repository.NewSnapshotRepository(database)
	auditRepo := repository.NewAuditRepository(database)
	auditSvc := NewAuditService(auditRepo)

	root := t.TempDir()
	sourceRoot := filepath.Join(root, "source")
	targetRoot := filepath.Join(root, "target")
	mixedParentPath := filepath.Join(sourceRoot, "活动记录", "婚礼")
	videoLeafPath := filepath.Join(mixedParentPath, "原片视频")
	photoLeafPath := filepath.Join(mixedParentPath, "相册照片")
	mixedLeafPath := filepath.Join(mixedParentPath, "混合精选")

	writeFile := func(path string) {
		t.Helper()
		mustMkdirAll(t, filepath.Dir(path))
		if err := os.WriteFile(path, []byte("fixture"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(%q) error = %v", path, err)
		}
	}
	writeFile(filepath.Join(videoLeafPath, "clip01.mp4"))
	writeFile(filepath.Join(videoLeafPath, "clip02.mkv"))
	writeFile(filepath.Join(photoLeafPath, "p1.jpg"))
	writeFile(filepath.Join(photoLeafPath, "p2.png"))
	writeFile(filepath.Join(mixedLeafPath, "highlight.mp4"))
	writeFile(filepath.Join(mixedLeafPath, "cover.jpg"))

	videoTargetDir := normalizeWorkflowPath(filepath.Join(targetRoot, "video"))
	videoThumbDir := normalizeWorkflowPath(filepath.Join(targetRoot, "video", "thumbs"))
	photoTargetDir := normalizeWorkflowPath(filepath.Join(targetRoot, "photo"))
	photoArchiveDir := normalizeWorkflowPath(filepath.Join(targetRoot, "photo", "archives"))
	mixedThumbDir := normalizeWorkflowPath(filepath.Join(targetRoot, "mixed", "thumbs"))
	mixedArchiveDir := normalizeWorkflowPath(filepath.Join(targetRoot, "mixed", "archives"))

	entrySource := &classifiedEntrySourceExecutor{
		nodeType: "classified-entry-source",
		entry: ClassifiedEntry{
			Path:     normalizeWorkflowPath(mixedParentPath),
			Name:     "婚礼",
			Category: "mixed",
			Subtree: []ClassifiedEntry{
				{Path: normalizeWorkflowPath(videoLeafPath), Name: "原片视频", Category: "video"},
				{Path: normalizeWorkflowPath(photoLeafPath), Name: "相册照片", Category: "photo"},
				{Path: normalizeWorkflowPath(mixedLeafPath), Name: "混合精选", Category: "mixed"},
			},
		},
	}

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "src", Type: entrySource.Type(), Enabled: true},
			{ID: "reader", Type: classificationReaderExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"entry": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "src", SourcePort: "entry"}},
			}},
			{ID: "split", Type: folderSplitterExecutorType, Enabled: true, Config: map[string]any{
				"split_mixed":        true,
				"split_with_subdirs": true,
			}, Inputs: map[string]repository.NodeInputSpec{
				"entry": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "reader", SourcePort: "entry"}},
			}},
			{ID: "router", Type: categoryRouterExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "split", SourcePort: "items"}},
			}},
			{ID: "mixed-router", Type: mixedLeafRouterExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "router", SourcePort: "mixed_leaf"}},
			}},
			{ID: "rename-video", Type: renameNodeExecutorType, Enabled: true, Config: map[string]any{
				"strategy": "template",
				"template": "{name}",
			}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "router", SourcePort: "video"}},
			}},
			{ID: "move-video", Type: phase4MoveNodeExecutorType, Enabled: true, Config: map[string]any{
				"target_dir": videoTargetDir,
			}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "rename-video", SourcePort: "items"}},
			}},
			{ID: "thumbnail-video", Type: thumbnailNodeExecutorType, Enabled: true, Config: map[string]any{
				"output_dir": videoThumbDir,
			}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "move-video", SourcePort: "items"}},
			}},
			{ID: "rename-photo", Type: renameNodeExecutorType, Enabled: true, Config: map[string]any{
				"strategy": "template",
				"template": "{name}",
			}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "router", SourcePort: "photo"}},
			}},
			{ID: "move-photo", Type: phase4MoveNodeExecutorType, Enabled: true, Config: map[string]any{
				"target_dir": photoTargetDir,
			}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "rename-photo", SourcePort: "items"}},
			}},
			{ID: "compress-photo", Type: compressNodeExecutorType, Enabled: true, Config: map[string]any{
				"target_dir": photoArchiveDir,
			}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "move-photo", SourcePort: "items"}},
			}},
			{ID: "thumbnail-mixed", Type: thumbnailNodeExecutorType, Enabled: true, Config: map[string]any{
				"output_dir": mixedThumbDir,
			}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "mixed-router", SourcePort: "video"}},
			}},
			{ID: "compress-mixed", Type: compressNodeExecutorType, Enabled: true, Config: map[string]any{
				"target_dir": mixedArchiveDir,
			}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "mixed-router", SourcePort: "photo"}},
			}},
			{ID: "mixed-collect", Type: collectNodeExecutorType, Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items_1": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "thumbnail-mixed", SourcePort: "items"}},
				"items_2": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "compress-mixed", SourcePort: "items"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e-src-reader", Source: "src", SourcePort: "entry", Target: "reader", TargetPort: "entry"},
			{ID: "e-reader-split", Source: "reader", SourcePort: "entry", Target: "split", TargetPort: "entry"},
			{ID: "e-split-router", Source: "split", SourcePort: "items", Target: "router", TargetPort: "items"},
			{ID: "e-router-mixed-router", Source: "router", SourcePort: "mixed_leaf", Target: "mixed-router", TargetPort: "items"},
			{ID: "e-router-rename-video", Source: "router", SourcePort: "video", Target: "rename-video", TargetPort: "items"},
			{ID: "e-rename-video-move", Source: "rename-video", SourcePort: "items", Target: "move-video", TargetPort: "items"},
			{ID: "e-move-video-thumb", Source: "move-video", SourcePort: "items", Target: "thumbnail-video", TargetPort: "items"},
			{ID: "e-router-rename-photo", Source: "router", SourcePort: "photo", Target: "rename-photo", TargetPort: "items"},
			{ID: "e-rename-photo-move", Source: "rename-photo", SourcePort: "items", Target: "move-photo", TargetPort: "items"},
			{ID: "e-move-photo-compress", Source: "move-photo", SourcePort: "items", Target: "compress-photo", TargetPort: "items"},
			{ID: "e-mixed-router-thumb", Source: "mixed-router", SourcePort: "video", Target: "thumbnail-mixed", TargetPort: "items"},
			{ID: "e-mixed-router-compress", Source: "mixed-router", SourcePort: "photo", Target: "compress-mixed", TargetPort: "items"},
			{ID: "e-mixed-thumb-collect", Source: "thumbnail-mixed", SourcePort: "items", Target: "mixed-collect", TargetPort: "items_1"},
			{ID: "e-mixed-compress-collect", Source: "compress-mixed", SourcePort: "items", Target: "mixed-collect", TargetPort: "items_2"},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}
	def := &repository.WorkflowDefinition{ID: "wf-complex-mixed-leaf-fanout", Name: "wf-complex-mixed-leaf-fanout", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := workflowDefRepo.Create(ctx, def); err != nil {
		t.Fatalf("workflowDefRepo.Create() error = %v", err)
	}

	adapter := fs.NewOSAdapter()
	svc := NewWorkflowRunnerService(jobRepo, folderRepo, snapshotRepo, workflowDefRepo, workflowRunRepo, nodeRunRepo, nodeSnapshotRepo, adapter, nil, auditSvc)
	svc.RegisterExecutor(entrySource)
	testThumbnailExecutor := newThumbnailNodeExecutor(adapter, folderRepo)
	testThumbnailExecutor.lookPath = func(string) (string, error) {
		return "/usr/bin/ffmpeg", nil
	}
	testThumbnailExecutor.runFFmpeg = func(ctx context.Context, _ string, args ...string) ([]byte, error) {
		if len(args) == 0 {
			return nil, nil
		}
		outputPath := args[len(args)-1]
		writer, err := adapter.OpenFileWrite(ctx, outputPath, 0o644)
		if err != nil {
			return nil, err
		}
		if _, err := writer.Write([]byte("thumb")); err != nil {
			_ = writer.Close()
			return nil, err
		}
		if err := writer.Close(); err != nil {
			return nil, err
		}

		return nil, nil
	}
	svc.RegisterExecutor(testThumbnailExecutor)

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

	run := waitWorkflowRunByJob(t, workflowRunRepo, jobID)
	nodeRuns, _, err := nodeRunRepo.List(ctx, repository.NodeRunListFilter{WorkflowRunID: run.ID, Page: 1, Limit: 80})
	if err != nil {
		t.Fatalf("nodeRunRepo.List() error = %v", err)
	}

	for _, nodeID := range []string{"split", "router", "mixed-router", "rename-video", "move-video", "thumbnail-video", "rename-photo", "move-photo", "compress-photo", "thumbnail-mixed", "compress-mixed", "mixed-collect"} {
		if got := nodeRunStatusByID(nodeRuns, nodeID); got != "succeeded" {
			t.Fatalf("%s status = %q, want succeeded", nodeID, got)
		}
	}

	splitRun := nodeRunByID(nodeRuns, "split")
	if splitRun == nil {
		t.Fatalf("split node run not found")
	}
	splitOutputs, typed, err := parseTypedNodeOutputs(splitRun.OutputJSON)
	if err != nil {
		t.Fatalf("parse split output error = %v", err)
	}
	if !typed {
		t.Fatalf("split output is not typed")
	}
	splitItems, ok := categoryRouterToItems(splitOutputs["items"].Value)
	if !ok {
		t.Fatalf("split items output type = %T, want processing items", splitOutputs["items"].Value)
	}
	if len(splitItems) != 3 {
		t.Fatalf("split items len = %d, want 3", len(splitItems))
	}
	splitPathSet := map[string]struct{}{}
	for _, item := range splitItems {
		splitPathSet[normalizeWorkflowPath(item.SourcePath)] = struct{}{}
	}
	if _, exists := splitPathSet[normalizeWorkflowPath(mixedParentPath)]; exists {
		t.Fatalf("split output should not contain non-leaf mixed path %q", mixedParentPath)
	}
	for _, expected := range []string{normalizeWorkflowPath(videoLeafPath), normalizeWorkflowPath(photoLeafPath), normalizeWorkflowPath(mixedLeafPath)} {
		if _, exists := splitPathSet[expected]; !exists {
			t.Fatalf("split output missing expected leaf path %q", expected)
		}
	}

	routerRun := nodeRunByID(nodeRuns, "router")
	if routerRun == nil {
		t.Fatalf("router node run not found")
	}
	routerOutputs, typed, err := parseTypedNodeOutputs(routerRun.OutputJSON)
	if err != nil {
		t.Fatalf("parse router output error = %v", err)
	}
	if !typed {
		t.Fatalf("router output is not typed")
	}
	mixedLeafItems, ok := categoryRouterToItems(routerOutputs["mixed_leaf"].Value)
	if !ok {
		t.Fatalf("router mixed_leaf output type = %T, want processing items", routerOutputs["mixed_leaf"].Value)
	}
	if len(mixedLeafItems) != 1 {
		t.Fatalf("router mixed_leaf len = %d, want 1", len(mixedLeafItems))
	}
	if got := normalizeWorkflowPath(mixedLeafItems[0].SourcePath); got != normalizeWorkflowPath(mixedLeafPath) {
		t.Fatalf("router mixed_leaf source_path = %q, want %q", got, normalizeWorkflowPath(mixedLeafPath))
	}

	mixedRouterRun := nodeRunByID(nodeRuns, "mixed-router")
	if mixedRouterRun == nil {
		t.Fatalf("mixed-router node run not found")
	}
	mixedRouterOutputs, typed, err := parseTypedNodeOutputs(mixedRouterRun.OutputJSON)
	if err != nil {
		t.Fatalf("parse mixed-router output error = %v", err)
	}
	if !typed {
		t.Fatalf("mixed-router output is not typed")
	}
	mixedVideoItems, ok := categoryRouterToItems(mixedRouterOutputs["video"].Value)
	if !ok || len(mixedVideoItems) != 1 {
		t.Fatalf("mixed-router video output type/len = %T/%d, want []ProcessingItem/1", mixedRouterOutputs["video"].Value, len(mixedVideoItems))
	}
	mixedPhotoItems, ok := categoryRouterToItems(mixedRouterOutputs["photo"].Value)
	if !ok || len(mixedPhotoItems) != 1 {
		t.Fatalf("mixed-router photo output type/len = %T/%d, want []ProcessingItem/1", mixedRouterOutputs["photo"].Value, len(mixedPhotoItems))
	}

	mixedUnexpectedMovePath := normalizeWorkflowPath(filepath.Join(targetRoot, "video", "混合精选"))
	if pathExists(t, mixedUnexpectedMovePath) {
		t.Fatalf("mixed leaf path should not be moved to %q", mixedUnexpectedMovePath)
	}

	mixedThumbPath := normalizeWorkflowPath(filepath.Join(targetRoot, "mixed", "thumbs", "混合精选.jpg"))
	mixedArchivePath := normalizeWorkflowPath(filepath.Join(targetRoot, "mixed", "archives", "混合精选.cbz"))
	videoThumbsDirPath := normalizeWorkflowPath(filepath.Join(targetRoot, "video", "thumbs"))
	photoArchiveDirPath := normalizeWorkflowPath(filepath.Join(targetRoot, "photo", "archives"))
	mixedThumbPath = normalizeWorkflowPath(filepath.Join(targetRoot, "mixed", "thumbs", "highlight.jpg"))
	videoThumbEntries, err := os.ReadDir(videoThumbsDirPath)
	if err != nil {
		t.Fatalf("os.ReadDir(%q) error = %v", videoThumbsDirPath, err)
	}
	if len(videoThumbEntries) == 0 {
		t.Fatalf("video thumbnails should exist under %q", videoThumbsDirPath)
	}
	photoArchiveEntries, err := os.ReadDir(photoArchiveDirPath)
	if err != nil {
		t.Fatalf("os.ReadDir(%q) error = %v", photoArchiveDirPath, err)
	}
	if len(photoArchiveEntries) == 0 {
		t.Fatalf("photo archives should exist under %q", photoArchiveDirPath)
	}
	if !pathExists(t, mixedThumbPath) {
		t.Fatalf("mixed thumbnail path %q should exist", mixedThumbPath)
	}
	if !pathExists(t, mixedArchivePath) {
		t.Fatalf("mixed archive path %q should exist", mixedArchivePath)
	}

	videoEntries, err := os.ReadDir(videoLeafPath)
	if err != nil {
		t.Fatalf("os.ReadDir(%q) error = %v", videoLeafPath, err)
	}
	if len(videoEntries) != 0 {
		t.Fatalf("source video leaf %q should be empty after merge move", videoLeafPath)
	}
	photoEntries, err := os.ReadDir(photoLeafPath)
	if err != nil {
		t.Fatalf("os.ReadDir(%q) error = %v", photoLeafPath, err)
	}
	if len(photoEntries) != 0 {
		t.Fatalf("source photo leaf %q should be empty after merge move", photoLeafPath)
	}
	if !pathExists(t, normalizeWorkflowPath(mixedLeafPath)) {
		t.Fatalf("mixed leaf %q should remain in source path", normalizeWorkflowPath(mixedLeafPath))
	}

	remainingEntries, err := os.ReadDir(mixedLeafPath)
	if err != nil {
		t.Fatalf("os.ReadDir(%q) error = %v", mixedLeafPath, err)
	}
	if len(remainingEntries) == 0 {
		t.Fatalf("mixed leaf %q should remain non-empty after processing", mixedLeafPath)
	}

	logs, _, err := auditRepo.List(ctx, repository.AuditListFilter{
		JobID:  jobID,
		Action: "workflow.compress-node",
		Page:   1,
		Limit:  20,
	})
	if err != nil {
		t.Fatalf("auditRepo.List() error = %v", err)
	}
	foundMixedCompress := false
	for _, log := range logs {
		if normalizeWorkflowPath(log.FolderPath) != normalizeWorkflowPath(mixedArchivePath) {
			continue
		}
		if log.Result != "success" {
			continue
		}
		foundMixedCompress = true
		break
	}
	if !foundMixedCompress {
		t.Fatalf("missing mixed compress audit log for %q", normalizeWorkflowPath(mixedArchivePath))
	}
}

func TestSyncFolderStatusesByWorkflowRunPartialBuildsMappingsWithoutValidation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	folder := &repository.Folder{
		ID:             "folder-partial-output",
		Path:           "/source/folder-partial-output",
		SourceDir:      "/source",
		RelativePath:   "folder-partial-output",
		Name:           "folder-partial-output",
		Category:       "photo",
		CategorySource: "manual",
		Status:         "done",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	job := &repository.Job{
		ID:            "job-partial-output",
		Type:          "workflow",
		WorkflowDefID: "wf-partial-output",
		Status:        "running",
		FolderIDs:     `["folder-partial-output"]`,
		Total:         1,
	}
	if err := jobRepo.Create(ctx, job); err != nil {
		t.Fatalf("jobRepo.Create() error = %v", err)
	}

	run := &repository.WorkflowRun{
		ID:            "run-partial-output",
		JobID:         job.ID,
		WorkflowDefID: job.WorkflowDefID,
		FolderID:      folder.ID,
		Status:        "running",
	}
	if err := workflowRunRepo.Create(ctx, run); err != nil {
		t.Fatalf("workflowRunRepo.Create() error = %v", err)
	}

	if err := nodeRunRepo.Create(ctx, &repository.NodeRun{
		ID:            "node-run-partial-output",
		WorkflowRunID: run.ID,
		NodeID:        "rename",
		NodeType:      renameNodeExecutorType,
		Sequence:      1,
		Status:        "succeeded",
		OutputJSON:    "{}",
	}); err != nil {
		t.Fatalf("nodeRunRepo.Create() error = %v", err)
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

	manifest := &stubManifestBuilder{}
	mapping := &stubMappingBuilder{}
	validator := &stubOutputValidator{}
	completion := &stubCompletionUpdater{}
	svc.SetSourceManifestBuilder(manifest)
	svc.SetOutputPipeline(mapping, validator, completion)

	if err := svc.syncFolderStatusesByWorkflowRun(ctx, run.ID, "partial"); err != nil {
		t.Fatalf("syncFolderStatusesByWorkflowRun() error = %v", err)
	}

	if manifest.ensureCalls != 1 {
		t.Fatalf("manifest ensure calls = %d, want 1", manifest.ensureCalls)
	}
	if manifest.lastRunID != run.ID {
		t.Fatalf("manifest run_id = %q, want %q", manifest.lastRunID, run.ID)
	}
	if len(manifest.lastItems) != 1 || manifest.lastItems[0].FolderID != folder.ID {
		t.Fatalf("manifest items = %+v, want one item with folder_id=%q", manifest.lastItems, folder.ID)
	}

	if mapping.buildCalls != 1 {
		t.Fatalf("mapping build calls = %d, want 1", mapping.buildCalls)
	}
	if mapping.lastRunID != run.ID {
		t.Fatalf("mapping run_id = %q, want %q", mapping.lastRunID, run.ID)
	}

	if validator.validateWorkflowRunCalls != 0 {
		t.Fatalf("validator calls = %d, want 0 in partial path", validator.validateWorkflowRunCalls)
	}

	if completion.syncCalls != 0 {
		t.Fatalf("completion sync calls = %d, want 0 in partial path", completion.syncCalls)
	}
	if completion.markPendingCalls != 1 || completion.lastFolderID != folder.ID {
		t.Fatalf("completion mark_pending calls/folder = %d/%q, want 1/%q", completion.markPendingCalls, completion.lastFolderID, folder.ID)
	}
}

func TestSyncFolderStatusesByWorkflowRunSucceededWithoutOutputChecksMarksPending(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)

	folder := &repository.Folder{
		ID:             "folder-empty-output",
		Path:           "/source/folder-empty-output",
		SourceDir:      "/source",
		RelativePath:   "folder-empty-output",
		Name:           "folder-empty-output",
		Category:       "mixed",
		CategorySource: "workflow",
		Status:         "done",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	job := &repository.Job{
		ID:            "job-empty-output",
		Type:          "workflow",
		WorkflowDefID: "wf-empty-output",
		Status:        "running",
		FolderIDs:     `["folder-empty-output"]`,
		Total:         1,
	}
	if err := jobRepo.Create(ctx, job); err != nil {
		t.Fatalf("jobRepo.Create() error = %v", err)
	}

	run := &repository.WorkflowRun{
		ID:            "run-empty-output",
		JobID:         job.ID,
		WorkflowDefID: job.WorkflowDefID,
		FolderID:      folder.ID,
		Status:        "running",
	}
	if err := workflowRunRepo.Create(ctx, run); err != nil {
		t.Fatalf("workflowRunRepo.Create() error = %v", err)
	}

	if err := nodeRunRepo.Create(ctx, &repository.NodeRun{
		ID:            "node-run-empty-output",
		WorkflowRunID: run.ID,
		NodeID:        "rename",
		NodeType:      renameNodeExecutorType,
		Sequence:      1,
		Status:        "skipped",
		OutputJSON:    "",
	}); err != nil {
		t.Fatalf("nodeRunRepo.Create() error = %v", err)
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

	manifest := &stubManifestBuilder{}
	mapping := &stubMappingBuilder{}
	validator := &stubOutputValidator{}
	completion := &stubCompletionUpdater{}
	svc.SetSourceManifestBuilder(manifest)
	svc.SetOutputPipeline(mapping, validator, completion)

	if err := svc.syncFolderStatusesByWorkflowRun(ctx, run.ID, "succeeded"); err != nil {
		t.Fatalf("syncFolderStatusesByWorkflowRun() error = %v", err)
	}

	if manifest.ensureCalls != 1 {
		t.Fatalf("manifest ensure calls = %d, want 1", manifest.ensureCalls)
	}
	if mapping.buildCalls != 1 {
		t.Fatalf("mapping build calls = %d, want 1", mapping.buildCalls)
	}
	if validator.validateWorkflowRunCalls != 1 {
		t.Fatalf("validator calls = %d, want 1", validator.validateWorkflowRunCalls)
	}
	if completion.syncCalls != 0 {
		t.Fatalf("completion sync calls = %d, want 0", completion.syncCalls)
	}
	if completion.markPendingCalls != 1 || completion.lastFolderID != folder.ID {
		t.Fatalf("completion mark_pending calls/folder = %d/%q, want 1/%q", completion.markPendingCalls, completion.lastFolderID, folder.ID)
	}
}

func TestSyncFolderStatusesByWorkflowRunSucceededMarksFolderDoneAfterOutputCheck(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	jobRepo := repository.NewJobRepository(database)
	folderRepo := repository.NewFolderRepository(database)
	configRepo := repository.NewConfigRepository(database)
	workflowDefRepo := repository.NewWorkflowDefinitionRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	nodeSnapshotRepo := repository.NewNodeSnapshotRepository(database)
	sourceManifestRepo := repository.NewSourceManifestRepository(database)
	outputMappingRepo := repository.NewOutputMappingRepository(database)
	outputCheckRepo := repository.NewOutputCheckRepository(database)
	adapter := fs.NewMockAdapter()

	if err := configRepo.SaveAppConfig(ctx, &repository.AppConfig{
		Version: 1,
		OutputDirs: repository.AppConfigOutputDirs{
			Video: []string{"/target/video"},
			Mixed: []string{"/target/mixed"},
		},
	}); err != nil {
		t.Fatalf("configRepo.SaveAppConfig() error = %v", err)
	}

	folder := &repository.Folder{
		ID:             "folder-output-pipeline-done",
		Path:           "/source/video-season",
		SourceDir:      "/source",
		RelativePath:   "video-season",
		Name:           "video-season",
		Category:       "video",
		CategorySource: "workflow",
		Status:         "pending",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	job := &repository.Job{
		ID:            "job-output-pipeline-done",
		Type:          "workflow",
		WorkflowDefID: "wf-output-pipeline-done",
		Status:        "running",
		FolderIDs:     `["folder-output-pipeline-done"]`,
		Total:         1,
	}
	if err := jobRepo.Create(ctx, job); err != nil {
		t.Fatalf("jobRepo.Create() error = %v", err)
	}

	run := &repository.WorkflowRun{
		ID:            "run-output-pipeline-done",
		JobID:         job.ID,
		WorkflowDefID: job.WorkflowDefID,
		FolderID:      folder.ID,
		Status:        "running",
	}
	if err := workflowRunRepo.Create(ctx, run); err != nil {
		t.Fatalf("workflowRunRepo.Create() error = %v", err)
	}

	if err := sourceManifestRepo.CreateBatchForWorkflowRun(ctx, run.ID, folder.ID, "batch-output-pipeline-done", []*repository.FolderSourceManifest{{
		ID:            "manifest-output-pipeline-done",
		WorkflowRunID: run.ID,
		FolderID:      folder.ID,
		BatchID:       "batch-output-pipeline-done",
		SourcePath:    "/source/video-season/movie.mkv",
		RelativePath:  "movie.mkv",
		FileName:      "movie.mkv",
		SizeBytes:     10,
	}}); err != nil {
		t.Fatalf("sourceManifestRepo.CreateBatchForWorkflowRun() error = %v", err)
	}

	outputJSON := mustJSONMarshal(t, mustTypedOutputsMap(t, map[string]TypedValue{
		"items": {
			Type: PortTypeProcessingItemList,
			Value: []ProcessingItem{{
				FolderID:    folder.ID,
				SourcePath:  "/target/video/video-season",
				CurrentPath: "/target/video/video-season",
				FolderName:  "video-season",
				Category:    "video",
			}},
		},
		"step_results": {
			Type: PortTypeProcessingStepResultList,
			Value: []ProcessingStepResult{{
				FolderID:   folder.ID,
				SourcePath: "/source/video-season",
				TargetPath: "/target/video/video-season",
				NodeType:   phase4MoveNodeExecutorType,
				Status:     "moved",
			}},
		},
	}))
	if err := nodeRunRepo.Create(ctx, &repository.NodeRun{
		ID:            "node-run-output-pipeline-done",
		WorkflowRunID: run.ID,
		NodeID:        "move-video",
		NodeType:      phase4MoveNodeExecutorType,
		Sequence:      1,
		Status:        "succeeded",
		OutputJSON:    outputJSON,
	}); err != nil {
		t.Fatalf("nodeRunRepo.Create() error = %v", err)
	}

	adapter.AddFile("/target/video/video-season/movie.mkv", []byte("video"))

	svc := NewWorkflowRunnerService(
		jobRepo,
		folderRepo,
		repository.NewSnapshotRepository(database),
		workflowDefRepo,
		workflowRunRepo,
		nodeRunRepo,
		nodeSnapshotRepo,
		adapter,
		nil,
		nil,
	)
	svc.SetOutputPipeline(
		NewOutputMappingService(workflowRunRepo, nodeRunRepo, folderRepo, outputMappingRepo),
		NewOutputValidationService(adapter, folderRepo, configRepo, sourceManifestRepo, outputMappingRepo, outputCheckRepo),
		NewFolderCompletionService(folderRepo, outputCheckRepo),
	)

	if err := svc.syncFolderStatusesByWorkflowRun(ctx, run.ID, "succeeded"); err != nil {
		t.Fatalf("syncFolderStatusesByWorkflowRun() error = %v", err)
	}

	updated, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() error = %v", err)
	}
	if updated.Status != "done" {
		t.Fatalf("folder status = %q, want done", updated.Status)
	}
	if updated.OutputCheckSummary.Status != "passed" {
		t.Fatalf("output_check_summary status = %q, want passed", updated.OutputCheckSummary.Status)
	}
}

func nodeRunByID(nodeRuns []*repository.NodeRun, nodeID string) *repository.NodeRun {
	for _, nodeRun := range nodeRuns {
		if nodeRun.NodeID == nodeID {
			return nodeRun
		}
	}

	return nil
}

func waitJobDone(t *testing.T, jobRepo repository.JobRepository, jobID string) *repository.Job {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		job, err := jobRepo.GetByID(context.Background(), jobID)
		if err != nil {
			t.Fatalf("jobRepo.GetByID() error = %v", err)
		}
		if job.Status == "succeeded" || job.Status == "failed" || job.Status == "partial" {
			return job
		}
		time.Sleep(10 * time.Millisecond)
	}

	t.Fatalf("timeout waiting job %q done", jobID)
	return nil
}
