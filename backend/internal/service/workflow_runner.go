package service

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/sse"
)

type StartWorkflowJobInput struct {
	WorkflowDefID string
	SourceDir     string
}

type WorkflowRunDetail struct {
	Run           *repository.WorkflowRun
	NodeRuns      []*repository.NodeRun
	ReviewSummary *ProcessingReviewSummary
}

type ProcessingReviewSummary struct {
	Total          int `json:"total"`
	Pending        int `json:"pending"`
	Approved       int `json:"approved"`
	RolledBack     int `json:"rolled_back"`
	Rejected       int `json:"rejected"`
	FailedStepRuns int `json:"failed_step_runs"`
}

type NodeExecutionInput struct {
	WorkflowRun *repository.WorkflowRun
	NodeRun     *repository.NodeRun
	Node        repository.WorkflowGraphNode
	Folder      *repository.Folder
	SourceDir   string
	AppConfig   *repository.AppConfig
	Inputs      map[string]*TypedValue
	ProgressFn  func(percent int, msg string)
}

type ExecutionStatus string

const (
	ExecutionSuccess ExecutionStatus = "success"
	ExecutionFailure ExecutionStatus = "failure"
	ExecutionPending ExecutionStatus = "pending"
)

var processingStatusDrivenNodeTypes = map[string]struct{}{
	renameNodeExecutorType:     {},
	phase4MoveNodeExecutorType: {},
	compressNodeExecutorType:   {},
	thumbnailNodeExecutorType:  {},
}

type NodeExecutionOutput struct {
	Outputs       map[string]TypedValue
	Status        ExecutionStatus
	PendingReason string
	ErrorCode     string
	PendingState  map[string]any
}

type PortDef struct {
	Name           string   `json:"name"`
	Type           PortType `json:"type"`
	Required       bool     `json:"required"`
	Lazy           bool     `json:"lazy"`
	SkipOnEmpty    bool     `json:"skip_on_empty,omitempty"`
	AcceptDefault  bool     `json:"accept_default,omitempty"`
	RequiredOutput bool     `json:"required_output,omitempty"`
	AllowEmpty     bool     `json:"allow_empty,omitempty"`
	Description    string   `json:"description"`
}

type InputValueSource string

const (
	InputValueSourceMissing                    InputValueSource = "missing"
	InputValueSourceResolved                   InputValueSource = "resolved"
	InputValueSourceEmptyOutput                InputValueSource = "empty_output"
	InputValueSourceDefaultFromSkippedUpstream InputValueSource = "default_from_skipped_upstream"
)

type NodeSchema struct {
	Type         string         `json:"type,omitempty"`
	TypeID       string         `json:"type_id,omitempty"`
	Label        string         `json:"label,omitempty"`
	DisplayName  string         `json:"display_name,omitempty"`
	Description  string         `json:"description"`
	Category     string         `json:"category,omitempty"`
	Inputs       []PortDef      `json:"input_ports,omitempty"`
	Outputs      []PortDef      `json:"output_ports,omitempty"`
	ConfigSchema map[string]any `json:"config_schema,omitempty"`
}

func (s NodeSchema) TypeName() string {
	if s.TypeID != "" {
		return s.TypeID
	}
	return s.Type
}

func (s NodeSchema) DisplayLabel() string {
	if s.DisplayName != "" {
		return s.DisplayName
	}
	return s.Label
}

func (s NodeSchema) InputDefs() []PortDef {
	return s.Inputs
}

func (s NodeSchema) OutputDefs() []PortDef {
	return s.Outputs
}

func (s NodeSchema) InputPort(name string) *PortDef {
	for _, port := range s.InputDefs() {
		if port.Name == name {
			candidate := port
			return &candidate
		}
	}
	return nil
}

func (s NodeSchema) OutputPort(name string) *PortDef {
	for _, port := range s.OutputDefs() {
		if port.Name == name {
			candidate := port
			return &candidate
		}
	}
	return nil
}

type NodeRollbackInput struct {
	WorkflowRun *repository.WorkflowRun
	NodeRun     *repository.NodeRun
	Snapshots   []*repository.NodeSnapshot
	Folder      *repository.Folder
}

type WorkflowNodeExecutor interface {
	Type() string
	Schema() NodeSchema
	Execute(ctx context.Context, input NodeExecutionInput) (NodeExecutionOutput, error)
	Resume(ctx context.Context, input NodeExecutionInput, resumeData map[string]any) (NodeExecutionOutput, error)
	Rollback(ctx context.Context, input NodeRollbackInput) error
}

type WorkflowRunnerService struct {
	jobs          repository.JobRepository
	folders       repository.FolderRepository
	snapshots     repository.SnapshotRepository
	reviews       repository.ProcessingReviewRepository
	workflowDefs  repository.WorkflowDefinitionRepository
	workflowRuns  repository.WorkflowRunRepository
	nodeRuns      repository.NodeRunRepository
	nodeSnapshots repository.NodeSnapshotRepository
	executors     map[string]WorkflowNodeExecutor
	broker        *sse.Broker
	auditSvc      *AuditService
	typeRegistry  *TypeRegistry
	config        repository.ConfigRepository
	manifestSvc   SourceManifestBuilder
	mappingSvc    OutputMappingBuilder
	validatorSvc  OutputValidator
	completionSvc FolderCompletionUpdater
}

func NewWorkflowRunnerService(
	jobRepo repository.JobRepository,
	folderRepo repository.FolderRepository,
	snapshotRepo repository.SnapshotRepository,
	workflowDefRepo repository.WorkflowDefinitionRepository,
	workflowRunRepo repository.WorkflowRunRepository,
	nodeRunRepo repository.NodeRunRepository,
	nodeSnapshotRepo repository.NodeSnapshotRepository,
	fsAdapter fs.FSAdapter,
	broker *sse.Broker,
	auditSvc *AuditService,
) *WorkflowRunnerService {
	svc := &WorkflowRunnerService{
		jobs:          jobRepo,
		folders:       folderRepo,
		snapshots:     snapshotRepo,
		workflowDefs:  workflowDefRepo,
		workflowRuns:  workflowRunRepo,
		nodeRuns:      nodeRunRepo,
		nodeSnapshots: nodeSnapshotRepo,
		executors:     make(map[string]WorkflowNodeExecutor),
		broker:        broker,
		auditSvc:      auditSvc,
		typeRegistry:  NewTypeRegistry(),
	}

	svc.RegisterExecutor(&triggerNodeExecutor{})
	svc.RegisterExecutor(newFolderTreeScannerExecutor(fsAdapter))
	svc.RegisterExecutor(newNameKeywordClassifierExecutor())
	svc.RegisterExecutor(newFileTreeClassifierExecutor())
	svc.RegisterExecutor(newConfidenceCheckExecutor())
	svc.RegisterExecutor(&extRatioClassifierNodeExecutor{fs: fsAdapter})
	svc.RegisterExecutor(newSignalAggregatorExecutor())
	svc.RegisterExecutor(newClassificationWriterExecutor(folderRepo, snapshotRepo))
	svc.RegisterExecutor(newClassificationReaderExecutor())
	svc.RegisterExecutor(newDBSubtreeReaderExecutor(folderRepo, fsAdapter))
	svc.RegisterExecutor(newFolderSplitterExecutor())
	svc.RegisterExecutor(newCategoryRouterExecutor())
	svc.RegisterExecutor(newMixedLeafRouterExecutor(fsAdapter))
	svc.RegisterExecutor(newCollectNodeExecutor())
	svc.RegisterExecutor(newRenameNodeExecutor())
	svc.RegisterExecutor(newPhase4MoveNodeExecutor(fsAdapter, folderRepo))
	svc.RegisterExecutor(newThumbnailNodeExecutor(fsAdapter, folderRepo))
	svc.RegisterExecutor(newCompressNodeExecutor(fsAdapter))
	svc.RegisterExecutor(newClassificationDBResultPreviewExecutor())
	svc.RegisterExecutor(newProcessingResultPreviewExecutor())
	svc.RegisterExecutor(newFolderPickerNodeExecutor(fsAdapter, folderRepo))

	return svc
}

func (s *WorkflowRunnerService) SetProcessingReviewRepository(repo repository.ProcessingReviewRepository) {
	s.reviews = repo
}

func (s *WorkflowRunnerService) SetConfigRepository(repo repository.ConfigRepository) {
	s.config = repo
}

func (s *WorkflowRunnerService) SetSourceManifestBuilder(builder SourceManifestBuilder) {
	s.manifestSvc = builder
	for _, executor := range s.executors {
		writer, ok := executor.(*classificationWriterNodeExecutor)
		if !ok {
			continue
		}
		writer.SetSourceManifestBuilder(builder)
	}
}

func (s *WorkflowRunnerService) SetOutputPipeline(mappingSvc OutputMappingBuilder, validatorSvc OutputValidator, completionSvc FolderCompletionUpdater) {
	s.mappingSvc = mappingSvc
	s.validatorSvc = validatorSvc
	s.completionSvc = completionSvc
}

func (s *WorkflowRunnerService) RegisterExecutor(executor WorkflowNodeExecutor) {
	if executor == nil {
		return
	}

	s.executors[executor.Type()] = executor
}

func (s *WorkflowRunnerService) ListNodeSchemas() []NodeSchema {
	schemas := make([]NodeSchema, 0, len(s.executors))
	for _, executor := range s.executors {
		schemas = append(schemas, executor.Schema())
	}

	sort.Slice(schemas, func(i, j int) bool {
		return schemas[i].TypeName() < schemas[j].TypeName()
	})

	return schemas
}

func (s *WorkflowRunnerService) StartJob(ctx context.Context, input StartWorkflowJobInput) (string, error) {
	if input.WorkflowDefID == "" {
		return "", fmt.Errorf("workflowRunner.StartJob: workflow_def_id is required")
	}
	def, err := s.workflowDefs.GetByID(ctx, input.WorkflowDefID)
	if err != nil {
		return "", fmt.Errorf("workflowRunner.StartJob get workflow def %q: %w", input.WorkflowDefID, err)
	}

	rootFolderID := ""
	if def != nil {
		graph, parseErr := parseWorkflowGraph(def.GraphJSON)
		if parseErr != nil {
			return "", fmt.Errorf("workflowRunner.StartJob parse graph for workflow def %q: %w", input.WorkflowDefID, parseErr)
		}
		rootFolderID = inferWorkflowRootFolderID(graph)
	}

	folderIDs := []string{}
	if rootFolderID != "" {
		folderIDs = append(folderIDs, rootFolderID)
	}
	folderIDsJSON, err := json.Marshal(folderIDs)
	if err != nil {
		return "", fmt.Errorf("workflowRunner.StartJob marshal folder_ids: %w", err)
	}

	sourceDir := strings.TrimSpace(input.SourceDir)

	jobID := uuid.NewString()
	if err := s.jobs.Create(ctx, &repository.Job{
		ID:            jobID,
		Type:          "workflow",
		WorkflowDefID: input.WorkflowDefID,
		SourceDir:     sourceDir,
		Status:        "pending",
		FolderIDs:     string(folderIDsJSON),
		Total:         1,
	}); err != nil {
		return "", fmt.Errorf("workflowRunner.StartJob create job: %w", err)
	}

	go s.runJob(context.Background(), jobID, input.WorkflowDefID, sourceDir, rootFolderID)
	return jobID, nil
}

func (s *WorkflowRunnerService) runJob(ctx context.Context, jobID, workflowDefID, sourceDir, rootFolderID string) {
	_ = s.jobs.UpdateStatus(ctx, jobID, "running", "")

	run := &repository.WorkflowRun{
		ID:            uuid.NewString(),
		JobID:         jobID,
		FolderID:      strings.TrimSpace(rootFolderID),
		SourceDir:     sourceDir,
		WorkflowDefID: workflowDefID,
		Status:        "pending",
	}
	if err := s.workflowRuns.Create(ctx, run); err != nil {
		_ = s.jobs.IncrementProgress(ctx, jobID, 0, 1)
		_ = s.jobs.UpdateStatus(ctx, jobID, "failed", fmt.Sprintf("创建工作流运行失败: %v", err))
		return
	}
	if err := s.executeWorkflowRun(ctx, run.ID, false); err != nil {
		_ = s.jobs.IncrementProgress(ctx, jobID, 0, 1)
		_ = s.jobs.UpdateStatus(ctx, jobID, "failed", err.Error())
		return
	}

	latestRun, err := s.workflowRuns.GetByID(ctx, run.ID)
	if err != nil {
		_ = s.jobs.IncrementProgress(ctx, jobID, 0, 1)
		_ = s.jobs.UpdateStatus(ctx, jobID, "failed", fmt.Sprintf("读取工作流运行结果失败: %v", err))
		return
	}

	switch latestRun.Status {
	case "waiting_input":
		_ = s.jobs.UpdateStatus(ctx, jobID, "waiting_input", "")
	case "succeeded", "partial", "rolled_back":
		_ = s.jobs.IncrementProgress(ctx, jobID, 1, 0)
		_ = s.jobs.UpdateStatus(ctx, jobID, latestRun.Status, "")
	default:
		_ = s.jobs.IncrementProgress(ctx, jobID, 0, 1)
		jobErr := strings.TrimSpace(latestRun.Error)
		if jobErr == "" {
			jobErr = "工作流运行失败"
		}
		_ = s.jobs.UpdateStatus(ctx, jobID, "failed", jobErr)
	}
}

func inferWorkflowRootFolderID(graph *repository.WorkflowGraph) string {
	if graph == nil {
		return ""
	}

	for _, node := range graph.Nodes {
		if !node.Enabled || strings.TrimSpace(node.Type) != folderPickerExecutorType {
			continue
		}

		folderID := strings.TrimSpace(folderPickerParseSavedFolderID(node.Config))
		if folderID != "" {
			return folderID
		}
	}

	return ""
}

func (s *WorkflowRunnerService) ListWorkflowRuns(ctx context.Context, jobID string, page, limit int) ([]*repository.WorkflowRun, int, error) {
	items, total, err := s.workflowRuns.List(ctx, repository.WorkflowRunListFilter{
		JobID: jobID,
		Page:  page,
		Limit: limit,
	})
	if err != nil {
		return nil, 0, fmt.Errorf("workflowRunner.ListWorkflowRuns: %w", err)
	}

	return items, total, nil
}

func (s *WorkflowRunnerService) GetWorkflowRunDetail(ctx context.Context, workflowRunID string) (*WorkflowRunDetail, error) {
	run, err := s.workflowRuns.GetByID(ctx, workflowRunID)
	if err != nil {
		return nil, fmt.Errorf("workflowRunner.GetWorkflowRunDetail get run %q: %w", workflowRunID, err)
	}
	nodeRuns, _, err := s.nodeRuns.List(ctx, repository.NodeRunListFilter{WorkflowRunID: workflowRunID, Page: 1, Limit: 1000})
	if err != nil {
		return nil, fmt.Errorf("workflowRunner.GetWorkflowRunDetail list node runs %q: %w", workflowRunID, err)
	}

	detail := &WorkflowRunDetail{Run: run, NodeRuns: nodeRuns}
	if s.reviews != nil {
		items, listErr := s.reviews.ListByWorkflowRunID(ctx, workflowRunID)
		if listErr != nil {
			return nil, fmt.Errorf("workflowRunner.GetWorkflowRunDetail list reviews %q: %w", workflowRunID, listErr)
		}
		detail.ReviewSummary = buildProcessingReviewSummary(items, nodeRuns)
	}

	return detail, nil
}

func (s *WorkflowRunnerService) ResumeWorkflowRun(ctx context.Context, workflowRunID string) error {
	if err := s.ResumeWorkflowRunWithData(ctx, workflowRunID, nil); err != nil {
		return fmt.Errorf("workflowRunner.ResumeWorkflowRun: %w", err)
	}

	return nil
}

func (s *WorkflowRunnerService) ResumeWorkflowRunWithData(ctx context.Context, workflowRunID string, resumeData map[string]any) error {
	if resumeData != nil {
		waitingNodeRun, err := s.nodeRuns.GetWaitingInputByWorkflowRunID(ctx, workflowRunID)
		if err != nil {
			return fmt.Errorf("workflowRunner.ResumeWorkflowRunWithData get waiting node run for %q: %w", workflowRunID, err)
		}

		persisted := make(map[string]any)
		if strings.TrimSpace(waitingNodeRun.ResumeData) != "" {
			if err := json.Unmarshal([]byte(waitingNodeRun.ResumeData), &persisted); err != nil {
				return fmt.Errorf("workflowRunner.ResumeWorkflowRunWithData unmarshal resume data for node run %q: %w", waitingNodeRun.ID, err)
			}
		}
		for key, value := range resumeData {
			persisted[key] = value
		}

		encoded, err := json.Marshal(persisted)
		if err != nil {
			return fmt.Errorf("workflowRunner.ResumeWorkflowRunWithData marshal resume data for node run %q: %w", waitingNodeRun.ID, err)
		}
		if err := s.nodeRuns.UpdateResumeData(ctx, waitingNodeRun.ID, string(encoded)); err != nil {
			return fmt.Errorf("workflowRunner.ResumeWorkflowRunWithData persist resume data for node run %q: %w", waitingNodeRun.ID, err)
		}
	}

	if err := s.executeWorkflowRun(ctx, workflowRunID, true); err != nil {
		return fmt.Errorf("workflowRunner.ResumeWorkflowRunWithData: %w", err)
	}

	run, err := s.workflowRuns.GetByID(ctx, workflowRunID)
	if err != nil {
		return fmt.Errorf("workflowRunner.ResumeWorkflowRunWithData get workflow run %q: %w", workflowRunID, err)
	}
	job, err := s.jobs.GetByID(ctx, run.JobID)
	if err != nil {
		return fmt.Errorf("workflowRunner.ResumeWorkflowRunWithData get job %q: %w", run.JobID, err)
	}

	if run.Status == "waiting_input" {
		if err := s.jobs.UpdateStatus(ctx, run.JobID, "waiting_input", ""); err != nil {
			return fmt.Errorf("workflowRunner.ResumeWorkflowRunWithData set job waiting_input %q: %w", run.JobID, err)
		}
		return nil
	}

	if strings.TrimSpace(job.Status) == "waiting_input" {
		if err := s.jobs.IncrementProgress(ctx, run.JobID, 1, 0); err != nil {
			return fmt.Errorf("workflowRunner.ResumeWorkflowRunWithData increment job progress %q: %w", run.JobID, err)
		}
	}
	jobErr := ""
	if run.Status == "failed" {
		jobErr = strings.TrimSpace(run.Error)
	}
	if err := s.jobs.UpdateStatus(ctx, run.JobID, run.Status, jobErr); err != nil {
		return fmt.Errorf("workflowRunner.ResumeWorkflowRunWithData update job %q status %q: %w", run.JobID, run.Status, err)
	}

	return nil
}

func (s *WorkflowRunnerService) RollbackWorkflowRun(ctx context.Context, workflowRunID string) error {
	run, err := s.workflowRuns.GetByID(ctx, workflowRunID)
	if err != nil {
		return fmt.Errorf("workflowRunner.RollbackWorkflowRun get workflow run %q: %w", workflowRunID, err)
	}

	nodeRuns, _, err := s.nodeRuns.List(ctx, repository.NodeRunListFilter{WorkflowRunID: workflowRunID, Page: 1, Limit: 2000})
	if err != nil {
		return fmt.Errorf("workflowRunner.RollbackWorkflowRun list node runs %q: %w", workflowRunID, err)
	}
	sort.Slice(nodeRuns, func(i, j int) bool {
		return nodeRuns[i].Sequence > nodeRuns[j].Sequence
	})

	var folder *repository.Folder
	if strings.TrimSpace(run.FolderID) != "" {
		folder, err = s.folders.GetByID(ctx, run.FolderID)
		if err != nil {
			return fmt.Errorf("workflowRunner.RollbackWorkflowRun get folder %q: %w", run.FolderID, err)
		}
	}

	for _, nodeRun := range nodeRuns {
		if nodeRun.Status != "succeeded" {
			continue
		}
		executor, ok := s.executors[nodeRun.NodeType]
		if !ok {
			continue
		}
		snaps, snapErr := s.nodeSnapshots.ListByNodeRunID(ctx, nodeRun.ID)
		if snapErr != nil {
			return fmt.Errorf("workflowRunner.RollbackWorkflowRun list snapshots for node run %q: %w", nodeRun.ID, snapErr)
		}
		if rbErr := executor.Rollback(ctx, NodeRollbackInput{
			WorkflowRun: run,
			NodeRun:     nodeRun,
			Snapshots:   snaps,
			Folder:      folder,
		}); rbErr != nil {
			return fmt.Errorf("workflowRunner.RollbackWorkflowRun rollback node %q: %w", nodeRun.NodeID, rbErr)
		}
		_ = s.writeNodeRollbackAudit(ctx, run, nodeRun, folder, snaps)

		if strings.TrimSpace(run.FolderID) != "" {
			folder, err = s.folders.GetByID(ctx, run.FolderID)
			if err != nil {
				return fmt.Errorf("workflowRunner.RollbackWorkflowRun reload folder %q: %w", run.FolderID, err)
			}
		}
	}

	if err := s.workflowRuns.UpdateStatus(ctx, workflowRunID, "rolled_back", ""); err != nil {
		return fmt.Errorf("workflowRunner.RollbackWorkflowRun update workflow run status %q: %w", workflowRunID, err)
	}
	if err := s.syncFolderStatusesByWorkflowRun(ctx, workflowRunID, "rolled_back"); err != nil {
		return fmt.Errorf("workflowRunner.RollbackWorkflowRun sync folder statuses for %q: %w", workflowRunID, err)
	}
	s.publishWorkflowRunUpdated(ctx, workflowRunID)

	return nil
}

func (s *WorkflowRunnerService) executeWorkflowRun(ctx context.Context, workflowRunID string, resume bool) (retErr error) {
	run, err := s.workflowRuns.GetByID(ctx, workflowRunID)
	if err != nil {
		return fmt.Errorf("workflowRunner.executeWorkflowRun get workflow run %q: %w", workflowRunID, err)
	}

	if err := s.workflowRuns.UpdateStatus(ctx, run.ID, "running", run.ResumeNodeID); err != nil {
		return fmt.Errorf("workflowRunner.executeWorkflowRun set running for %q: %w", run.ID, err)
	}
	s.publishWorkflowRunUpdated(ctx, run.ID)

	def, err := s.workflowDefs.GetByID(ctx, run.WorkflowDefID)
	if err != nil {
		return fmt.Errorf("workflowRunner.executeWorkflowRun get workflow def %q: %w", run.WorkflowDefID, err)
	}

	graph, err := parseWorkflowGraph(def.GraphJSON)
	if err != nil {
		return fmt.Errorf("workflowRunner.executeWorkflowRun parse graph for workflow def %q: %w", def.ID, err)
	}
	s.normalizeGraphPortReferences(graph)

	levels, err := topologicalLevels(graph)
	if err != nil {
		return fmt.Errorf("workflowRunner.executeWorkflowRun topo levels for workflow def %q: %w", def.ID, err)
	}

	var folder *repository.Folder
	if strings.TrimSpace(run.FolderID) != "" {
		folder, err = s.folders.GetByID(ctx, run.FolderID)
		if err != nil {
			return fmt.Errorf("workflowRunner.executeWorkflowRun get folder %q: %w", run.FolderID, err)
		}
	}

	runStartedAt := time.Now()
	appConfig := s.getRuntimeAppConfig(ctx)
	workflowFailureReason := ""
	workflowFailureNodeID := ""
	if !resume {
		_ = s.writeWorkflowRunAudit(ctx, run, folder, "workflow.run.start", "success", 0, nil)
	}
	defer func() {
		if retErr != nil {
			reason := strings.TrimSpace(workflowFailureReason)
			if reason == "" {
				reason = strings.TrimSpace(retErr.Error())
			}
			if reason == "" {
				reason = "workflow run failed"
			}
			run.Error = reason
			run.LastNodeID = strings.TrimSpace(workflowFailureNodeID)
			_ = s.markWorkflowRunFailed(ctx, run.ID, workflowFailureNodeID, reason)
			_ = s.syncFolderStatusesByWorkflowRun(ctx, run.ID, "failed")
			_ = s.writeWorkflowRunAudit(ctx, run, folder, "workflow.run.failed", "failed", time.Since(runStartedAt).Milliseconds(), fmt.Errorf("%s", reason))
		}
	}()

	existingRuns, _, err := s.nodeRuns.List(ctx, repository.NodeRunListFilter{WorkflowRunID: run.ID, Page: 1, Limit: 2000})
	if err != nil {
		return fmt.Errorf("workflowRunner.executeWorkflowRun list node runs for workflow run %q: %w", run.ID, err)
	}
	outputCache := make(map[string]map[string]TypedValue)
	nodeStatusCache := make(map[string]string)
	var outputMu sync.RWMutex
	for _, existingRun := range existingRuns {
		if existingRun.NodeID != "" {
			nodeStatusCache[existingRun.NodeID] = strings.TrimSpace(existingRun.Status)
		}
		if existingRun.NodeID == "" || strings.TrimSpace(existingRun.OutputJSON) == "" {
			continue
		}
		schema := s.schemaForNode(existingRun.NodeType)
		outputs, parseErr := s.parseNodeOutputsForSchema(existingRun.OutputJSON, schema)
		if parseErr != nil {
			continue
		}
		outputCache[existingRun.NodeID] = outputs
	}
	seq := len(existingRuns)
	var seqMu sync.Mutex

	resumeNodeID := ""
	if resume {
		resumeNodeID = run.ResumeNodeID
	}
	startNow := resumeNodeID == ""

	var resumeData map[string]any
	if resume {
		waitingNodeRun, waitErr := s.nodeRuns.GetWaitingInputByWorkflowRunID(ctx, workflowRunID)
		if waitErr == nil && strings.TrimSpace(waitingNodeRun.ResumeData) != "" {
			if err := json.Unmarshal([]byte(waitingNodeRun.ResumeData), &resumeData); err != nil {
				return fmt.Errorf("workflowRunner.executeWorkflowRun unmarshal resume data for node run %q: %w", waitingNodeRun.ID, err)
			}
		}
	}

	for _, level := range levels {
		levelNodes := make([]repository.WorkflowGraphNode, 0, len(level))
		for _, node := range level {
			if !startNow {
				if node.ID == resumeNodeID {
					startNow = true
				} else {
					continue
				}
			}
			levelNodes = append(levelNodes, node)
		}
		if len(levelNodes) == 0 {
			continue
		}

		levelCtx, cancelLevel := context.WithCancel(ctx)
		var wg sync.WaitGroup
		resultCh := make(chan nodeExecutionResult, len(levelNodes))

		for _, node := range levelNodes {
			node := node
			wg.Add(1)
			go func() {
				defer wg.Done()
				resultCh <- s.executeWorkflowNode(levelCtx, run, folder, appConfig, node, resume && node.ID == resumeNodeID, resumeData, &seq, &seqMu, outputCache, nodeStatusCache, &outputMu)
			}()
		}

		wg.Wait()
		cancelLevel()
		close(resultCh)

		var firstErr error
		for result := range resultCh {
			if result.Pending {
				return nil
			}
			if firstErr == nil && result.Err != nil {
				firstErr = result.Err
				workflowFailureReason = strings.TrimSpace(result.Reason)
				workflowFailureNodeID = strings.TrimSpace(result.NodeID)
			}
		}
		if firstErr != nil {
			return firstErr
		}
	}

	reviewEntered, reviewErr := s.prepareProcessingReviews(ctx, run.ID, folder)
	if reviewErr != nil {
		return fmt.Errorf("workflowRunner.executeWorkflowRun prepare processing reviews for %q: %w", run.ID, reviewErr)
	}
	if reviewEntered {
		_ = s.writeWorkflowRunAudit(ctx, run, folder, "workflow.processing.review_pending", "success", time.Since(runStartedAt).Milliseconds(), nil)
		return nil
	}

	if err := s.syncFolderStatusesByWorkflowRun(ctx, run.ID, "succeeded"); err != nil {
		return fmt.Errorf("workflowRunner.executeWorkflowRun sync folder statuses for %q: %w", run.ID, err)
	}
	if err := s.workflowRuns.UpdateStatus(ctx, run.ID, "succeeded", ""); err != nil {
		return fmt.Errorf("workflowRunner.executeWorkflowRun set succeeded for %q: %w", run.ID, err)
	}
	s.publishWorkflowRunUpdated(ctx, run.ID)

	_ = s.writeWorkflowRunAudit(ctx, run, folder, "workflow.run.complete", "success", time.Since(runStartedAt).Milliseconds(), nil)
	return nil
}

type nodeExecutionResult struct {
	Pending bool
	Err     error
	NodeID  string
	Reason  string
}

func (s *WorkflowRunnerService) executeWorkflowNode(
	ctx context.Context,
	run *repository.WorkflowRun,
	folder *repository.Folder,
	appConfig *repository.AppConfig,
	node repository.WorkflowGraphNode,
	resume bool,
	resumeData map[string]any,
	seq *int,
	seqMu *sync.Mutex,
	outputCache map[string]map[string]TypedValue,
	nodeStatusCache map[string]string,
	outputMu *sync.RWMutex,
) nodeExecutionResult {
	outputMu.RLock()
	inputs, inputSources := s.resolveNodeInputs(node, outputCache, nodeStatusCache, run.SourceDir)
	outputMu.RUnlock()

	schema := s.schemaForNode(node.Type)
	skipNode, failNode, errCode, errMsg, skipReason := classifyNodeInputs(node, inputs, inputSources, schema)

	seqMu.Lock()
	*seq++
	currentSeq := *seq
	seqMu.Unlock()

	nodeRun := &repository.NodeRun{
		ID:            uuid.NewString(),
		WorkflowRunID: run.ID,
		NodeID:        node.ID,
		NodeType:      node.Type,
		Sequence:      currentSeq,
		Status:        "pending",
	}
	if err := s.nodeRuns.Create(ctx, nodeRun); err != nil {
		reason := fmt.Sprintf("create node run for node %q: %v", node.ID, err)
		_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, reason)
		return nodeExecutionResult{Err: fmt.Errorf("workflowRunner.executeWorkflowRun %s", reason), NodeID: node.ID, Reason: reason}
	}

	if skipNode {
		skipMessage := ""
		if strings.TrimSpace(skipReason) != "" {
			skipMessage = "skip_reason=" + strings.TrimSpace(skipReason)
		}
		if err := s.nodeRuns.UpdateFinish(ctx, nodeRun.ID, "skipped", "{}", skipMessage); err != nil {
			reason := fmt.Sprintf("finish skipped node run %q: %v", node.ID, err)
			_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, reason)
			return nodeExecutionResult{Err: fmt.Errorf("workflowRunner.executeWorkflowRun %s", reason), NodeID: node.ID, Reason: reason}
		}
		outputMu.Lock()
		nodeStatusCache[node.ID] = "skipped"
		outputMu.Unlock()
		return nodeExecutionResult{}
	}
	if failNode {
		failureMessage := withErrorCode(errCode, errMsg)
		_ = s.nodeRuns.UpdateFinish(ctx, nodeRun.ID, "failed", "", failureMessage)
		_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, failureMessage)
		_ = s.writeNodeExecutionAudit(
			ctx,
			NodeExecutionInput{WorkflowRun: run, NodeRun: nodeRun, Node: node, Folder: folder, SourceDir: run.SourceDir, Inputs: inputs},
			folder,
			nil,
			fmt.Errorf("%s", failureMessage),
			time.Now(),
			errCode,
		)
		s.publish("workflow_run.node_failed", map[string]any{
			"job_id":          run.JobID,
			"workflow_run_id": run.ID,
			"folder_id":       run.FolderID,
			"node_run_id":     nodeRun.ID,
			"node_id":         node.ID,
			"node_type":       node.Type,
			"error":           failureMessage,
			"error_code":      errCode,
		})
		s.publishFolderClassificationUpdated(ctx, run, "failed", node.ID, node.Type, failureMessage)
		return nodeExecutionResult{
			Err:    fmt.Errorf("workflowRunner.executeWorkflowRun input validation node %q: %s", node.ID, failureMessage),
			NodeID: node.ID,
			Reason: failureMessage,
		}
	}

	if s.manifestSvc != nil {
		if _, processingNode := processingStatusDrivenNodeTypes[strings.TrimSpace(node.Type)]; processingNode {
			inputItems, _ := categoryRouterExtractItems(inputs)
			inputItems = workflowRunSourceManifestItems(run, inputItems)
			if err := s.manifestSvc.EnsureForWorkflowRun(ctx, run.ID, inputItems); err != nil {
				reason := withErrorCode("manifest_snapshot_missing", fmt.Sprintf("freeze source manifest for node %q: %v", node.ID, err))
				_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, reason)
				return nodeExecutionResult{
					Err:    fmt.Errorf("workflowRunner.executeWorkflowRun %s", reason),
					NodeID: node.ID,
					Reason: reason,
				}
			}
		}
	}

	inputPayload := map[string]any{
		"workflow_run_id": run.ID,
		"source_dir":      run.SourceDir,
		"node":            node,
		"inputs":          typedInputValuesForJSON(inputs),
		"app_config":      appConfig,
	}
	if folder != nil {
		inputPayload["folder_id"] = folder.ID
		inputPayload["folder_path"] = folder.Path
	}
	inputJSON, err := json.Marshal(inputPayload)
	if err != nil {
		reason := fmt.Sprintf("marshal node input for node %q: %v", node.ID, err)
		_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, reason)
		return nodeExecutionResult{Err: fmt.Errorf("workflowRunner.executeWorkflowRun %s", reason), NodeID: node.ID, Reason: reason}
	}

	if err := s.nodeRuns.UpdateStart(ctx, nodeRun.ID, string(inputJSON)); err != nil {
		reason := fmt.Sprintf("update start for node run %q: %v", nodeRun.ID, err)
		_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, reason)
		return nodeExecutionResult{Err: fmt.Errorf("workflowRunner.executeWorkflowRun %s", reason), NodeID: node.ID, Reason: reason}
	}

	if err := s.createNodeSnapshot(ctx, run, nodeRun, "pre", folder, nil); err != nil {
		reason := fmt.Sprintf("create pre snapshot for node %q: %v", node.ID, err)
		_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, reason)
		return nodeExecutionResult{Err: fmt.Errorf("workflowRunner.executeWorkflowRun %s", reason), NodeID: node.ID, Reason: reason}
	}

	s.publish("workflow_run.node_started", map[string]any{
		"job_id":          run.JobID,
		"workflow_run_id": run.ID,
		"folder_id":       run.FolderID,
		"node_run_id":     nodeRun.ID,
		"node_id":         node.ID,
		"node_type":       node.Type,
		"sequence":        nodeRun.Sequence,
	})
	s.publishFolderClassificationUpdated(ctx, run, "classifying", node.ID, node.Type, "")

	executor, ok := s.executors[node.Type]
	if !ok {
		err := fmt.Errorf("workflowRunner.executeWorkflowRun: executor not found for node type %q", node.Type)
		_ = s.nodeRuns.UpdateFinish(ctx, nodeRun.ID, "failed", "", err.Error())
		_ = s.createNodeSnapshot(ctx, run, nodeRun, "post", folder, map[string]TypedValue{"error": {Type: PortTypeString, Value: err.Error()}})
		_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, err.Error())
		_ = s.writeNodeExecutionAudit(
			ctx,
			NodeExecutionInput{WorkflowRun: run, NodeRun: nodeRun, Node: node, Folder: folder, SourceDir: run.SourceDir, Inputs: inputs},
			folder,
			nil,
			err,
			time.Now(),
			"",
		)
		s.publish("workflow_run.node_failed", map[string]any{
			"job_id":          run.JobID,
			"workflow_run_id": run.ID,
			"folder_id":       run.FolderID,
			"node_run_id":     nodeRun.ID,
			"node_id":         node.ID,
			"node_type":       node.Type,
			"error":           err.Error(),
		})
		s.publishFolderClassificationUpdated(ctx, run, "failed", node.ID, node.Type, err.Error())
		return nodeExecutionResult{Err: err, NodeID: node.ID, Reason: err.Error()}
	}

	execInput := NodeExecutionInput{
		WorkflowRun: run,
		NodeRun:     nodeRun,
		Node:        node,
		Folder:      folder,
		SourceDir:   run.SourceDir,
		AppConfig:   appConfig,
		Inputs:      inputs,
		ProgressFn: func(percent int, msg string) {
			s.publish("workflow_run.node_progress", map[string]any{
				"job_id":          run.JobID,
				"workflow_run_id": run.ID,
				"folder_id":       run.FolderID,
				"node_run_id":     nodeRun.ID,
				"node_id":         node.ID,
				"node_type":       node.Type,
				"percent":         percent,
				"message":         msg,
			})
		},
	}

	nodeStartedAt := time.Now()
	var execOutput NodeExecutionOutput
	var execErr error
	if resume {
		execOutput, execErr = executor.Resume(ctx, execInput, resumeData)
	} else {
		execOutput, execErr = executor.Execute(ctx, execInput)
	}
	if execErr != nil {
		_ = s.nodeRuns.UpdateFinish(ctx, nodeRun.ID, "failed", "", execErr.Error())
		_ = s.createNodeSnapshot(ctx, run, nodeRun, "post", folder, map[string]TypedValue{"error": {Type: PortTypeString, Value: execErr.Error()}})
		_ = s.writeNodeExecutionAudit(ctx, execInput, folder, nil, execErr, nodeStartedAt, "")
		_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, execErr.Error())
		s.publish("workflow_run.node_failed", map[string]any{
			"job_id":          run.JobID,
			"workflow_run_id": run.ID,
			"folder_id":       run.FolderID,
			"node_run_id":     nodeRun.ID,
			"node_id":         node.ID,
			"node_type":       node.Type,
			"error":           execErr.Error(),
		})
		s.publishFolderClassificationUpdated(ctx, run, "failed", node.ID, node.Type, execErr.Error())
		return nodeExecutionResult{
			Err:    fmt.Errorf("workflowRunner.executeWorkflowRun execute node %q: %w", node.ID, execErr),
			NodeID: node.ID,
			Reason: execErr.Error(),
		}
	}

	if execOutput.Status == "" {
		execOutput.Status = ExecutionSuccess
	}
	if execOutput.Outputs == nil {
		execOutput.Outputs = map[string]TypedValue{}
	}

	if execOutput.Status == ExecutionSuccess {
		if code, message := validateNodeOutputs(execOutput.Outputs, schema); code != "" {
			finalMsg := withErrorCode(code, message)
			_ = s.nodeRuns.UpdateFinish(ctx, nodeRun.ID, "failed", "", finalMsg)
			_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, finalMsg)
			_ = s.writeNodeExecutionAudit(ctx, execInput, folder, execOutput.Outputs, fmt.Errorf("%s", finalMsg), nodeStartedAt, code)
			s.publish("workflow_run.node_failed", map[string]any{
				"job_id":          run.JobID,
				"workflow_run_id": run.ID,
				"folder_id":       run.FolderID,
				"node_run_id":     nodeRun.ID,
				"node_id":         node.ID,
				"node_type":       node.Type,
				"error":           finalMsg,
				"error_code":      code,
			})
			s.publishFolderClassificationUpdated(ctx, run, "failed", node.ID, node.Type, finalMsg)
			return nodeExecutionResult{
				Err:    fmt.Errorf("workflowRunner.executeWorkflowRun output validation node %q: %s", node.ID, finalMsg),
				NodeID: node.ID,
				Reason: finalMsg,
			}
		}
	}

	outputJSON, marshalErr := s.marshalTypedValuesJSON(execOutput.Outputs)
	if marshalErr != nil {
		reason := fmt.Sprintf("marshal node output for node %q: %v", node.ID, marshalErr)
		_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, reason)
		return nodeExecutionResult{Err: fmt.Errorf("workflowRunner.executeWorkflowRun %s", reason), NodeID: node.ID, Reason: reason}
	}

	if execOutput.Status == ExecutionPending {
		errMsg := execOutput.PendingReason
		persistedResumeData := make(map[string]any)
		for key, value := range resumeData {
			persistedResumeData[key] = value
		}
		for key, value := range execOutput.PendingState {
			persistedResumeData[key] = value
		}

		if len(persistedResumeData) > 0 {
			encodedResumeData, encodeErr := json.Marshal(persistedResumeData)
			if encodeErr != nil {
				reason := fmt.Sprintf("marshal resume data for node %q: %v", node.ID, encodeErr)
				_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, reason)
				return nodeExecutionResult{Err: fmt.Errorf("workflowRunner.executeWorkflowRun %s", reason), NodeID: node.ID, Reason: reason}
			}
			if err := s.nodeRuns.UpdateResumeData(ctx, nodeRun.ID, string(encodedResumeData)); err != nil {
				reason := fmt.Sprintf("persist resume data for node run %q: %v", nodeRun.ID, err)
				_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, reason)
				return nodeExecutionResult{Err: fmt.Errorf("workflowRunner.executeWorkflowRun %s", reason), NodeID: node.ID, Reason: reason}
			}
		}
		if err := s.nodeRuns.UpdateFinish(ctx, nodeRun.ID, "waiting_input", string(outputJSON), errMsg); err != nil {
			reason := fmt.Sprintf("update waiting_input for node run %q: %v", nodeRun.ID, err)
			_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, reason)
			return nodeExecutionResult{Err: fmt.Errorf("workflowRunner.executeWorkflowRun %s", reason), NodeID: node.ID, Reason: reason}
		}

		if err := s.workflowRuns.UpdateStatus(ctx, run.ID, "waiting_input", node.ID); err != nil {
			return nodeExecutionResult{Err: fmt.Errorf("workflowRunner.executeWorkflowRun set waiting_input for %q: %w", run.ID, err)}
		}
		s.publishWorkflowRunUpdated(ctx, run.ID)

		s.publish("workflow_run.node_pending", map[string]any{
			"job_id":          run.JobID,
			"workflow_run_id": run.ID,
			"folder_id":       run.FolderID,
			"node_run_id":     nodeRun.ID,
			"node_id":         node.ID,
			"node_type":       node.Type,
			"error":           execOutput.PendingReason,
		})
		s.publishFolderClassificationUpdated(ctx, run, "waiting_input", node.ID, node.Type, execOutput.PendingReason)

		return nodeExecutionResult{Pending: true}
	}

	if execOutput.Status == ExecutionFailure {
		errMsg := execOutput.PendingReason
		if strings.TrimSpace(errMsg) == "" {
			errMsg = "node returned failure status"
		}
		errMsg = withErrorCode(execOutput.ErrorCode, errMsg)
		_ = s.nodeRuns.UpdateFinish(ctx, nodeRun.ID, "failed", string(outputJSON), errMsg)
		_ = s.createNodeSnapshot(ctx, run, nodeRun, "post", folder, map[string]TypedValue{"error": {Type: PortTypeString, Value: errMsg}})
		_ = s.writeNodeExecutionAudit(ctx, execInput, folder, execOutput.Outputs, fmt.Errorf("%s", errMsg), nodeStartedAt, execOutput.ErrorCode)
		_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, errMsg)
		s.publish("workflow_run.node_failed", map[string]any{
			"job_id":          run.JobID,
			"workflow_run_id": run.ID,
			"folder_id":       run.FolderID,
			"node_run_id":     nodeRun.ID,
			"node_id":         node.ID,
			"node_type":       node.Type,
			"error":           errMsg,
			"error_code":      execOutput.ErrorCode,
		})
		s.publishFolderClassificationUpdated(ctx, run, "failed", node.ID, node.Type, errMsg)
		return nodeExecutionResult{
			Err:    fmt.Errorf("workflowRunner.executeWorkflowRun execute node %q: %s", node.ID, errMsg),
			NodeID: node.ID,
			Reason: errMsg,
		}
	}

	if err := s.nodeRuns.UpdateFinish(ctx, nodeRun.ID, "succeeded", string(outputJSON), ""); err != nil {
		reason := fmt.Sprintf("update finish for node run %q: %v", nodeRun.ID, err)
		_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, reason)
		return nodeExecutionResult{Err: fmt.Errorf("workflowRunner.executeWorkflowRun %s", reason), NodeID: node.ID, Reason: reason}
	}

	outputMu.Lock()
	outputCache[node.ID] = cloneTypedValueMap(execOutput.Outputs)
	nodeStatusCache[node.ID] = "succeeded"
	outputMu.Unlock()

	if err := s.createNodeSnapshot(ctx, run, nodeRun, "post", folder, execOutput.Outputs); err != nil {
		reason := fmt.Sprintf("create post snapshot for node %q: %v", node.ID, err)
		_ = s.markWorkflowRunFailed(ctx, run.ID, node.ID, reason)
		return nodeExecutionResult{Err: fmt.Errorf("workflowRunner.executeWorkflowRun %s", reason), NodeID: node.ID, Reason: reason}
	}
	_ = s.writeNodeExecutionAudit(ctx, execInput, folder, execOutput.Outputs, nil, nodeStartedAt, "")

	s.publish("workflow_run.node_done", map[string]any{
		"job_id":          run.JobID,
		"workflow_run_id": run.ID,
		"folder_id":       run.FolderID,
		"node_run_id":     nodeRun.ID,
		"node_id":         node.ID,
		"node_type":       node.Type,
		"sequence":        nodeRun.Sequence,
	})
	s.publishFolderClassificationUpdated(ctx, run, "classifying", node.ID, node.Type, "")

	return nodeExecutionResult{}
}

func workflowRunSourceManifestItems(run *repository.WorkflowRun, items []ProcessingItem) []ProcessingItem {
	if run == nil || strings.TrimSpace(run.FolderID) == "" || len(items) == 0 {
		return items
	}

	fallbackFolderID := strings.TrimSpace(run.FolderID)
	out := make([]ProcessingItem, len(items))
	changed := false
	for index, item := range items {
		out[index] = item
		if strings.TrimSpace(out[index].FolderID) != "" {
			continue
		}
		out[index].FolderID = fallbackFolderID
		changed = true
	}
	if !changed {
		return items
	}
	return out
}

func (s *WorkflowRunnerService) getRuntimeAppConfig(ctx context.Context) *repository.AppConfig {
	if s == nil || s.config == nil {
		return nil
	}
	cfg, err := s.config.GetAppConfig(ctx)
	if err != nil {
		return nil
	}
	return cfg
}

func withErrorCode(errorCode, message string) string {
	if strings.TrimSpace(errorCode) == "" {
		return message
	}
	if strings.TrimSpace(message) == "" {
		return errorCode
	}
	return fmt.Sprintf("%s: %s", errorCode, message)
}

func (s *WorkflowRunnerService) markWorkflowRunFailed(ctx context.Context, runID, nodeID, reason string) error {
	finalReason := strings.TrimSpace(reason)
	if finalReason == "" {
		finalReason = "workflow run failed"
	}
	if err := s.workflowRuns.UpdateFailure(ctx, runID, nodeID, finalReason); err != nil {
		return fmt.Errorf("workflowRunner.markWorkflowRunFailed update workflow run %q: %w", runID, err)
	}
	s.publishWorkflowRunUpdated(ctx, runID)
	return nil
}

func summarizeTypedValues(values map[string]*TypedValue) map[string]any {
	if len(values) == 0 {
		return map[string]any{
			"keys":  []string{},
			"count": 0,
		}
	}

	keys := make([]string, 0, len(values))
	types := make(map[string]string, len(values))
	for key, value := range values {
		keys = append(keys, key)
		if value != nil {
			types[key] = string(value.Type)
		} else {
			types[key] = ""
		}
	}
	sort.Strings(keys)

	return map[string]any{
		"keys":  keys,
		"count": len(keys),
		"types": types,
	}
}

func summarizeTypedValuesMap(values map[string]TypedValue) map[string]any {
	if len(values) == 0 {
		return map[string]any{
			"keys":  []string{},
			"count": 0,
		}
	}

	keys := make([]string, 0, len(values))
	types := make(map[string]string, len(values))
	for key, value := range values {
		keys = append(keys, key)
		types[key] = string(value.Type)
	}
	sort.Strings(keys)

	return map[string]any{
		"keys":  keys,
		"count": len(keys),
		"types": types,
	}
}

func (s *WorkflowRunnerService) writeNodeExecutionAudit(
	ctx context.Context,
	input NodeExecutionInput,
	currentFolder *repository.Folder,
	outputs map[string]TypedValue,
	execErr error,
	startedAt time.Time,
	errorCode string,
) error {
	if s.auditSvc == nil {
		return nil
	}

	detail := map[string]any{
		"workflow_run_id": workflowRunIDFromInput(input.WorkflowRun),
		"node_run_id":     nodeRunIDFromInput(input.NodeRun),
		"node_id":         input.Node.ID,
		"node_type":       input.Node.Type,
		"input_summary":   summarizeTypedValues(input.Inputs),
		"output_summary":  summarizeTypedValuesMap(outputs),
	}
	action := "workflow." + input.Node.Type
	folderPath := folderPathForAudit(input.Folder, currentFolder)
	result := "success"

	switch input.Node.Type {
	case "move":
		detail["source_path"] = folderPathForAudit(input.Folder, nil)
		detail["target_path"] = folderPathForAudit(currentFolder, nil)
	case subtreeAggregatorExecutorType:
		entries := classifiedEntriesForAudit(outputs)
		detail["entry_count"] = len(entries)
		if len(entries) > 0 {
			if folderPath == "" {
				folderPath = strings.TrimSpace(entries[0].Path)
			}
			if input.Folder == nil && currentFolder == nil && strings.TrimSpace(entries[0].FolderID) != "" {
				detail["entry_folder_id"] = strings.TrimSpace(entries[0].FolderID)
			}
		}
	case phase4MoveNodeExecutorType:
		results := moveResultsForAudit(outputs)
		detail["results"] = results
		if len(results) > 0 {
			folderPath = strings.TrimSpace(results[0].TargetPath)
			if folderPath == "" {
				folderPath = strings.TrimSpace(results[0].SourcePath)
			}
			if strings.TrimSpace(results[0].Status) != "" {
				result = strings.TrimSpace(results[0].Status)
			}
		}
	case compressNodeExecutorType:
		archives := stepResultTargetsForNode(outputs, compressNodeExecutorType)
		detail["archive_paths"] = archives
		if len(archives) > 0 {
			folderPath = archives[0]
		}
	case thumbnailNodeExecutorType:
		thumbnails := stepResultTargetsForNode(outputs, thumbnailNodeExecutorType)
		detail["thumbnail_paths"] = thumbnails
		if len(thumbnails) > 0 {
			folderPath = thumbnails[0]
		}
	}

	if execErr != nil {
		result = "failed"
		detail["error"] = execErr.Error()
	}
	if strings.TrimSpace(errorCode) != "" {
		detail["error_code"] = strings.TrimSpace(errorCode)
	}

	detailJSON, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("workflowRunner.writeNodeExecutionAudit marshal detail: %w", err)
	}

	inferredFolderID, inferredFolderPath := inferFolderRefFromAuditOutputs(input.Inputs, outputs)
	if strings.TrimSpace(inferredFolderID) == "" {
		inferredFolderID = folderIDForAudit(input.Folder, currentFolder)
	}
	if strings.TrimSpace(inferredFolderPath) != "" && strings.TrimSpace(folderPath) == "" {
		folderPath = inferredFolderPath
	}

	auditLog := &repository.AuditLog{
		JobID:         workflowJobIDFromInput(input.WorkflowRun),
		WorkflowRunID: workflowRunIDFromInput(input.WorkflowRun),
		NodeRunID:     nodeRunIDFromInput(input.NodeRun),
		NodeID:        strings.TrimSpace(input.Node.ID),
		NodeType:      strings.TrimSpace(input.Node.Type),
		FolderID:      inferredFolderID,
		FolderPath:    folderPath,
		Action:        action,
		Result:        result,
		Detail:        detailJSON,
		DurationMs:    time.Since(startedAt).Milliseconds(),
		ErrorMsg:      errorString(execErr),
	}
	if input.Node.Type == subtreeAggregatorExecutorType && strings.TrimSpace(auditLog.FolderID) == "" {
		entries := classifiedEntriesForAudit(outputs)
		if len(entries) > 0 {
			auditLog.FolderID = strings.TrimSpace(entries[0].FolderID)
			if strings.TrimSpace(auditLog.FolderPath) == "" {
				auditLog.FolderPath = strings.TrimSpace(entries[0].Path)
			}
		}
	}
	err = s.auditSvc.Write(ctx, auditLog)

	return err
}

func (s *WorkflowRunnerService) writeNodeRollbackAudit(ctx context.Context, run *repository.WorkflowRun, nodeRun *repository.NodeRun, folder *repository.Folder, snapshots []*repository.NodeSnapshot) error {
	if s.auditSvc == nil || nodeRun == nil {
		return nil
	}

	folderID := folderIDForAudit(folder, nil)
	folderPath := folderPathForAudit(folder, nil)

	detailJSON, err := json.Marshal(map[string]any{
		"workflow_run_id": workflowRunIDFromInput(run),
		"node_run_id":     nodeRun.ID,
		"node_id":         nodeRun.NodeID,
		"node_type":       nodeRun.NodeType,
		"snapshot_count":  len(snapshots),
	})
	if err != nil {
		return fmt.Errorf("workflowRunner.writeNodeRollbackAudit marshal detail: %w", err)
	}

	return s.auditSvc.Write(ctx, &repository.AuditLog{
		JobID:         workflowJobIDFromInput(run),
		WorkflowRunID: workflowRunIDFromInput(run),
		NodeRunID:     nodeRun.ID,
		NodeID:        strings.TrimSpace(nodeRun.NodeID),
		NodeType:      strings.TrimSpace(nodeRun.NodeType),
		FolderID:      folderID,
		FolderPath:    folderPath,
		Action:        "workflow." + nodeRun.NodeType + ".rollback",
		Result:        "success",
		Detail:        detailJSON,
	})
}

func (s *WorkflowRunnerService) writeWorkflowRunAudit(ctx context.Context, run *repository.WorkflowRun, folder *repository.Folder, action, result string, durationMs int64, auditErr error) error {
	if s.auditSvc == nil {
		return nil
	}

	detail := map[string]any{
		"workflow_run_id": workflowRunIDFromInput(run),
		"workflow_def_id": run.WorkflowDefID,
		"last_node_id":    strings.TrimSpace(run.LastNodeID),
	}
	if auditErr != nil {
		detail["error"] = auditErr.Error()
	}

	detailJSON, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("workflowRunner.writeWorkflowRunAudit marshal detail: %w", err)
	}

	return s.auditSvc.Write(ctx, &repository.AuditLog{
		JobID:         workflowJobIDFromInput(run),
		WorkflowRunID: workflowRunIDFromInput(run),
		FolderID:      folderIDForAudit(folder, nil),
		FolderPath:    folderPathForAudit(folder, nil),
		Action:        action,
		Result:        result,
		Detail:        detailJSON,
		DurationMs:    durationMs,
		ErrorMsg:      errorString(auditErr),
	})
}

func nodeRunIDFromInput(run *repository.NodeRun) string {
	if run == nil {
		return ""
	}

	return run.ID
}

func workflowRunIDFromInput(run *repository.WorkflowRun) string {
	if run == nil {
		return ""
	}

	return run.ID
}

func workflowJobIDFromInput(run *repository.WorkflowRun) string {
	if run == nil {
		return ""
	}

	return run.JobID
}

func folderIDForAudit(primary *repository.Folder, fallback *repository.Folder) string {
	if primary != nil && strings.TrimSpace(primary.ID) != "" {
		return strings.TrimSpace(primary.ID)
	}
	if fallback != nil {
		return strings.TrimSpace(fallback.ID)
	}

	return ""
}

func folderPathForAudit(primary *repository.Folder, fallback *repository.Folder) string {
	if primary != nil && strings.TrimSpace(primary.Path) != "" {
		return strings.TrimSpace(primary.Path)
	}
	if fallback != nil {
		return strings.TrimSpace(fallback.Path)
	}

	return ""
}

func inferFolderRefFromAuditOutputs(inputs map[string]*TypedValue, outputs map[string]TypedValue) (string, string) {
	folderID, folderPath := inferFolderRefFromTypedValueMap(outputs)
	if strings.TrimSpace(folderID) != "" || strings.TrimSpace(folderPath) != "" {
		return folderID, folderPath
	}

	return inferFolderRefFromTypedValuePointerMap(inputs)
}

func inferFolderRefFromTypedValuePointerMap(values map[string]*TypedValue) (string, string) {
	if len(values) == 0 {
		return "", ""
	}

	converted := make(map[string]TypedValue, len(values))
	for key, value := range values {
		if value == nil {
			continue
		}
		converted[key] = *value
	}

	return inferFolderRefFromTypedValueMap(converted)
}

func inferFolderRefFromTypedValueMap(values map[string]TypedValue) (string, string) {
	if len(values) == 0 {
		return "", ""
	}

	fallbackFolderPath := ""
	for _, value := range values {
		if entries, err := parseClassifiedEntryList(value.Value); err == nil && len(entries) > 0 {
			if folderID, folderPath := inferFolderRefFromClassifiedEntries(entries); folderID != "" || folderPath != "" {
				return folderID, folderPath
			}
		}

		if items, ok := categoryRouterToItems(value.Value); ok && len(items) > 0 {
			if folderID, folderPath := inferFolderRefFromProcessingItems(items); folderID != "" || folderPath != "" {
				return folderID, folderPath
			}
		}

		if results := processingStepResultsFromAny(value.Value); len(results) > 0 {
			if folderPath := inferFolderPathFromStepResults(results); folderPath != "" && strings.TrimSpace(fallbackFolderPath) == "" {
				fallbackFolderPath = folderPath
			}
		}
	}

	return "", fallbackFolderPath
}

func inferFolderRefFromClassifiedEntries(entries []ClassifiedEntry) (string, string) {
	for _, entry := range entries {
		folderID := strings.TrimSpace(entry.FolderID)
		folderPath := strings.TrimSpace(entry.Path)
		if folderID != "" || folderPath != "" {
			return folderID, folderPath
		}
		if childID, childPath := inferFolderRefFromClassifiedEntries(entry.Subtree); childID != "" || childPath != "" {
			return childID, childPath
		}
	}

	return "", ""
}

func inferFolderRefFromProcessingItems(items []ProcessingItem) (string, string) {
	for _, item := range items {
		folderID := strings.TrimSpace(item.FolderID)
		folderPath := processingItemCurrentPath(item)
		if folderPath == "" {
			folderPath = strings.TrimSpace(item.SourcePath)
		}
		if folderID != "" || folderPath != "" {
			return folderID, folderPath
		}
	}

	return "", ""
}

func inferFolderPathFromStepResults(results []ProcessingStepResult) string {
	for _, result := range results {
		if sourcePath := strings.TrimSpace(result.SourcePath); sourcePath != "" {
			return sourcePath
		}
		if targetPath := strings.TrimSpace(result.TargetPath); targetPath != "" {
			return targetPath
		}
	}

	return ""
}

func moveResultsForAudit(outputs map[string]TypedValue) []MoveResult {
	if len(outputs) == 0 {
		return nil
	}

	resultOutput, ok := outputs["step_results"]
	if !ok {
		return nil
	}

	stepResults := processingStepResultsFromAny(resultOutput.Value)
	if len(stepResults) == 0 {
		return nil
	}

	results := make([]MoveResult, 0, len(stepResults))
	for _, step := range stepResults {
		if strings.TrimSpace(step.NodeType) != "move-node" {
			continue
		}
		results = append(results, MoveResult{
			SourcePath: strings.TrimSpace(step.SourcePath),
			TargetPath: strings.TrimSpace(step.TargetPath),
			Status:     strings.TrimSpace(step.Status),
			Error:      strings.TrimSpace(step.Error),
		})
	}
	if len(results) == 0 {
		return nil
	}
	return results
}

func classifiedEntriesForAudit(outputs map[string]TypedValue) []ClassifiedEntry {
	if len(outputs) == 0 {
		return nil
	}

	entryOutput, ok := outputs["entry"]
	if !ok {
		return nil
	}

	switch typed := entryOutput.Value.(type) {
	case []ClassifiedEntry:
		return append([]ClassifiedEntry(nil), typed...)
	case ClassifiedEntry:
		return []ClassifiedEntry{typed}
	default:
		return nil
	}
}

func stringSliceOutput(outputs map[string]TypedValue, key string) []string {
	output, ok := outputs[key]
	if !ok {
		return nil
	}

	return uniqueCompactStringSlice(anyToStringSlice(output.Value))
}

func stepResultTargetsForNode(outputs map[string]TypedValue, nodeType string) []string {
	output, ok := outputs["step_results"]
	if !ok {
		return nil
	}
	stepResults := processingStepResultsFromAny(output.Value)
	if len(stepResults) == 0 {
		return nil
	}
	targets := make([]string, 0, len(stepResults))
	for _, step := range stepResults {
		if strings.TrimSpace(step.NodeType) != nodeType {
			continue
		}
		target := strings.TrimSpace(step.TargetPath)
		if target == "" {
			continue
		}
		targets = append(targets, target)
	}
	return uniqueCompactStringSlice(targets)
}

func errorString(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

func (s *WorkflowRunnerService) schemaForNode(nodeType string) NodeSchema {
	executor, ok := s.executors[nodeType]
	if !ok {
		return NodeSchema{}
	}
	return executor.Schema()
}

func (s *WorkflowRunnerService) resolveNodeInputs(
	node repository.WorkflowGraphNode,
	outputCache map[string]map[string]TypedValue,
	nodeStatusCache map[string]string,
	sourceDir string,
) (map[string]*TypedValue, map[string]InputValueSource) {
	schema := s.schemaForNode(node.Type)
	inputs := make(map[string]*TypedValue, len(schema.InputDefs()))
	inputSources := make(map[string]InputValueSource, len(schema.InputDefs()))
	inputDefMap := make(map[string]PortDef, len(schema.InputDefs()))
	for _, port := range schema.InputDefs() {
		inputs[port.Name] = nil
		inputSources[port.Name] = InputValueSourceMissing
		inputDefMap[port.Name] = port
	}

	for portName, spec := range node.Inputs {
		if spec.ConstValue != nil {
			portType := inferPortTypeForInput(schema, portName)
			inputs[portName] = &TypedValue{Type: portType, Value: *spec.ConstValue}
			if isListPortType(portType) && isEmptyListPortValue((*spec.ConstValue)) {
				inputSources[portName] = InputValueSourceEmptyOutput
			} else {
				inputSources[portName] = InputValueSourceResolved
			}
			continue
		}
		if spec.LinkSource == nil {
			continue
		}

		sourceOutputs := outputCache[spec.LinkSource.SourceNodeID]
		upstreamStatus := strings.TrimSpace(nodeStatusCache[spec.LinkSource.SourceNodeID])
		sourcePort := strings.TrimSpace(spec.LinkSource.SourcePort)
		if sourcePort == "" {
			inputs[portName] = nil
			continue
		}
		value, ok := sourceOutputs[sourcePort]
		if !ok {
			if portDef, exists := inputDefMap[portName]; exists && portDef.AcceptDefault && upstreamStatus == "skipped" {
				if defaultValue, supported := defaultInputValueForPortType(portDef.Type); supported {
					inputs[portName] = &TypedValue{Type: portDef.Type, Value: defaultValue}
					inputSources[portName] = InputValueSourceDefaultFromSkippedUpstream
					continue
				}
			}
			inputs[portName] = nil
			continue
		}
		copied := value
		inputs[portName] = &copied
		if portDef, exists := inputDefMap[portName]; exists && isListPortType(portDef.Type) && isEmptyListPortValue(copied.Value) {
			inputSources[portName] = InputValueSourceEmptyOutput
		} else {
			inputSources[portName] = InputValueSourceResolved
		}
	}

	if strings.TrimSpace(sourceDir) != "" {
		for _, portName := range []string{"source_dir", "path"} {
			if existing, exists := inputs[portName]; exists && existing != nil {
				if existing.Value == nil {
					inputs[portName] = nil
				} else if text, ok := existing.Value.(string); ok && strings.TrimSpace(text) == "" {
					inputs[portName] = nil
				}
			}
			if _, exists := inputs[portName]; exists && inputs[portName] == nil {
				inputs[portName] = &TypedValue{Type: inferPortTypeForInput(schema, portName), Value: sourceDir}
				inputSources[portName] = InputValueSourceResolved
			}
		}
	}

	return inputs, inputSources
}

func classifyNodeInputs(
	node repository.WorkflowGraphNode,
	inputs map[string]*TypedValue,
	inputSources map[string]InputValueSource,
	schema NodeSchema,
) (skip bool, fail bool, errCode string, errMsg string, skipReason string) {
	for _, port := range schema.InputDefs() {
		if !port.Required || port.Lazy {
			continue
		}
		spec, exists := node.Inputs[port.Name]
		if !exists || (spec.ConstValue == nil && spec.LinkSource == nil) {
			continue
		}
		source := InputValueSourceMissing
		if candidate, ok := inputSources[port.Name]; ok {
			source = candidate
		}
		value, ok := inputs[port.Name]
		if !ok || value == nil || value.Value == nil {
			return false, true, "NODE_INPUT_MISSING", fmt.Sprintf("required input %q is empty", port.Name), ""
		}
		if isListPortType(port.Type) && isEmptyListPortValue(value.Value) {
			if source == InputValueSourceDefaultFromSkippedUpstream && port.AcceptDefault {
				return true, false, "", "", "default_from_skipped_upstream"
			}
			if port.SkipOnEmpty {
				return true, false, "", "", "empty_input"
			}
			return false, true, "NODE_INPUT_EMPTY", fmt.Sprintf("required input %q cannot be empty list", port.Name), ""
		}
		if !isPortValueTypeCompatible(port.Type, value.Value) {
			return false, true, "NODE_INPUT_TYPE", fmt.Sprintf("input %q type mismatch, expect %s", port.Name, port.Type), ""
		}
	}
	for _, port := range schema.InputDefs() {
		if !port.Required || port.Lazy {
			continue
		}
		if _, exists := node.Inputs[port.Name]; !exists {
			return true, false, "", "", "missing_required_binding"
		}
	}
	return false, false, "", "", ""
}

func shouldSkipNode(node repository.WorkflowGraphNode, inputs map[string]*TypedValue, inputSources map[string]InputValueSource, schema NodeSchema) bool {
	skip, _, _, _, _ := classifyNodeInputs(node, inputs, inputSources, schema)
	return skip
}

func defaultInputValueForPortType(portType PortType) (any, bool) {
	switch portType {
	case PortTypeStringList:
		return []string{}, true
	case PortTypeFolderTreeList:
		return []FolderTree{}, true
	case PortTypeClassificationSignalList:
		return []ClassificationSignal{}, true
	case PortTypeClassifiedEntryList:
		return []ClassifiedEntry{}, true
	case PortTypeProcessingItemList:
		return []ProcessingItem{}, true
	case PortTypeProcessingStepResultList:
		return []ProcessingStepResult{}, true
	default:
		return nil, false
	}
}

func validateNodeOutputs(outputs map[string]TypedValue, schema NodeSchema) (errCode string, errMsg string) {
	for _, port := range schema.OutputDefs() {
		if !port.RequiredOutput {
			continue
		}
		value, ok := outputs[port.Name]
		if !ok || value.Value == nil {
			return "NODE_OUTPUT_MISSING", fmt.Sprintf("required output %q is missing", port.Name)
		}
		if !port.AllowEmpty && isListPortType(port.Type) && isEmptyListPortValue(value.Value) {
			return "NODE_OUTPUT_EMPTY", fmt.Sprintf("required output %q cannot be empty", port.Name)
		}
		if !isPortValueTypeCompatible(port.Type, value.Value) {
			return "NODE_OUTPUT_TYPE", fmt.Sprintf("output %q type mismatch, expect %s", port.Name, port.Type)
		}
	}
	return "", ""
}

func isListPortType(portType PortType) bool {
	switch portType {
	case PortTypeStringList, PortTypeFolderTreeList, PortTypeClassificationSignalList, PortTypeClassifiedEntryList, PortTypeProcessingItemList, PortTypeProcessingStepResultList:
		return true
	default:
		return false
	}
}

func isEmptyListPortValue(value any) bool {
	switch typed := value.(type) {
	case []string:
		return len(typed) == 0
	case []FolderTree:
		return len(typed) == 0
	case []ClassificationSignal:
		return len(typed) == 0
	case []ClassifiedEntry:
		return len(typed) == 0
	case []ProcessingItem:
		return len(typed) == 0
	case []ProcessingStepResult:
		return len(typed) == 0
	case []any:
		return len(typed) == 0
	}

	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return true
	}
	if rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array {
		return rv.Len() == 0
	}

	return false
}

func isPortValueTypeCompatible(portType PortType, value any) bool {
	if value == nil {
		return true
	}
	switch portType {
	case PortTypeJSON:
		return true
	case PortTypePath, PortTypeString:
		_, ok := value.(string)
		return ok
	case PortTypeBoolean:
		_, ok := value.(bool)
		return ok
	case PortTypeStringList:
		switch value.(type) {
		case []string, []any:
			return true
		default:
			return false
		}
	case PortTypeFolderTreeList:
		switch value.(type) {
		case []FolderTree, []any:
			return true
		default:
			return false
		}
	case PortTypeClassificationSignalList:
		switch value.(type) {
		case []ClassificationSignal, []any:
			return true
		default:
			return false
		}
	case PortTypeClassifiedEntryList:
		switch value.(type) {
		case []ClassifiedEntry, []any:
			return true
		default:
			return false
		}
	case PortTypeProcessingItemList:
		switch value.(type) {
		case []ProcessingItem, []any:
			return true
		default:
			return false
		}
	case PortTypeProcessingStepResultList:
		switch value.(type) {
		case []ProcessingStepResult, []any:
			return true
		default:
			return false
		}
	default:
		return true
	}
}

func (s *WorkflowRunnerService) createNodeSnapshot(
	ctx context.Context,
	run *repository.WorkflowRun,
	nodeRun *repository.NodeRun,
	kind string,
	folder *repository.Folder,
	outputs map[string]TypedValue,
) error {
	manifest := map[string]any{}
	if folder != nil {
		manifest["folder_id"] = folder.ID
		manifest["folder_path"] = folder.Path
		manifest["name"] = folder.Name
		manifest["category"] = folder.Category
		manifest["status"] = folder.Status
	}
	manifestJSON, err := json.Marshal(manifest)
	if err != nil {
		return fmt.Errorf("workflowRunner.createNodeSnapshot marshal fs manifest for node run %q: %w", nodeRun.ID, err)
	}

	outputJSON := ""
	if outputs != nil {
		encodedOutputs, encodeErr := typedValueMapToJSON(outputs, s.typeRegistry)
		if encodeErr != nil {
			return fmt.Errorf("workflowRunner.createNodeSnapshot encode typed outputs for node run %q: %w", nodeRun.ID, encodeErr)
		}
		data, marshalErr := json.Marshal(map[string]any{"outputs": encodedOutputs})
		if marshalErr != nil {
			return fmt.Errorf("workflowRunner.createNodeSnapshot marshal output for node run %q: %w", nodeRun.ID, marshalErr)
		}
		outputJSON = string(data)
	}

	if err := s.nodeSnapshots.Create(ctx, &repository.NodeSnapshot{
		ID:            uuid.NewString(),
		NodeRunID:     nodeRun.ID,
		WorkflowRunID: run.ID,
		Kind:          kind,
		FSManifest:    string(manifestJSON),
		OutputJSON:    outputJSON,
	}); err != nil {
		return fmt.Errorf("workflowRunner.createNodeSnapshot create snapshot for node run %q: %w", nodeRun.ID, err)
	}

	return nil
}

func (s *WorkflowRunnerService) publish(eventType string, payload any) {
	if s.broker == nil {
		return
	}

	_ = s.broker.Publish(eventType, payload)
}

func (s *WorkflowRunnerService) publishWorkflowRunUpdated(ctx context.Context, workflowRunID string) {
	if strings.TrimSpace(workflowRunID) == "" {
		return
	}

	run, err := s.workflowRuns.GetByID(ctx, workflowRunID)
	if err != nil {
		return
	}

	var resumeNodeID any = nil
	if strings.TrimSpace(run.ResumeNodeID) != "" {
		resumeNodeID = strings.TrimSpace(run.ResumeNodeID)
	}

	s.publish("workflow_run.updated", map[string]any{
		"job_id":          run.JobID,
		"workflow_run_id": run.ID,
		"workflow_def_id": run.WorkflowDefID,
		"folder_id":       strings.TrimSpace(run.FolderID),
		"status":          run.Status,
		"last_node_id":    strings.TrimSpace(run.LastNodeID),
		"resume_node_id":  resumeNodeID,
		"error":           strings.TrimSpace(run.Error),
	})

	s.publishFolderClassificationUpdated(
		ctx,
		run,
		workflowRunStatusToClassificationStatus(run.Status),
		strings.TrimSpace(run.LastNodeID),
		"",
		strings.TrimSpace(run.Error),
	)
}

func (s *WorkflowRunnerService) publishFolderClassificationUpdated(
	ctx context.Context,
	run *repository.WorkflowRun,
	classificationStatus string,
	nodeID string,
	nodeType string,
	errMsg string,
) {
	if s.broker == nil || s.folders == nil || run == nil {
		return
	}

	folderID := strings.TrimSpace(run.FolderID)
	if folderID == "" {
		return
	}

	folder, err := s.folders.GetByID(ctx, folderID)
	if err != nil || folder == nil {
		return
	}

	updatedAt := time.Now().UTC()

	s.publish("folder.classification.updated", map[string]any{
		"folder_id":             folder.ID,
		"job_id":                strings.TrimSpace(run.JobID),
		"workflow_run_id":       strings.TrimSpace(run.ID),
		"folder_name":           folder.Name,
		"folder_path":           folder.Path,
		"source_dir":            folder.SourceDir,
		"relative_path":         folder.RelativePath,
		"category":              folder.Category,
		"category_source":       folder.CategorySource,
		"classification_status": strings.TrimSpace(classificationStatus),
		"node_id":               strings.TrimSpace(nodeID),
		"node_type":             strings.TrimSpace(nodeType),
		"error":                 strings.TrimSpace(errMsg),
		"updated_at":            updatedAt.Format(time.RFC3339Nano),
	})
}

func workflowRunStatusToClassificationStatus(status string) string {
	switch strings.TrimSpace(status) {
	case "waiting_input":
		return "waiting_input"
	case "succeeded":
		return "completed"
	case "running", "pending":
		return "classifying"
	default:
		return "failed"
	}
}

func (s *WorkflowRunnerService) syncFolderStatusesByWorkflowRun(ctx context.Context, workflowRunID, runStatus string) error {
	if s.folders == nil || strings.TrimSpace(workflowRunID) == "" {
		return nil
	}

	nodeRuns, _, err := s.nodeRuns.List(ctx, repository.NodeRunListFilter{WorkflowRunID: workflowRunID, Page: 1, Limit: 2000})
	if err != nil {
		return fmt.Errorf("list node runs for workflow run %q: %w", workflowRunID, err)
	}
	if !workflowRunHasProcessingNode(nodeRuns) {
		return nil
	}

	if s.outputPipelineReady() {
		switch strings.TrimSpace(runStatus) {
		case "succeeded":
			if err := s.runOutputValidationChainForWorkflowRun(ctx, workflowRunID, nodeRuns); err != nil {
				return fmt.Errorf("run output validation chain for workflow run %q: %w", workflowRunID, err)
			}
			return nil
		case "partial":
			if err := s.buildOutputMappingsForWorkflowRun(ctx, workflowRunID); err != nil {
				return fmt.Errorf("build output mappings for workflow run %q: %w", workflowRunID, err)
			}
		}
	}

	targetStatus, shouldSync := workflowRunStatusToFolderStatus(runStatus)
	if !shouldSync {
		return nil
	}

	folderIDs, err := s.collectWorkflowRunFolderIDs(ctx, workflowRunID, nodeRuns)
	if err != nil {
		return err
	}
	for _, folderID := range folderIDs {
		if targetStatus == "pending" && s.completionSvc != nil {
			if markErr := s.completionSvc.MarkPending(ctx, folderID); markErr != nil {
				return fmt.Errorf("mark folder %q pending: %w", folderID, markErr)
			}
			continue
		}
		if updateErr := s.folders.UpdateStatus(ctx, folderID, targetStatus); updateErr != nil {
			return fmt.Errorf("update folder %q status to %q: %w", folderID, targetStatus, updateErr)
		}
	}
	return nil
}

func (s *WorkflowRunnerService) outputPipelineReady() bool {
	return s.mappingSvc != nil && s.validatorSvc != nil && s.completionSvc != nil
}

func (s *WorkflowRunnerService) runOutputValidationChainForWorkflowRun(ctx context.Context, workflowRunID string, nodeRuns []*repository.NodeRun) error {
	if !s.outputPipelineReady() {
		return nil
	}
	if err := s.buildOutputMappingsForWorkflowRun(ctx, workflowRunID); err != nil {
		return err
	}
	checks, err := s.validatorSvc.ValidateWorkflowRun(ctx, workflowRunID)
	if err != nil {
		return fmt.Errorf("validate workflow run outputs: %w", err)
	}
	syncedAny := false
	for _, check := range checks {
		if check == nil {
			continue
		}
		syncedAny = true
		if err := s.completionSvc.Sync(ctx, check.FolderID, check); err != nil {
			return fmt.Errorf("sync completion for folder %q: %w", check.FolderID, err)
		}
	}
	if !syncedAny {
		return s.markWorkflowRunFoldersPending(ctx, workflowRunID, nodeRuns)
	}

	return nil
}

func (s *WorkflowRunnerService) markWorkflowRunFoldersPending(ctx context.Context, workflowRunID string, nodeRuns []*repository.NodeRun) error {
	if s.completionSvc == nil {
		return nil
	}
	folderIDs, err := s.collectWorkflowRunFolderIDs(ctx, workflowRunID, nodeRuns)
	if err != nil {
		return err
	}
	for _, folderID := range folderIDs {
		if markErr := s.completionSvc.MarkPending(ctx, folderID); markErr != nil {
			return fmt.Errorf("mark folder %q pending: %w", folderID, markErr)
		}
	}
	return nil
}

func (s *WorkflowRunnerService) buildOutputMappingsForWorkflowRun(ctx context.Context, workflowRunID string) error {
	if err := s.freezeWorkflowRunSourceManifests(ctx, workflowRunID); err != nil {
		return fmt.Errorf("freeze workflow run source manifests: %w", err)
	}
	if err := s.mappingSvc.Build(ctx, workflowRunID); err != nil {
		return fmt.Errorf("build output mappings: %w", err)
	}

	return nil
}

func workflowRunStatusToFolderStatus(runStatus string) (string, bool) {
	switch strings.TrimSpace(runStatus) {
	case "waiting_input":
		return "pending", true
	case "succeeded":
		return "done", true
	case "partial":
		return "pending", true
	case "failed":
		return "pending", true
	case "rolled_back":
		return "pending", true
	default:
		return "", false
	}
}

func (s *WorkflowRunnerService) freezeWorkflowRunSourceManifests(ctx context.Context, workflowRunID string) error {
	if s.manifestSvc == nil {
		return nil
	}
	nodeRuns, _, err := s.nodeRuns.List(ctx, repository.NodeRunListFilter{WorkflowRunID: workflowRunID, Page: 1, Limit: 2000})
	if err != nil {
		return fmt.Errorf("list node runs for workflow run %q: %w", workflowRunID, err)
	}
	folderIDs, err := s.collectWorkflowRunFolderIDs(ctx, workflowRunID, nodeRuns)
	if err != nil {
		return err
	}
	if len(folderIDs) == 0 {
		return nil
	}

	items := make([]ProcessingItem, 0, len(folderIDs))
	for _, folderID := range folderIDs {
		items = append(items, ProcessingItem{FolderID: folderID})
	}
	if err := s.manifestSvc.EnsureForWorkflowRun(ctx, workflowRunID, items); err != nil {
		return fmt.Errorf("manifest_snapshot_missing: %w", err)
	}

	return nil
}

func workflowRunHasProcessingNode(nodeRuns []*repository.NodeRun) bool {
	for _, nodeRun := range nodeRuns {
		if nodeRun == nil {
			continue
		}
		if _, ok := processingStatusDrivenNodeTypes[strings.TrimSpace(nodeRun.NodeType)]; ok {
			return true
		}
	}
	return false
}

func (s *WorkflowRunnerService) collectWorkflowRunFolderIDs(ctx context.Context, workflowRunID string, nodeRuns []*repository.NodeRun) ([]string, error) {
	ordered := make([]string, 0, 8)
	seen := make(map[string]struct{}, 8)
	addFolderID := func(raw string) {
		folderID := strings.TrimSpace(raw)
		if folderID == "" {
			return
		}
		if _, exists := seen[folderID]; exists {
			return
		}
		seen[folderID] = struct{}{}
		ordered = append(ordered, folderID)
	}

	run, err := s.workflowRuns.GetByID(ctx, workflowRunID)
	if err != nil {
		return nil, fmt.Errorf("get workflow run %q: %w", workflowRunID, err)
	}
	addFolderID(run.FolderID)

	if s.reviews != nil {
		reviews, listErr := s.reviews.ListByWorkflowRunID(ctx, workflowRunID)
		if listErr != nil {
			return nil, fmt.Errorf("list processing reviews for workflow run %q: %w", workflowRunID, listErr)
		}
		for _, review := range reviews {
			if review == nil {
				continue
			}
			addFolderID(review.FolderID)
		}
	}

	for _, nodeRun := range nodeRuns {
		if nodeRun == nil || strings.TrimSpace(nodeRun.OutputJSON) == "" {
			continue
		}
		typedOutputs, typed, parseErr := parseTypedNodeOutputs(nodeRun.OutputJSON)
		if parseErr != nil || !typed {
			continue
		}

		for _, output := range typedOutputs {
			if entries, entryErr := parseClassifiedEntryList(output.Value); entryErr == nil {
				for _, entry := range entries {
					collectClassifiedEntryFolderIDs(entry, addFolderID)
				}
			}
			if items, ok := categoryRouterToItems(output.Value); ok {
				for _, item := range items {
					addFolderID(item.FolderID)
				}
			}
		}
	}

	return ordered, nil
}

func collectClassifiedEntryFolderIDs(entry ClassifiedEntry, add func(string)) {
	add(entry.FolderID)
	for _, child := range entry.Subtree {
		collectClassifiedEntryFolderIDs(child, add)
	}
}

func parseWorkflowGraph(graphJSON string) (*repository.WorkflowGraph, error) {
	return parseWorkflowGraphWithOptions(graphJSON, true)
}

func parseWorkflowGraphForValidation(graphJSON string) (*repository.WorkflowGraph, error) {
	return parseWorkflowGraphWithOptions(graphJSON, false)
}

func parseWorkflowGraphWithOptions(graphJSON string, requireNonEmptyNodes bool) (*repository.WorkflowGraph, error) {
	if strings.TrimSpace(graphJSON) == "" {
		return nil, fmt.Errorf("parseWorkflowGraph: graph_json is empty")
	}

	var raw struct {
		Nodes []struct {
			ID         string                              `json:"id"`
			Type       string                              `json:"type"`
			Label      string                              `json:"label"`
			Config     map[string]any                      `json:"config"`
			Inputs     map[string]repository.NodeInputSpec `json:"inputs"`
			UIPosition *repository.NodeUIPosition          `json:"ui_position"`
			Enabled    *bool                               `json:"enabled"`
		} `json:"nodes"`
		Edges []repository.WorkflowGraphEdge `json:"edges"`
	}
	if err := json.Unmarshal([]byte(graphJSON), &raw); err != nil {
		return nil, fmt.Errorf("parseWorkflowGraph: %w", err)
	}
	if requireNonEmptyNodes && len(raw.Nodes) == 0 {
		return nil, fmt.Errorf("parseWorkflowGraph: nodes is empty")
	}

	filtered := make([]repository.WorkflowGraphNode, 0, len(raw.Nodes))
	for _, node := range raw.Nodes {
		enabled := true
		if node.Enabled != nil {
			enabled = *node.Enabled
		}
		if !enabled {
			continue
		}
		filtered = append(filtered, repository.WorkflowGraphNode{
			ID:         node.ID,
			Type:       node.Type,
			Label:      node.Label,
			Config:     node.Config,
			Inputs:     node.Inputs,
			UIPosition: node.UIPosition,
			Enabled:    enabled,
		})
	}
	if requireNonEmptyNodes && len(filtered) == 0 {
		return nil, fmt.Errorf("parseWorkflowGraph: all nodes are disabled")
	}

	graph := &repository.WorkflowGraph{
		Nodes: filtered,
		Edges: raw.Edges,
	}

	return graph, nil
}

func (s *WorkflowRunnerService) normalizeGraphPortReferences(graph *repository.WorkflowGraph) {
	if graph == nil {
		return
	}

	nodeTypeByID := make(map[string]string, len(graph.Nodes))
	for _, node := range graph.Nodes {
		nodeTypeByID[node.ID] = node.Type
	}

	for nodeIndex := range graph.Nodes {
		node := &graph.Nodes[nodeIndex]
		for inputName, spec := range node.Inputs {
			if spec.LinkSource == nil {
				continue
			}
			if strings.TrimSpace(spec.LinkSource.SourcePort) != "" {
				spec.LinkSource.OutputPortIndex = 0
				node.Inputs[inputName] = spec
				continue
			}

			sourceType := nodeTypeByID[spec.LinkSource.SourceNodeID]
			sourceSchema := s.schemaForNode(sourceType)
			index := spec.LinkSource.OutputPortIndex
			if index < 0 || index >= len(sourceSchema.OutputDefs()) {
				continue
			}

			spec.LinkSource.SourcePort = sourceSchema.OutputDefs()[index].Name
			spec.LinkSource.OutputPortIndex = 0
			node.Inputs[inputName] = spec
		}
	}
}

func typedInputValuesForJSON(inputs map[string]*TypedValue) map[string]any {
	if len(inputs) == 0 {
		return map[string]any{}
	}

	out := make(map[string]any, len(inputs))
	for key, value := range inputs {
		if value == nil {
			out[key] = nil
			continue
		}
		out[key] = value.Value
	}

	return out
}

func cloneTypedValueMap(values map[string]TypedValue) map[string]TypedValue {
	if len(values) == 0 {
		return map[string]TypedValue{}
	}

	out := make(map[string]TypedValue, len(values))
	for key, value := range values {
		out[key] = value
	}

	return out
}

func inferPortTypeForInput(schema NodeSchema, portName string) PortType {
	if port := schema.InputPort(portName); port != nil {
		if port.Type != "" {
			return port.Type
		}
	}

	return PortTypeJSON
}

func inferPortTypeForOutput(schema NodeSchema, portName string) PortType {
	if port := schema.OutputPort(portName); port != nil {
		if port.Type != "" {
			return port.Type
		}
	}

	return PortTypeJSON
}

func typedValueMapToJSON(values map[string]TypedValue, registry *TypeRegistry) (map[string]TypedValueJSON, error) {
	out := make(map[string]TypedValueJSON, len(values))
	for key, value := range values {
		encoded, err := registry.Marshal(value)
		if err != nil {
			return nil, fmt.Errorf("encode output %q: %w", key, err)
		}
		out[key] = encoded
	}

	return out, nil
}

func typedValueMapFromJSON(values map[string]TypedValueJSON, registry *TypeRegistry) (map[string]TypedValue, error) {
	out := make(map[string]TypedValue, len(values))
	for key, value := range values {
		decoded, err := registry.Unmarshal(value)
		if err != nil {
			return nil, fmt.Errorf("decode output %q: %w", key, err)
		}
		out[key] = decoded
	}

	return out, nil
}

func inferTypedValueFromRaw(key string, raw json.RawMessage, registry *TypeRegistry) (TypedValue, bool, error) {
	typeCandidates := inferPortTypesForOutputKey(key)
	if len(typeCandidates) == 0 {
		typeCandidates = []PortType{
			PortTypeProcessingItemList,
			PortTypeProcessingStepResultList,
			PortTypeClassifiedEntryList,
			PortTypeFolderTreeList,
			PortTypeClassificationSignalList,
			PortTypeStringList,
			PortTypeString,
			PortTypeBoolean,
			PortTypePath,
			PortTypeJSON,
		}
	}

	for _, portType := range typeCandidates {
		decoded, err := registry.Unmarshal(TypedValueJSON{Type: portType, Value: raw})
		if err == nil {
			return decoded, true, nil
		}
	}

	return TypedValue{}, false, nil
}

func inferPortTypesForOutputKey(key string) []PortType {
	switch key {
	case "items", "archive_items", "video", "photo", "manga", "mixed", "other", "mixed_leaf", "unsupported":
		return []PortType{PortTypeProcessingItemList}
	case "step_results":
		return []PortType{PortTypeProcessingStepResultList}
	case "entry":
		return []PortType{PortTypeJSON, PortTypeClassifiedEntryList}
	default:
		return nil
	}
}

func parseTypedValueMapWithInference(values map[string]json.RawMessage, registry *TypeRegistry) (map[string]TypedValue, bool, error) {
	out := make(map[string]TypedValue, len(values))
	inferredAny := false

	for key, raw := range values {
		var encoded TypedValueJSON
		if err := json.Unmarshal(raw, &encoded); err == nil && encoded.Type != "" {
			decoded, decodeErr := registry.Unmarshal(encoded)
			if decodeErr != nil {
				return nil, false, fmt.Errorf("decode output %q: %w", key, decodeErr)
			}
			out[key] = decoded
			continue
		}

		decoded, inferred, inferErr := inferTypedValueFromRaw(key, raw, registry)
		if inferErr != nil {
			return nil, false, fmt.Errorf("infer output %q: %w", key, inferErr)
		}
		if !inferred {
			return nil, false, fmt.Errorf("decode output %q: unsupported legacy output shape", key)
		}
		out[key] = decoded
		inferredAny = true
	}

	return out, inferredAny, nil
}

func (s *WorkflowRunnerService) marshalTypedValuesJSON(values map[string]TypedValue) (string, error) {
	encoded, err := typedValueMapToJSON(values, s.typeRegistry)
	if err != nil {
		return "", err
	}

	raw, err := json.Marshal(encoded)
	if err != nil {
		return "", err
	}

	return string(raw), nil
}

func (s *WorkflowRunnerService) parseNodeOutputsForSchema(rawOutput string, _ NodeSchema) (map[string]TypedValue, error) {
	var typedEncoded map[string]TypedValueJSON
	if err := json.Unmarshal([]byte(rawOutput), &typedEncoded); err != nil {
		return nil, err
	}

	out, err := typedValueMapFromJSON(typedEncoded, s.typeRegistry)
	if err != nil {
		return nil, fmt.Errorf("parse typed node outputs: %w", err)
	}
	return out, nil
}

func parseTypedNodeOutputs(rawOutput string) (map[string]TypedValue, bool, error) {
	registry := NewTypeRegistry()
	var lastDecodeErr error

	var direct map[string]TypedValueJSON
	if err := json.Unmarshal([]byte(rawOutput), &direct); err == nil && len(direct) > 0 {
		decoded, decodeErr := typedValueMapFromJSON(direct, registry)
		if decodeErr == nil {
			return decoded, true, nil
		}
		lastDecodeErr = decodeErr
	}

	var wrapped struct {
		Outputs map[string]TypedValueJSON `json:"outputs"`
	}
	if err := json.Unmarshal([]byte(rawOutput), &wrapped); err == nil && len(wrapped.Outputs) > 0 {
		decoded, decodeErr := typedValueMapFromJSON(wrapped.Outputs, registry)
		if decodeErr == nil {
			return decoded, true, nil
		}
		lastDecodeErr = decodeErr
	}

	var wrappedRaw struct {
		Outputs map[string]json.RawMessage `json:"outputs"`
	}
	if err := json.Unmarshal([]byte(rawOutput), &wrappedRaw); err == nil && len(wrappedRaw.Outputs) > 0 {
		decoded, inferred, decodeErr := parseTypedValueMapWithInference(wrappedRaw.Outputs, registry)
		if decodeErr == nil {
			return decoded, true, nil
		}
		if !inferred {
			return nil, false, decodeErr
		}
	}

	var directRaw map[string]json.RawMessage
	if err := json.Unmarshal([]byte(rawOutput), &directRaw); err == nil && len(directRaw) > 0 {
		if _, hasWrappedOutputs := directRaw["outputs"]; !hasWrappedOutputs {
			decoded, inferred, decodeErr := parseTypedValueMapWithInference(directRaw, registry)
			if decodeErr == nil {
				return decoded, true, nil
			}
			if !inferred {
				return nil, false, decodeErr
			}
		}
	}

	if lastDecodeErr != nil {
		return nil, false, lastDecodeErr
	}

	return nil, false, nil
}

func topologicalNodes(graph *repository.WorkflowGraph) ([]repository.WorkflowGraphNode, error) {
	nodeMap := make(map[string]repository.WorkflowGraphNode, len(graph.Nodes))
	indegree := make(map[string]int, len(graph.Nodes))
	order := make([]string, 0, len(graph.Nodes))
	for _, node := range graph.Nodes {
		if node.ID == "" {
			return nil, fmt.Errorf("topologicalNodes: node id is empty")
		}
		if _, ok := nodeMap[node.ID]; ok {
			return nil, fmt.Errorf("topologicalNodes: duplicate node id %q", node.ID)
		}
		nodeMap[node.ID] = node
		indegree[node.ID] = 0
		order = append(order, node.ID)
	}

	adj := make(map[string][]string, len(graph.Nodes))
	adjSeen := make(map[string]map[string]struct{}, len(graph.Nodes))
	addEdge := func(sourceID, targetID string) {
		if _, ok := nodeMap[sourceID]; !ok {
			return
		}
		if _, ok := nodeMap[targetID]; !ok {
			return
		}
		if _, ok := adjSeen[sourceID]; !ok {
			adjSeen[sourceID] = make(map[string]struct{})
		}
		if _, ok := adjSeen[sourceID][targetID]; ok {
			return
		}
		adjSeen[sourceID][targetID] = struct{}{}
		adj[sourceID] = append(adj[sourceID], targetID)
		indegree[targetID]++
	}

	for _, edge := range graph.Edges {
		addEdge(edge.Source, edge.Target)
	}
	for _, node := range graph.Nodes {
		for _, spec := range node.Inputs {
			if spec.LinkSource == nil {
				continue
			}
			addEdge(strings.TrimSpace(spec.LinkSource.SourceNodeID), node.ID)
		}
	}

	queue := make([]string, 0)
	for _, id := range order {
		if indegree[id] == 0 {
			queue = append(queue, id)
		}
	}

	out := make([]repository.WorkflowGraphNode, 0, len(graph.Nodes))
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		out = append(out, nodeMap[id])

		for _, target := range adj[id] {
			indegree[target]--
			if indegree[target] == 0 {
				queue = append(queue, target)
			}
		}
	}

	if len(out) != len(graph.Nodes) {
		return nil, fmt.Errorf("topologicalNodes: cycle detected")
	}

	return out, nil
}

func topologicalLevels(graph *repository.WorkflowGraph) ([][]repository.WorkflowGraphNode, error) {
	nodeMap := make(map[string]repository.WorkflowGraphNode, len(graph.Nodes))
	indegree := make(map[string]int, len(graph.Nodes))
	order := make([]string, 0, len(graph.Nodes))
	for _, node := range graph.Nodes {
		if node.ID == "" {
			return nil, fmt.Errorf("topologicalLevels: node id is empty")
		}
		if _, ok := nodeMap[node.ID]; ok {
			return nil, fmt.Errorf("topologicalLevels: duplicate node id %q", node.ID)
		}
		nodeMap[node.ID] = node
		indegree[node.ID] = 0
		order = append(order, node.ID)
	}

	adj := make(map[string][]string, len(graph.Nodes))
	adjSeen := make(map[string]map[string]struct{}, len(graph.Nodes))
	addEdge := func(sourceID, targetID string) {
		if _, ok := nodeMap[sourceID]; !ok {
			return
		}
		if _, ok := nodeMap[targetID]; !ok {
			return
		}
		if _, ok := adjSeen[sourceID]; !ok {
			adjSeen[sourceID] = make(map[string]struct{})
		}
		if _, ok := adjSeen[sourceID][targetID]; ok {
			return
		}
		adjSeen[sourceID][targetID] = struct{}{}
		adj[sourceID] = append(adj[sourceID], targetID)
		indegree[targetID]++
	}

	for _, edge := range graph.Edges {
		addEdge(edge.Source, edge.Target)
	}
	for _, node := range graph.Nodes {
		for _, spec := range node.Inputs {
			if spec.LinkSource == nil {
				continue
			}
			addEdge(strings.TrimSpace(spec.LinkSource.SourceNodeID), node.ID)
		}
	}

	queue := make([]string, 0, len(order))
	for _, id := range order {
		if indegree[id] == 0 {
			queue = append(queue, id)
		}
	}

	processed := 0
	levels := make([][]repository.WorkflowGraphNode, 0)
	for len(queue) > 0 {
		current := append([]string(nil), queue...)
		queue = queue[:0]
		level := make([]repository.WorkflowGraphNode, 0, len(current))
		for _, id := range current {
			level = append(level, nodeMap[id])
			processed++
		}
		levels = append(levels, level)

		for _, id := range current {
			for _, target := range adj[id] {
				indegree[target]--
				if indegree[target] == 0 {
					queue = append(queue, target)
				}
			}
		}
	}

	if processed != len(graph.Nodes) {
		return nil, fmt.Errorf("topologicalLevels: cycle detected")
	}

	return levels, nil
}

type triggerNodeExecutor struct{}

func (e *triggerNodeExecutor) Type() string {
	return "trigger"
}

func (e *triggerNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "触发器",
		Description: "触发节点，启动工作流并将当前处理文件夹传递给下游",
		Outputs: []PortDef{{
			Name:        "folder",
			Type:        PortTypeJSON,
			Description: "当前处理的文件夹数据",
		}},
	}
}

func (e *triggerNodeExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	if input.Folder == nil {
		return NodeExecutionOutput{Outputs: map[string]TypedValue{"folder": {Type: PortTypeJSON, Value: nil}}, Status: ExecutionSuccess}, nil
	}

	return NodeExecutionOutput{Outputs: map[string]TypedValue{"folder": {Type: PortTypeJSON, Value: input.Folder}}, Status: ExecutionSuccess}, nil
}

func (e *triggerNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *triggerNodeExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

type extRatioClassifierNodeExecutor struct {
	fs fs.FSAdapter
}

func (e *extRatioClassifierNodeExecutor) Type() string {
	return "ext-ratio-classifier"
}

func (e *extRatioClassifierNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "扩展名分类器",
		Description: "根据目录内文件扩展名比例判断分类类别",
		Inputs: []PortDef{
			{Name: "trees", Type: PortTypeFolderTreeList, Description: "目录树列表", Required: false},
		},
		Outputs: []PortDef{{
			Name:        "signal",
			Type:        PortTypeClassificationSignalList,
			Description: "分类信号列表",
		}},
	}
}

func (e *extRatioClassifierNodeExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	rawTrees, ok := firstPresentTyped(input.Inputs, "trees")
	if !ok {
		return NodeExecutionOutput{Outputs: map[string]TypedValue{"signal": {Type: PortTypeClassificationSignalList, Value: nil}}, Status: ExecutionSuccess}, nil
	}

	trees, found, err := parseFolderTreesInput(rawTrees)
	if err != nil {
		return NodeExecutionOutput{}, fmt.Errorf("extRatioClassifier.Execute parse trees: %w", err)
	}
	if !found {
		return NodeExecutionOutput{Outputs: map[string]TypedValue{"signal": {Type: PortTypeClassificationSignalList, Value: nil}}, Status: ExecutionSuccess}, nil
	}

	signals := make([]ClassificationSignal, 0, len(trees))
	for _, tree := range trees {
		fileNames := collectTreeFileNames(tree)

		category := Classify(tree.Name, fileNames)
		confidence := 0.85
		if category == "other" {
			confidence = 0.5
		}
		reason := fmt.Sprintf("ext-ratio: %s", category)

		signals = append(signals, ClassificationSignal{
			SourcePath: tree.Path,
			Category:   category,
			Confidence: confidence,
			Reason:     reason,
		})
	}

	return NodeExecutionOutput{Outputs: map[string]TypedValue{"signal": {Type: PortTypeClassificationSignalList, Value: signals}}, Status: ExecutionSuccess}, nil
}

func (e *extRatioClassifierNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *extRatioClassifierNodeExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func collectTreeFileNames(tree FolderTree) []string {
	fileNames := make([]string, 0, len(tree.Files))
	for _, file := range tree.Files {
		fileNames = append(fileNames, file.Name)
	}

	for _, subdir := range tree.Subdirs {
		fileNames = append(fileNames, collectTreeFileNames(subdir)...)
	}

	return fileNames
}

func firstPresentTyped(inputs map[string]*TypedValue, keys ...string) (any, bool) {
	for _, key := range keys {
		value, ok := inputs[key]
		if !ok || value == nil || value.Value == nil {
			continue
		}
		return value.Value, true
	}

	return nil, false
}

func stringConfig(config map[string]any, key string) string {
	if config == nil {
		return ""
	}

	raw, ok := config[key]
	if !ok {
		return ""
	}
	text, ok := raw.(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(text)
}
