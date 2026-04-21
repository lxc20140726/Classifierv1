package repository

import (
	"encoding/json"
	"time"
)

type Folder struct {
	ID                  string                   `db:"id"`
	Path                string                   `db:"path"`
	SourceDir           string                   `db:"source_dir"`
	RelativePath        string                   `db:"relative_path"`
	IdentityFingerprint string                   `db:"identity_fingerprint"`
	Name                string                   `db:"name"`
	Category            string                   `db:"category"`
	CategorySource      string                   `db:"category_source"`
	Status              string                   `db:"status"`
	ImageCount          int                      `db:"image_count"`
	VideoCount          int                      `db:"video_count"`
	OtherFileCount      int                      `db:"other_file_count"`
	HasOtherFiles       bool                     `db:"has_other_files"`
	TotalFiles          int                      `db:"total_files"`
	TotalSize           int64                    `db:"total_size"`
	MarkedForMove       bool                     `db:"marked_for_move"`
	DeletedAt           *time.Time               `db:"deleted_at"`
	DeleteStagingPath   string                   `db:"delete_staging_path"`
	CoverImagePath      string                   `db:"cover_image_path"`
	OutputCheckSummary  FolderOutputCheckSummary `json:"output_check_summary"`
	ScannedAt           time.Time                `db:"scanned_at"`
	UpdatedAt           time.Time                `db:"updated_at"`
	WorkflowSummary     FolderWorkflowSummary    `json:"workflow_summary"`
}

type FolderOutputCheckSummary struct {
	Status        string     `json:"status"`
	WorkflowRunID string     `json:"workflow_run_id,omitempty"`
	CheckedAt     *time.Time `json:"checked_at,omitempty"`
	MismatchCount int        `json:"mismatch_count"`
	FailedFiles   []string   `json:"failed_files,omitempty"`
}

type FolderSourceManifest struct {
	ID            string    `db:"id"`
	WorkflowRunID string    `db:"workflow_run_id"`
	FolderID      string    `db:"folder_id"`
	BatchID       string    `db:"batch_id"`
	SourcePath    string    `db:"source_path"`
	RelativePath  string    `db:"relative_path"`
	FileName      string    `db:"file_name"`
	SizeBytes     int64     `db:"size_bytes"`
	CreatedAt     time.Time `db:"created_at"`
}

type FolderOutputMapping struct {
	ID                 string    `db:"id"`
	WorkflowRunID      string    `db:"workflow_run_id"`
	FolderID           string    `db:"folder_id"`
	SourcePath         string    `db:"source_path"`
	SourceRelativePath string    `db:"source_relative_path"`
	OutputPath         string    `db:"output_path"`
	OutputContainer    string    `db:"output_container"`
	NodeType           string    `db:"node_type"`
	ArtifactType       string    `db:"artifact_type"`
	RequiredArtifact   bool      `db:"required_artifact"`
	CreatedAt          time.Time `db:"created_at"`
}

type FolderOutputCheck struct {
	ID             string             `db:"id"`
	FolderID       string             `db:"folder_id"`
	WorkflowRunID  string             `db:"workflow_run_id"`
	Status         string             `db:"status"`
	MismatchCount  int                `db:"mismatch_count"`
	FailedFiles    []string           `db:"-"`
	FailedFilesRaw json.RawMessage    `db:"failed_files"`
	Errors         []OutputCheckError `db:"-"`
	ErrorsRaw      json.RawMessage    `db:"errors"`
	CheckedAt      time.Time          `db:"checked_at"`
	CreatedAt      time.Time          `db:"created_at"`
}

type OutputCheckError struct {
	Code       string `json:"code"`
	Message    string `json:"message"`
	SourcePath string `json:"source_path,omitempty"`
	OutputPath string `json:"output_path,omitempty"`
	NodeType   string `json:"node_type,omitempty"`
}

type FolderPathObservation struct {
	ID           string    `db:"id"`
	FolderID     string    `db:"folder_id"`
	Path         string    `db:"path"`
	SourceDir    string    `db:"source_dir"`
	RelativePath string    `db:"relative_path"`
	IsCurrent    bool      `db:"is_current"`
	FirstSeenAt  time.Time `db:"first_seen_at"`
	LastSeenAt   time.Time `db:"last_seen_at"`
}

type FolderScanMatchType string

const (
	FolderScanMatchTypeNewDiscovery        FolderScanMatchType = "new_discovery"
	FolderScanMatchTypeCurrentPathMatch    FolderScanMatchType = "current_path_match"
	FolderScanMatchTypeHistoricalPathMatch FolderScanMatchType = "historical_path_match"
	FolderScanMatchTypeSourceRelativeMatch FolderScanMatchType = "source_relative_match"
)

type WorkflowStageSummary struct {
	Status        string     `json:"status"`
	WorkflowRunID string     `json:"workflow_run_id,omitempty"`
	JobID         string     `json:"job_id,omitempty"`
	UpdatedAt     *time.Time `json:"updated_at,omitempty"`
}

type FolderWorkflowSummary struct {
	Classification WorkflowStageSummary `json:"classification"`
	Processing     WorkflowStageSummary `json:"processing"`
}

type Job struct {
	ID            string         `db:"id"`
	Type          string         `db:"type"`
	WorkflowDefID string         `db:"workflow_def_id"`
	SourceDir     string         `db:"source_dir"`
	Status        string         `db:"status"`
	FolderIDs     string         `db:"folder_ids"`
	FolderTargets []FolderTarget `db:"-" json:"folder_targets,omitempty"`
	Total         int            `db:"total"`
	Done          int            `db:"done"`
	Failed        int            `db:"failed"`
	Error         string         `db:"error"`
	StartedAt     *time.Time     `db:"started_at"`
	FinishedAt    *time.Time     `db:"finished_at"`
	CreatedAt     time.Time      `db:"created_at"`
	UpdatedAt     time.Time      `db:"updated_at"`
}

type ScheduledWorkflow struct {
	ID            string     `db:"id"`
	Name          string     `db:"name"`
	JobType       string     `db:"job_type"`
	WorkflowDefID string     `db:"workflow_def_id"`
	FolderIDs     string     `db:"folder_ids"`
	SourceDirs    string     `db:"source_dirs"`
	CronSpec      string     `db:"cron_spec"`
	Enabled       bool       `db:"enabled"`
	LastRunAt     *time.Time `db:"last_run_at"`
	CreatedAt     time.Time  `db:"created_at"`
	UpdatedAt     time.Time  `db:"updated_at"`
}

type Snapshot struct {
	ID            string          `db:"id"`
	JobID         string          `db:"job_id"`
	FolderID      string          `db:"folder_id"`
	OperationType string          `db:"operation_type"`
	Before        json.RawMessage `db:"before_state"`
	After         json.RawMessage `db:"after_state"`
	Detail        json.RawMessage `db:"detail"`
	Status        string          `db:"status"`
	CreatedAt     time.Time       `db:"created_at"`
}

type AuditLog struct {
	ID            string          `db:"id"              json:"id"`
	JobID         string          `db:"job_id"          json:"job_id"`
	WorkflowRunID string          `db:"workflow_run_id" json:"workflow_run_id"`
	NodeRunID     string          `db:"node_run_id"     json:"node_run_id"`
	NodeID        string          `db:"node_id"         json:"node_id"`
	NodeType      string          `db:"node_type"       json:"node_type"`
	FolderID      string          `db:"folder_id"       json:"folder_id"`
	FolderPath    string          `db:"folder_path"     json:"folder_path"`
	Action        string          `db:"action"          json:"action"`
	Level         string          `db:"level"           json:"level"`
	Detail        json.RawMessage `db:"detail"          json:"detail"`
	Result        string          `db:"result"          json:"result"`
	ErrorMsg      string          `db:"error_msg"       json:"error_msg"`
	DurationMs    int64           `db:"duration_ms"     json:"duration_ms"`
	CreatedAt     time.Time       `db:"created_at"      json:"created_at"`
}

type FolderListFilter struct {
	Status         string
	Category       string
	Q              string
	Page           int
	Limit          int
	IncludeDeleted bool
	OnlyDeleted    bool
	TopLevelOnly   bool
	SortBy         string
	SortOrder      string
}

type JobListFilter struct {
	Status string
	Page   int
	Limit  int
}

type ScheduledWorkflowListFilter struct {
	Page  int
	Limit int
}

type AuditListFilter struct {
	JobID             string
	WorkflowRunID     string
	NodeRunID         string
	NodeID            string
	NodeType          string
	Action            string
	Result            string
	FolderID          string
	FolderPathKeyword string
	From              time.Time
	To                time.Time
	Page              int
	Limit             int
}

type AppConfigOutputDirs struct {
	Video []string `json:"video"`
	Manga []string `json:"manga"`
	Photo []string `json:"photo"`
	Other []string `json:"other"`
	Mixed []string `json:"mixed"`
}

type AppConfig struct {
	Version       int                 `json:"version"`
	ScanInputDirs []string            `json:"scan_input_dirs"`
	ScanCron      string              `json:"scan_cron"`
	OutputDirs    AppConfigOutputDirs `json:"output_dirs"`
}

type WorkflowDefinition struct {
	ID          string    `db:"id"          json:"id"`
	Name        string    `db:"name"        json:"name"`
	Description string    `db:"description" json:"description,omitempty"`
	GraphJSON   string    `db:"graph_json"  json:"graph_json"`
	IsActive    bool      `db:"is_active"   json:"is_active"`
	Version     int       `db:"version"     json:"version"`
	CreatedAt   time.Time `db:"created_at" json:"created_at"`
	UpdatedAt   time.Time `db:"updated_at" json:"updated_at"`
}

type WorkflowRun struct {
	ID             string     `db:"id"              json:"id"`
	JobID          string     `db:"job_id"          json:"job_id"`
	FolderID       string     `db:"folder_id"       json:"folder_id"`
	FolderName     string     `db:"-"               json:"folder_name,omitempty"`
	FolderPath     string     `db:"-"               json:"folder_path,omitempty"`
	SourceDir      string     `db:"source_dir"      json:"source_dir"`
	WorkflowDefID  string     `db:"workflow_def_id" json:"workflow_def_id"`
	Status         string     `db:"status"          json:"status"`
	ResumeNodeID   string     `db:"resume_node_id"  json:"resume_node_id"`
	LastNodeID     string     `db:"last_node_id"    json:"last_node_id"`
	ExternalBlocks int        `db:"external_blocks" json:"external_blocks"`
	Error          string     `db:"error"           json:"error"`
	StartedAt      *time.Time `db:"started_at"      json:"started_at"`
	FinishedAt     *time.Time `db:"finished_at"     json:"finished_at"`
	CreatedAt      time.Time  `db:"created_at"      json:"created_at"`
	UpdatedAt      time.Time  `db:"updated_at"      json:"updated_at"`
}

type NodeRun struct {
	ID                 string     `db:"id"              json:"id"`
	WorkflowRunID      string     `db:"workflow_run_id" json:"workflow_run_id"`
	NodeID             string     `db:"node_id"         json:"node_id"`
	NodeType           string     `db:"node_type"       json:"node_type"`
	Sequence           int        `db:"sequence"        json:"sequence"`
	Status             string     `db:"status"          json:"status"`
	InputJSON          string     `db:"input_json"      json:"input_json"`
	OutputJSON         string     `db:"output_json"     json:"output_json"`
	InputSignature     string     `db:"input_signature" json:"input_signature"`
	ResumeToken        string     `db:"resume_token"    json:"resume_token"`
	ResumeData         string     `db:"resume_data"     json:"resume_data"`
	Error              string     `db:"error"           json:"error"`
	ProgressPercent    *int       `db:"progress_percent"     json:"progress_percent,omitempty"`
	ProgressDone       *int       `db:"progress_done"        json:"progress_done,omitempty"`
	ProgressTotal      *int       `db:"progress_total"       json:"progress_total,omitempty"`
	ProgressStage      *string    `db:"progress_stage"       json:"progress_stage,omitempty"`
	ProgressMessage    *string    `db:"progress_message"     json:"progress_message,omitempty"`
	ProgressSourcePath *string    `db:"progress_source_path" json:"progress_source_path,omitempty"`
	ProgressTargetPath *string    `db:"progress_target_path" json:"progress_target_path,omitempty"`
	ProgressUpdatedAt  *time.Time `db:"progress_updated_at"  json:"progress_updated_at,omitempty"`
	StartedAt          *time.Time `db:"started_at"      json:"started_at"`
	FinishedAt         *time.Time `db:"finished_at"     json:"finished_at"`
	CreatedAt          time.Time  `db:"created_at"      json:"created_at"`
}

type FolderTarget struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Path string `json:"path"`
}

type NodeRunProgress struct {
	Percent    int
	Done       int
	Total      int
	Stage      string
	Message    string
	SourcePath string
	TargetPath string
}

type NodeSnapshot struct {
	ID            string    `db:"id"`
	NodeRunID     string    `db:"node_run_id"`
	WorkflowRunID string    `db:"workflow_run_id"`
	Kind          string    `db:"kind"`
	FSManifest    string    `db:"fs_manifest"`
	OutputJSON    string    `db:"output_json"`
	Compensation  string    `db:"compensation"`
	CreatedAt     time.Time `db:"created_at"`
}

type ProcessingReviewItem struct {
	ID              string          `db:"id"                json:"id"`
	WorkflowRunID   string          `db:"workflow_run_id"   json:"workflow_run_id"`
	JobID           string          `db:"job_id"            json:"job_id"`
	FolderID        string          `db:"folder_id"         json:"folder_id"`
	Status          string          `db:"status"            json:"status"`
	BeforeJSON      json.RawMessage `db:"before_json"       json:"before"`
	AfterJSON       json.RawMessage `db:"after_json"        json:"after"`
	StepResultsJSON json.RawMessage `db:"step_results_json" json:"step_results"`
	DiffJSON        json.RawMessage `db:"diff_json"         json:"diff"`
	Error           string          `db:"error"             json:"error"`
	CreatedAt       time.Time       `db:"created_at"        json:"created_at"`
	UpdatedAt       time.Time       `db:"updated_at"        json:"updated_at"`
	ReviewedAt      *time.Time      `db:"reviewed_at"       json:"reviewed_at"`
}

type WorkflowGraph struct {
	Nodes []WorkflowGraphNode `json:"nodes"`
	Edges []WorkflowGraphEdge `json:"edges"`
}

type WorkflowGraphNode struct {
	ID         string                   `json:"id"`
	Type       string                   `json:"type"`
	Label      string                   `json:"label,omitempty"`
	Config     map[string]any           `json:"config"`
	Inputs     map[string]NodeInputSpec `json:"inputs,omitempty"`
	UIPosition *NodeUIPosition          `json:"ui_position,omitempty"`
	Enabled    bool                     `json:"enabled"`
}

type WorkflowGraphEdge struct {
	ID              string `json:"id,omitempty"`
	Source          string `json:"source"`
	SourcePort      string `json:"source_port,omitempty"`
	SourcePortIndex int    `json:"-"`
	Target          string `json:"target"`
	TargetPort      string `json:"target_port,omitempty"`
	TargetPortIndex int    `json:"-"`
}

type NodeInputSpec struct {
	ConstValue *any            `json:"const_value,omitempty"`
	LinkSource *NodeLinkSource `json:"link_source,omitempty"`
}

type NodeLinkSource struct {
	SourceNodeID    string `json:"source_node_id"`
	SourcePort      string `json:"source_port,omitempty"`
	OutputPortIndex int    `json:"output_port_index,omitempty"`
}

func (e *WorkflowGraphEdge) UnmarshalJSON(data []byte) error {
	type rawEdge struct {
		ID         string          `json:"id,omitempty"`
		Source     string          `json:"source"`
		SourcePort json.RawMessage `json:"source_port"`
		Target     string          `json:"target"`
		TargetPort json.RawMessage `json:"target_port"`
	}

	var raw rawEdge
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	e.ID = raw.ID
	e.Source = raw.Source
	e.Target = raw.Target

	if err := unmarshalPortReference(raw.SourcePort, &e.SourcePort, &e.SourcePortIndex); err != nil {
		return err
	}
	if err := unmarshalPortReference(raw.TargetPort, &e.TargetPort, &e.TargetPortIndex); err != nil {
		return err
	}

	return nil
}

func (e WorkflowGraphEdge) MarshalJSON() ([]byte, error) {
	type rawEdge struct {
		ID         string `json:"id,omitempty"`
		Source     string `json:"source"`
		SourcePort any    `json:"source_port,omitempty"`
		Target     string `json:"target"`
		TargetPort any    `json:"target_port,omitempty"`
	}

	return json.Marshal(rawEdge{
		ID:         e.ID,
		Source:     e.Source,
		SourcePort: marshalPortReference(e.SourcePort, e.SourcePortIndex),
		Target:     e.Target,
		TargetPort: marshalPortReference(e.TargetPort, e.TargetPortIndex),
	})
}

func (n *NodeLinkSource) UnmarshalJSON(data []byte) error {
	type rawNodeLinkSource struct {
		SourceNodeID    string          `json:"source_node_id"`
		SourcePort      json.RawMessage `json:"source_port"`
		OutputPortIndex int             `json:"output_port_index"`
	}

	var raw rawNodeLinkSource
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	n.SourceNodeID = raw.SourceNodeID
	n.OutputPortIndex = raw.OutputPortIndex
	if err := unmarshalPortReference(raw.SourcePort, &n.SourcePort, &n.OutputPortIndex); err != nil {
		return err
	}

	return nil
}

func (n NodeLinkSource) MarshalJSON() ([]byte, error) {
	type rawNodeLinkSource struct {
		SourceNodeID    string `json:"source_node_id"`
		SourcePort      any    `json:"source_port,omitempty"`
		OutputPortIndex int    `json:"output_port_index,omitempty"`
	}

	return json.Marshal(rawNodeLinkSource{
		SourceNodeID:    n.SourceNodeID,
		SourcePort:      marshalPortReference(n.SourcePort, n.OutputPortIndex),
		OutputPortIndex: n.OutputPortIndex,
	})
}

func unmarshalPortReference(raw json.RawMessage, name *string, index *int) error {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}

	var portName string
	if err := json.Unmarshal(raw, &portName); err == nil {
		*name = portName
		return nil
	}

	var portIndex int
	if err := json.Unmarshal(raw, &portIndex); err == nil {
		*index = portIndex
		return nil
	}

	return nil
}

func marshalPortReference(name string, index int) any {
	if name != "" {
		return name
	}
	if index > 0 {
		return index
	}
	return nil
}

type NodeUIPosition struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type WorkflowDefListFilter struct {
	Page  int
	Limit int
}

type WorkflowRunListFilter struct {
	JobID    string
	FolderID string
	Status   string
	Page     int
	Limit    int
}

type NodeRunListFilter struct {
	WorkflowRunID string
	Page          int
	Limit         int
}
