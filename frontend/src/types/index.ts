export type Category = 'photo' | 'video' | 'mixed' | 'manga' | 'other'
export type FolderStatus = 'pending' | 'done' | 'skip'
export type CategorySource = 'auto' | 'manual' | 'workflow'
export type FolderSortBy = 'updated_at' | 'total_size'
export type SortOrder = 'asc' | 'desc'
export type JobStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'partial' | 'cancelled' | 'waiting_input' | 'rolled_back'
export type WorkflowStageStatus = 'not_run' | 'running' | 'succeeded' | 'failed' | 'waiting_input' | 'partial' | 'rolled_back'

export interface WorkflowStageSummary {
  status: WorkflowStageStatus
  workflow_run_id?: string
  job_id?: string
  updated_at?: string
}

export interface FolderWorkflowSummary {
  classification: WorkflowStageSummary
  processing: WorkflowStageSummary
}

export interface Folder {
  id: string
  path: string
  source_dir: string
  relative_path: string
  name: string
  category: Category
  category_source: CategorySource
  status: FolderStatus
  image_count: number
  video_count: number
  other_file_count: number
  has_other_files: boolean
  total_files: number
  total_size: number
  marked_for_move: boolean
  deleted_at: string | null
  delete_staging_path?: string | null
  scanned_at: string
  updated_at: string
  workflow_summary: FolderWorkflowSummary
}

export interface FileRecord {
  original_path: string
  current_path: string
}

export interface Snapshot {
  id: string
  job_id: string
  folder_id: string
  operation_type: string
  before: FileRecord[]
  after: FileRecord[] | null
  detail: Record<string, unknown> | null
  status: 'pending' | 'committed' | 'reverted'
  created_at: string
}

export interface AppConfig {
  version?: number
  scan_cron?: string
  scan_input_dirs?: string[]
  output_dirs?: {
    video?: string[]
    manga?: string[]
    photo?: string[]
    other?: string[]
    mixed?: string[]
  }
}

export interface ApiError {
  error: string
}

export interface PaginatedResponse<T> {
  data: T[]
  total: number
  page: number
  limit: number
}

export interface Job {
  id: string
  type: string
  workflow_def_id?: string
  status: JobStatus
  folder_ids: string[] | null
  total: number
  done: number
  failed: number
  error: string
  started_at: string | null
  finished_at: string | null
  created_at: string
  updated_at: string
}

export interface ScheduledWorkflow {
  id: string
  name: string
  job_type: 'workflow' | 'scan'
  workflow_def_id: string
  folder_ids: string[]
  source_dirs: string[]
  cron_spec: string
  enabled: boolean
  last_run_at: string | null
  created_at: string
  updated_at: string
}

export interface JobProgress {
  job_id: string
  status: JobStatus
  done: number
  total: number
  failed: number
  updated_at: string
}

export interface ScanStartResponse {
  started: boolean
  job_id: string
  source_dirs: string[]
}

export interface ScanProgressEvent {
  job_id: string
  folder_id?: string
  folder_name?: string
  folder_path?: string
  source_dir?: string
  relative_path?: string
  category?: string
  done: number
  total: number
  error?: string
}

export interface JobDoneEvent {
  job_id: string
  status: JobStatus
  processed?: number
  failed?: number
  total: number
}

export interface AuditLog {
  id: string
  job_id: string
  workflow_run_id: string
  node_run_id: string
  node_id: string
  node_type: string
  folder_id: string
  folder_path: string
  action: string
  level: string
  detail: Record<string, unknown> | null
  result: string
  error_msg: string
  duration_ms: number
  created_at: string
}

export type WorkflowRunStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'partial' | 'waiting_input' | 'rolled_back'
export type NodeRunStatus = 'pending' | 'running' | 'succeeded' | 'failed' | 'skipped' | 'waiting_input'
export type NodeType =
  | 'trigger'
  | 'ext-ratio-classifier'
  | 'move-node'
  | 'folder-tree-scanner'
  | 'folder-picker'
  | 'name-keyword-classifier'
  | 'file-tree-classifier'
  | 'confidence-check'
  | 'subtree-aggregator'
  | 'classification-reader'
  | 'db-subtree-reader'
  | 'classification-db-result-preview'
  | 'processing-result-preview'
  | 'folder-splitter'
  | 'category-router'
  | 'mixed-leaf-router'
  | 'rename-node'
  | 'compress-node'
  | 'thumbnail-node'

export interface ProvideInputBody {
  category: 'photo' | 'video' | 'manga' | 'mixed' | 'other'
}

export interface WorkflowDefinition {
  id: string
  name: string
  description?: string
  graph_json: string
  is_active: boolean
  version: number
  created_at: string
  updated_at: string
}

export interface WorkflowRun {
  id: string
  job_id: string
  folder_id: string
  source_dir?: string
  workflow_def_id: string
  status: WorkflowRunStatus
  resume_node_id: string | null
  last_node_id?: string | null
  error: string
  started_at?: string | null
  finished_at?: string | null
  created_at: string
  updated_at: string
}

export interface NodeRun {
  id: string
  workflow_run_id: string
  node_id: string
  node_type: NodeType
  sequence: number
  status: NodeRunStatus
  input_json: string
  output_json: string
  error: string
  started_at: string | null
  finished_at: string | null
  created_at: string
}

export interface WorkflowRunDetail {
  data: WorkflowRun
  node_runs: NodeRun[]
  review_summary?: ProcessingReviewSummary
}

export type ProcessingReviewStatus = 'pending' | 'approved' | 'rolled_back'

export interface ProcessingReviewDiff {
  path_changed: boolean
  name_changed: boolean
  new_artifacts: string[]
  executed_steps: Array<{
    node_type: string
    node_label: string
    status: string
    source_path?: string
    target_path?: string
  }>
}

export interface ProcessingReviewSummary {
  total: number
  pending: number
  approved: number
  rolled_back: number
  rejected: number
  failed_step_runs: number
}

export interface ProcessingReviewItem {
  id: string
  workflow_run_id: string
  job_id: string
  folder_id: string
  status: ProcessingReviewStatus
  before: {
    path?: string
    name?: string
    cover_image?: string
    status?: string
    key_files_count?: number
  } | null
  after: {
    path?: string
    name?: string
    cover_image?: string
    status?: string
    artifact_paths?: string[]
  } | null
  step_results: Array<{
    source_path: string
    target_path?: string
    node_type: string
    node_label: string
    status: string
    error?: string
  }>
  diff: ProcessingReviewDiff | null
  error: string
  created_at: string
  updated_at: string
  reviewed_at: string | null
}

export type FolderLineageNodeType = 'origin' | 'historical_path' | 'current_path' | 'artifact'
export type FolderLineageEdgeType = 'moved_to' | 'renamed_to' | 'produced'
export type FolderLineageTimelineEventType =
  | 'scan_discovered'
  | 'move'
  | 'rename'
  | 'rollback'
  | 'artifact_created'
  | 'processing_failed'

export interface FolderLineageNode {
  id: string
  type: FolderLineageNodeType
  label: string
  path: string
  first_seen_at?: string
  last_seen_at?: string
  step_types?: string[]
  workflow_run_id?: string
  job_id?: string
}

export interface FolderLineageEdge {
  id: string
  type: FolderLineageEdgeType
  source: string
  target: string
  occurred_at?: string
  step_type?: string
  workflow_run_id?: string
  job_id?: string
}

export interface FolderLineageTimelineEvent {
  id: string
  type: FolderLineageTimelineEventType
  occurred_at: string
  title: string
  description?: string
  path_from?: string
  path_to?: string
  workflow_run_id?: string
  job_id?: string
  step_type?: string
}

export interface FolderLineageReviewStep {
  node_type: string
  node_label?: string
  status?: string
  source_path?: string
  target_path?: string
  error?: string
}

export interface FolderLineageReview {
  workflow_run_id: string
  job_id: string
  status: string
  before?: Record<string, unknown>
  after?: Record<string, unknown>
  diff?: Record<string, unknown>
  executed_steps: FolderLineageReviewStep[]
  updated_at: string
  reviewed_at?: string
}

export interface FolderLineageDirectory {
  id?: string
  path: string
  label: string
  artifact_type?: string
}

export interface FolderLineageFile {
  id: string
  directory_id: string
  name: string
  path: string
  artifact_type?: string
  node_type?: string
  workflow_run_id?: string
  job_id?: string
}

export interface FolderLineageSourceFile {
  id: string
  directory_id: string
  name: string
  path: string
  relative_path: string
  size_bytes: number
}

export interface FolderLineageLink {
  id: string
  source_file_id: string
  target_file_id: string
  workflow_run_id?: string
  job_id?: string
  node_type?: string
}

export interface FolderLineageFlow {
  source_directory: FolderLineageDirectory
  target_directories: FolderLineageDirectory[]
  source_files: FolderLineageSourceFile[]
  target_files: FolderLineageFile[]
  links: FolderLineageLink[]
}

export interface FolderLineageResponse {
  folder: Folder
  summary: {
    original_path: string
    current_path: string
    status: string
    category: string
    last_processed_at?: string
  }
  graph: {
    nodes: FolderLineageNode[]
    edges: FolderLineageEdge[]
  }
  flow?: FolderLineageFlow
  timeline: FolderLineageTimelineEvent[]
  review?: FolderLineageReview
}

export interface WorkflowNodeEvent {
  job_id: string
  workflow_run_id: string
  folder_id?: string
  node_id: string
  node_type: string
  status?: NodeRunStatus
  error?: string
}

export interface WorkflowRunUpdatedEvent {
  job_id: string
  workflow_run_id: string
  workflow_def_id: string
  status: WorkflowRunStatus
  last_node_id?: string
  resume_node_id?: string | null
  error?: string
}

export type LiveClassificationStatus = 'scanning' | 'classifying' | 'waiting_input' | 'completed' | 'failed'

export interface FolderClassificationLiveEvent {
  folder_id: string
  job_id: string
  workflow_run_id: string
  folder_name: string
  folder_path: string
  source_dir: string
  relative_path: string
  category: Category
  category_source: CategorySource
  classification_status: LiveClassificationStatus
  node_id?: string
  node_type?: string
  error?: string
  updated_at: string
}

export interface LiveClassificationItem {
  folder_id: string
  job_id: string
  workflow_run_id: string
  folder_name: string
  folder_path: string
  source_dir: string
  relative_path: string
  category: Category
  category_source: CategorySource
  classification_status: LiveClassificationStatus
  node_id: string
  node_type: string
  error: string
  entered_at: string
  last_event_at: string
}

export type ClassificationFileKind = 'photo' | 'video' | 'manga' | 'other'

export interface FolderClassificationTreeFile {
  name: string
  ext: string
  kind: ClassificationFileKind
  size_bytes: number
}

export interface FolderClassificationTreeEntry {
  folder_id: string
  path: string
  name: string
  category: Category
  category_source: CategorySource
  status: FolderStatus
  has_other_files: boolean
  total_files: number
  image_count: number
  video_count: number
  other_file_count: number
  files: FolderClassificationTreeFile[]
  subtree: FolderClassificationTreeEntry[]
}

export interface NodeUIPosition {
  x: number
  y: number
}

export interface NodeLinkSource {
  source_node_id: string
  source_port?: string
  output_port_index?: number
}

export interface NodeInputSpec {
  const_value?: unknown
  link_source?: NodeLinkSource
}

export interface WorkflowGraphNode {
  id: string
  type: NodeType | string
  label?: string
  config: Record<string, unknown>
  inputs?: Record<string, NodeInputSpec>
  ui_position?: NodeUIPosition
  enabled: boolean
}

export interface WorkflowGraphEdge {
  id?: string
  source: string
  source_port: number | string
  target: string
  target_port: number | string
}

export interface WorkflowGraph {
  nodes: WorkflowGraphNode[]
  edges: WorkflowGraphEdge[]
}

export interface NodeSchemaPort {
  name: string
  type: string
  description: string
  required: boolean
}

export interface NodeSchema {
  type: string
  label: string
  description: string
  input_ports: NodeSchemaPort[]
  output_ports: NodeSchemaPort[]
  config_schema?: Record<string, unknown>
}
