import { request } from '@/api/client'
import type { AuditLog, PaginatedResponse } from '@/types'

interface AuditLogQueryParams {
  jobId?: string
  workflowRunId?: string
  nodeRunId?: string
  nodeId?: string
  nodeType?: string
  folderId?: string
  action?: string
  result?: string
  folderPath?: string
  from?: string
  to?: string
  page?: number
  limit?: number
}

interface RawAuditLog {
  id?: string
  ID?: string
  job_id?: string
  JobID?: string
  workflow_run_id?: string
  WorkflowRunID?: string
  node_run_id?: string
  NodeRunID?: string
  node_id?: string
  NodeID?: string
  node_type?: string
  NodeType?: string
  folder_id?: string
  FolderID?: string
  folder_path?: string
  FolderPath?: string
  action?: string
  Action?: string
  level?: string
  Level?: string
  detail?: Record<string, unknown> | null
  Detail?: Record<string, unknown> | null
  result?: string
  Result?: string
  error_msg?: string
  ErrorMsg?: string
  duration_ms?: number
  DurationMs?: number
  created_at?: string
  CreatedAt?: string
}

function parseAuditLog(raw: RawAuditLog): AuditLog {
  return {
    id: raw.id ?? raw.ID ?? '',
    job_id: raw.job_id ?? raw.JobID ?? '',
    workflow_run_id: raw.workflow_run_id ?? raw.WorkflowRunID ?? '',
    node_run_id: raw.node_run_id ?? raw.NodeRunID ?? '',
    node_id: raw.node_id ?? raw.NodeID ?? '',
    node_type: raw.node_type ?? raw.NodeType ?? '',
    folder_id: raw.folder_id ?? raw.FolderID ?? '',
    folder_path: raw.folder_path ?? raw.FolderPath ?? '',
    action: raw.action ?? raw.Action ?? '',
    level: raw.level ?? raw.Level ?? 'info',
    detail: raw.detail ?? raw.Detail ?? null,
    result: raw.result ?? raw.Result ?? '',
    error_msg: raw.error_msg ?? raw.ErrorMsg ?? '',
    duration_ms: raw.duration_ms ?? raw.DurationMs ?? 0,
    created_at: raw.created_at ?? raw.CreatedAt ?? '',
  }
}

export async function listAuditLogs(
  params: AuditLogQueryParams = {},
): Promise<PaginatedResponse<AuditLog>> {
  const search = new URLSearchParams()

  if (params.jobId) search.set('job_id', params.jobId)
  if (params.workflowRunId) search.set('workflow_run_id', params.workflowRunId)
  if (params.nodeRunId) search.set('node_run_id', params.nodeRunId)
  if (params.nodeId) search.set('node_id', params.nodeId)
  if (params.nodeType) search.set('node_type', params.nodeType)
  if (params.folderId) search.set('folder_id', params.folderId)
  if (params.action) search.set('action', params.action)
  if (params.result) search.set('result', params.result)
  if (params.folderPath) search.set('folder_path', params.folderPath)
  if (params.from) search.set('from', params.from)
  if (params.to) search.set('to', params.to)
  if (params.page) search.set('page', String(params.page))
  if (params.limit) search.set('limit', String(params.limit))

  const suffix = search.toString() ? `?${search.toString()}` : ''
  const response = await request<PaginatedResponse<RawAuditLog>>(`/audit-logs${suffix}`)
  return {
    ...response,
    data: (response.data ?? []).map(parseAuditLog),
  }
}
