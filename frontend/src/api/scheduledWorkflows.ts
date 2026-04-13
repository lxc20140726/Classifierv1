import { request } from '@/api/client'
import type { PaginatedResponse, ScheduledWorkflow } from '@/types'

interface RawScheduledWorkflow {
  id?: string
  ID?: string
  name?: string
  Name?: string
  job_type?: 'workflow' | 'scan'
  JobType?: 'workflow' | 'scan'
  workflow_def_id?: string
  WorkflowDefID?: string
  folder_ids?: string[]
  FolderIDs?: string[]
  source_dirs?: string[]
  SourceDirs?: string[]
  cron_spec?: string
  CronSpec?: string
  enabled?: boolean
  Enabled?: boolean
  last_run_at?: string | null
  LastRunAt?: string | null
  created_at?: string
  CreatedAt?: string
  updated_at?: string
  UpdatedAt?: string
}

export interface ScheduledWorkflowBody {
  job_type: 'workflow' | 'scan'
  name: string
  workflow_def_id: string
  folder_ids: string[]
  source_dirs: string[]
  cron_spec: string
  enabled: boolean
}

function parseScheduledWorkflow(raw: RawScheduledWorkflow): ScheduledWorkflow {
  return {
    id: raw.id ?? raw.ID ?? '',
    name: raw.name ?? raw.Name ?? '',
    job_type: raw.job_type ?? raw.JobType ?? 'workflow',
    workflow_def_id: raw.workflow_def_id ?? raw.WorkflowDefID ?? '',
    folder_ids: raw.folder_ids ?? raw.FolderIDs ?? [],
    source_dirs: raw.source_dirs ?? raw.SourceDirs ?? [],
    cron_spec: raw.cron_spec ?? raw.CronSpec ?? '',
    enabled: raw.enabled ?? raw.Enabled ?? false,
    last_run_at: raw.last_run_at ?? raw.LastRunAt ?? null,
    created_at: raw.created_at ?? raw.CreatedAt ?? '',
    updated_at: raw.updated_at ?? raw.UpdatedAt ?? '',
  }
}

export async function listScheduledWorkflows() {
  const response = await request<PaginatedResponse<RawScheduledWorkflow>>('/scheduled-workflows?limit=100')
  return {
    ...response,
    data: (response.data ?? []).map(parseScheduledWorkflow),
  }
}

export async function createScheduledWorkflow(body: ScheduledWorkflowBody) {
  const response = await request<{ data: RawScheduledWorkflow }>('/scheduled-workflows', {
    method: 'POST',
    body: JSON.stringify(body),
  })

  return { data: parseScheduledWorkflow(response.data) }
}

export async function updateScheduledWorkflow(id: string, body: ScheduledWorkflowBody) {
  const response = await request<{ data: RawScheduledWorkflow }>(`/scheduled-workflows/${id}`, {
    method: 'PUT',
    body: JSON.stringify(body),
  })

  return { data: parseScheduledWorkflow(response.data) }
}

export function deleteScheduledWorkflow(id: string) {
  return request<{ deleted: boolean }>(`/scheduled-workflows/${id}`, { method: 'DELETE' })
}

export function runScheduledWorkflowNow(id: string) {
  return request<{ job_id: string }>(`/scheduled-workflows/${id}/run`, { method: 'POST' })
}
