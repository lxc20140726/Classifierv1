import { request } from '@/api/client'
import type { PaginatedResponse, WorkflowDefinition } from '@/types'

export interface WorkflowDefQueryParams {
  page?: number
  limit?: number
}

export interface CreateWorkflowDefBody {
  name: string
  graph_json: string
  is_active?: boolean
  version?: number
}

export interface UpdateWorkflowDefBody {
  name?: string
  graph_json?: string
  is_active?: boolean
  version?: number
}

export function listWorkflowDefs(params: WorkflowDefQueryParams = {}) {
  const search = new URLSearchParams()
  if (params.page) search.set('page', String(params.page))
  if (params.limit) search.set('limit', String(params.limit))
  const suffix = search.toString() ? `?${search.toString()}` : ''
  return request<PaginatedResponse<WorkflowDefinition>>(`/workflow-defs${suffix}`)
}

export function getWorkflowDef(id: string) {
  return request<{ data: WorkflowDefinition }>(`/workflow-defs/${id}`)
}

export function createWorkflowDef(body: CreateWorkflowDefBody) {
  return request<{ data: WorkflowDefinition }>('/workflow-defs', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function updateWorkflowDef(id: string, body: UpdateWorkflowDefBody) {
  return request<{ data: WorkflowDefinition }>(`/workflow-defs/${id}`, {
    method: 'PUT',
    body: JSON.stringify(body),
  })
}

export function deleteWorkflowDef(id: string) {
  return request<{ deleted: boolean }>(`/workflow-defs/${id}`, { method: 'DELETE' })
}
