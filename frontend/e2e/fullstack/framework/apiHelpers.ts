import { expect } from '@playwright/test'

import type { APIRequestContext } from '@playwright/test'

export interface FolderDTO {
  id: string
  name: string
  category: string
  status: string
  path: string
}

export interface WorkflowDefDTO {
  id: string
  name: string
  description: string
  graphJSON: string
  isActive: boolean
  version: number
}

export interface WorkflowRunDTO {
  id: string
  status: string
}

export interface ProcessingReviewSummaryDTO {
  pending: number
  approved: number
  rolled_back: number
  total: number
}

export interface ProcessingReviewItemDTO {
  id: string
  folderID: string
  status: string
  beforePath: string
  afterPath: string
}

export interface FolderLineageDTO {
  timeline: unknown[]
  flow: {
    links: unknown[]
  } | null
}

function str(value: unknown): string {
  return typeof value === 'string' ? value : ''
}

function bool(value: unknown): boolean {
  return typeof value === 'boolean' ? value : false
}

function num(value: unknown): number {
  return typeof value === 'number' ? value : 0
}

export async function saveRuntimeConfig(
  request: APIRequestContext,
  payload: { sourceDir: string; targetDir: string; scanInputDirs: string[] },
) {
  const response = await request.put('/api/config', {
    data: {
      scan_input_dirs: payload.scanInputDirs,
    },
  })
  expect(response.ok()).toBeTruthy()
}

export async function triggerScan(request: APIRequestContext) {
  const response = await request.post('/api/folders/scan')
  expect(response.ok()).toBeTruthy()
}

export async function listFolders(request: APIRequestContext): Promise<FolderDTO[]> {
  const response = await request.get('/api/folders?limit=300')
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as { data?: unknown[] }
  const data = Array.isArray(body.data) ? body.data : []
  return data.map((item) => {
    const row = item as Record<string, unknown>
    return {
      id: str(row.id ?? row.ID),
      name: str(row.name ?? row.Name),
      category: str(row.category ?? row.Category),
      status: str(row.status ?? row.Status),
      path: str(row.path ?? row.Path),
    }
  })
}

export async function getFolderDetail(request: APIRequestContext, folderID: string): Promise<FolderDTO> {
  const response = await request.get(`/api/folders/${folderID}`)
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as { data?: Record<string, unknown> }
  const row = (body.data ?? {}) as Record<string, unknown>
  return {
    id: str(row.id ?? row.ID),
    name: str(row.name ?? row.Name),
    category: str(row.category ?? row.Category),
    status: str(row.status ?? row.Status),
    path: str(row.path ?? row.Path),
  }
}

export async function listWorkflowDefs(request: APIRequestContext): Promise<WorkflowDefDTO[]> {
  const response = await request.get('/api/workflow-defs?limit=200')
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as { data?: unknown[] }
  const data = Array.isArray(body.data) ? body.data : []
  return data.map((item) => {
    const row = item as Record<string, unknown>
    return {
      id: str(row.id ?? row.ID),
      name: str(row.name ?? row.Name),
      description: str(row.description ?? row.Description),
      graphJSON: str(row.graph_json ?? row.GraphJSON),
      isActive: bool(row.is_active ?? row.IsActive),
      version: num(row.version ?? row.Version),
    }
  })
}

export async function findWorkflowDefByName(request: APIRequestContext, name: string): Promise<WorkflowDefDTO | null> {
  const defs = await listWorkflowDefs(request)
  const matched = defs.find((item) => item.name.trim() === name.trim())
  return matched ?? null
}

export async function createWorkflowDef(
  request: APIRequestContext,
  payload: { name: string; description?: string; graphJSON: string },
): Promise<string> {
  const response = await request.post('/api/workflow-defs', {
    data: {
      name: payload.name,
      description: payload.description ?? '',
      graph_json: payload.graphJSON,
    },
  })
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as { data?: { id?: string } }
  const id = body.data?.id ?? ''
  expect(id).not.toEqual('')
  return id
}

export async function upsertWorkflowDef(
  request: APIRequestContext,
  payload: { name: string; description: string; graphJSON: string; isActive?: boolean },
): Promise<string> {
  const existing = await findWorkflowDefByName(request, payload.name)
  if (existing == null) {
    return createWorkflowDef(request, {
      name: payload.name,
      description: payload.description,
      graphJSON: payload.graphJSON,
    })
  }

  const response = await request.put(`/api/workflow-defs/${existing.id}`, {
    data: {
      name: payload.name,
      description: payload.description,
      graph_json: payload.graphJSON,
      is_active: payload.isActive ?? existing.isActive,
      version: existing.version,
    },
  })
  expect(response.ok()).toBeTruthy()
  return existing.id
}

export async function startWorkflowJob(
  request: APIRequestContext,
  workflowDefID: string,
): Promise<string> {
  const response = await request.post('/api/jobs', {
    data: { workflow_def_id: workflowDefID },
  })
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as { job_id?: string }
  const jobID = body.job_id ?? ''
  expect(jobID).not.toEqual('')
  return jobID
}

export async function listWorkflowRunsByJob(
  request: APIRequestContext,
  jobID: string,
): Promise<WorkflowRunDTO[]> {
  const response = await request.get(`/api/jobs/${jobID}/workflow-runs?limit=50`)
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as { data?: unknown[] }
  const data = Array.isArray(body.data) ? body.data : []
  return data.map((item) => {
    const row = item as Record<string, unknown>
    return {
      id: str(row.id ?? row.ID),
      status: str(row.status ?? row.Status),
    }
  })
}

export async function getWorkflowRun(
  request: APIRequestContext,
  workflowRunID: string,
): Promise<WorkflowRunDTO> {
  const response = await request.get(`/api/workflow-runs/${workflowRunID}`)
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as { data?: Record<string, unknown> }
  const row = (body.data ?? {}) as Record<string, unknown>
  return {
    id: str(row.id ?? row.ID),
    status: str(row.status ?? row.Status),
  }
}

export async function waitForWorkflowRunStatus(
  request: APIRequestContext,
  jobID: string,
  expected: string,
  timeoutMs = 40000,
): Promise<string> {
  let workflowRunID = ''
  await expect
    .poll(async () => {
      const runs = await listWorkflowRunsByJob(request, jobID)
      workflowRunID = runs[0]?.id ?? ''
      return runs[0]?.status ?? ''
    }, { timeout: timeoutMs, message: `等待 workflow run 进入 ${expected}` })
    .toBe(expected)
  return workflowRunID
}

export async function waitForWorkflowRunStatusIn(
  request: APIRequestContext,
  jobID: string,
  expectedStatuses: string[],
  timeoutMs = 40000,
): Promise<WorkflowRunDTO> {
  let current: WorkflowRunDTO = { id: '', status: '' }
  await expect
    .poll(async () => {
      const runs = await listWorkflowRunsByJob(request, jobID)
      current = runs[0] ?? { id: '', status: '' }
      return expectedStatuses.includes(current.status)
    }, { timeout: timeoutMs, message: `等待 workflow run 进入状态：${expectedStatuses.join(', ')}` })
    .toBeTruthy()
  expect(current.id).not.toEqual('')
  return current
}

export async function listReviewsSummary(
  request: APIRequestContext,
  workflowRunID: string,
): Promise<ProcessingReviewSummaryDTO> {
  const response = await request.get(`/api/workflow-runs/${workflowRunID}/reviews`)
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as { summary?: Record<string, unknown> }
  const summary = body.summary ?? {}
  return {
    pending: num(summary.pending),
    approved: num(summary.approved),
    rolled_back: num(summary.rolled_back),
    total: num(summary.total),
  }
}

export async function listReviewItems(
  request: APIRequestContext,
  workflowRunID: string,
): Promise<ProcessingReviewItemDTO[]> {
  const response = await request.get(`/api/workflow-runs/${workflowRunID}/reviews`)
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as { data?: unknown[] }
  const rows = Array.isArray(body.data) ? body.data : []
  return rows.map((item) => {
    const row = item as Record<string, unknown>
    const before = (row.before ?? {}) as Record<string, unknown>
    const after = (row.after ?? {}) as Record<string, unknown>
    return {
      id: str(row.id),
      folderID: str(row.folder_id),
      status: str(row.status),
      beforePath: str(before.path),
      afterPath: str(after.path),
    }
  })
}

export async function approveAllReviews(request: APIRequestContext, workflowRunID: string): Promise<number> {
  const response = await request.post(`/api/workflow-runs/${workflowRunID}/reviews/approve-all`)
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as { approved?: number }
  return body.approved ?? 0
}

export async function rollbackAllReviews(request: APIRequestContext, workflowRunID: string): Promise<number> {
  const response = await request.post(`/api/workflow-runs/${workflowRunID}/reviews/rollback-all`)
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as { rolled_back?: number }
  return body.rolled_back ?? 0
}

export async function waitForPendingReviews(request: APIRequestContext, workflowRunID: string, timeoutMs = 40000) {
  await expect
    .poll(async () => {
      const summary = await listReviewsSummary(request, workflowRunID)
      return summary.pending
    }, { timeout: timeoutMs, message: '等待处理流进入待确认状态' })
    .toBeGreaterThan(0)
}

export async function provideWorkflowInput(
  request: APIRequestContext,
  workflowRunID: string,
  category: 'photo' | 'video' | 'manga' | 'mixed' | 'other',
) {
  const response = await request.post(`/api/workflow-runs/${workflowRunID}/provide-input`, {
    data: { category },
  })
  expect(response.status()).toBe(204)
}

export async function getFolderLineage(request: APIRequestContext, folderID: string): Promise<FolderLineageDTO> {
  const response = await request.get(`/api/folders/${folderID}/lineage`)
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as Record<string, unknown>
  const flowCandidate = body.flow as Record<string, unknown> | undefined
  const linksCandidate = Array.isArray(flowCandidate?.links) ? flowCandidate.links : []
  return {
    timeline: Array.isArray(body.timeline) ? body.timeline : [],
    flow: flowCandidate == null ? null : { links: linksCandidate },
  }
}
