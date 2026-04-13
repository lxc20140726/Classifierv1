import { request } from '@/api/client'
import type {
  PaginatedResponse,
  ProcessingReviewItem,
  ProcessingReviewSummary,
  ProvideInputBody,
  WorkflowRun,
  WorkflowRunDetail,
} from '@/types'

export interface WorkflowRunQueryParams {
  page?: number
  limit?: number
}

export interface StartWorkflowJobBody {
  workflow_def_id: string
}

export function startWorkflowJob(body: StartWorkflowJobBody) {
  return request<{ job_id: string }>('/jobs', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function listWorkflowRunsByJob(
  jobId: string,
  params: WorkflowRunQueryParams = {},
) {
  const search = new URLSearchParams()
  if (params.page) search.set('page', String(params.page))
  if (params.limit) search.set('limit', String(params.limit))
  const suffix = search.toString() ? `?${search.toString()}` : ''
  return request<PaginatedResponse<WorkflowRun>>(`/jobs/${jobId}/workflow-runs${suffix}`)
}

export function getWorkflowRunDetail(id: string) {
  return request<WorkflowRunDetail>(`/workflow-runs/${id}`)
}

export function resumeWorkflowRun(id: string) {
  return request<{ resumed: boolean }>(`/workflow-runs/${id}/resume`, { method: 'POST' })
}

export function rollbackWorkflowRun(id: string) {
  return request<{ rolled_back: boolean }>(`/workflow-runs/${id}/rollback`, { method: 'POST' })
}

export function provideWorkflowRunInput(id: string, body: ProvideInputBody) {
  return request<undefined>(`/workflow-runs/${id}/provide-input`, {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function provideWorkflowRunRawInput(id: string, body: Record<string, unknown>) {
  return request<undefined>(`/workflow-runs/${id}/provide-input`, {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function listWorkflowRunReviews(id: string) {
  return request<{ data: ProcessingReviewItem[]; summary: ProcessingReviewSummary }>(`/workflow-runs/${id}/reviews`)
}

export function approveWorkflowRunReview(id: string, reviewId: string) {
  return request<{ approved: boolean }>(`/workflow-runs/${id}/reviews/${reviewId}/approve`, {
    method: 'POST',
  })
}

export function rollbackWorkflowRunReview(id: string, reviewId: string) {
  return request<{ rolled_back: boolean }>(`/workflow-runs/${id}/reviews/${reviewId}/rollback`, {
    method: 'POST',
  })
}

export function approveAllWorkflowRunPendingReviews(id: string) {
  return request<{ approved: number }>(`/workflow-runs/${id}/reviews/approve-all`, {
    method: 'POST',
  })
}

export function rollbackAllWorkflowRunPendingReviews(id: string) {
  return request<{ rolled_back: number }>(`/workflow-runs/${id}/reviews/rollback-all`, {
    method: 'POST',
  })
}
