import { request } from '@/api/client'
import type { Job, JobProgress, PaginatedResponse } from '@/types'

export interface JobQueryParams {
  status?: string
  page?: number
  limit?: number
}

export function listJobs(params: JobQueryParams = {}) {
  const search = new URLSearchParams()

  if (params.status) search.set('status', params.status)
  if (params.page) search.set('page', String(params.page))
  if (params.limit) search.set('limit', String(params.limit))

  const suffix = search.toString() ? `?${search.toString()}` : ''
  return request<PaginatedResponse<Job>>(`/jobs${suffix}`)
}

export function getJob(id: string) {
  return request<{ data: Job }>(`/jobs/${id}`)
}

export function getJobProgress(id: string) {
  return request<JobProgress>(`/jobs/${id}/progress`)
}
