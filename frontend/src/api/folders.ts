import { request } from '@/api/client'
import type {
  Category,
  Folder,
  FolderLineageResponse,
  FolderSortBy,
  FolderStatus,
  FolderWorkflowSummary,
  FolderClassificationTreeEntry,
  PaginatedResponse,
  ScanStartResponse,
  SortOrder,
  WorkflowStageStatus,
} from '@/types'

export interface FolderQueryParams {
  status?: FolderStatus
  category?: Category
  q?: string
  page?: number
  limit?: number
  only_deleted?: boolean
  top_level_only?: boolean
  sort_by?: FolderSortBy
  sort_order?: SortOrder
}

interface RawFolder {
  id?: string
  ID?: string
  path?: string
  Path?: string
  source_dir?: string
  SourceDir?: string
  relative_path?: string
  RelativePath?: string
  name?: string
  Name?: string
  category?: Category
  Category?: Category
  category_source?: 'auto' | 'manual' | 'workflow'
  CategorySource?: 'auto' | 'manual' | 'workflow'
  status?: FolderStatus
  Status?: FolderStatus
  image_count?: number
  ImageCount?: number
  video_count?: number
  VideoCount?: number
  other_file_count?: number
  OtherFileCount?: number
  has_other_files?: boolean
  HasOtherFiles?: boolean
  total_files?: number
  TotalFiles?: number
  total_size?: number
  TotalSize?: number
  marked_for_move?: boolean
  MarkedForMove?: boolean
  deleted_at?: string | null
  DeletedAt?: string | null
  delete_staging_path?: string | null
  DeleteStagingPath?: string | null
  scanned_at?: string
  ScannedAt?: string
  updated_at?: string
  UpdatedAt?: string
  workflow_summary?: RawWorkflowSummary
  WorkflowSummary?: RawWorkflowSummary
}

interface RawWorkflowStageSummary {
  status?: WorkflowStageStatus
  status_text?: WorkflowStageStatus
  workflow_run_id?: string
  workflowRunId?: string
  WorkflowRunID?: string
  job_id?: string
  jobId?: string
  JobID?: string
  updated_at?: string
  updatedAt?: string
  UpdatedAt?: string
}

interface RawWorkflowSummary {
  classification?: RawWorkflowStageSummary
  Classification?: RawWorkflowStageSummary
  processing?: RawWorkflowStageSummary
  Processing?: RawWorkflowStageSummary
}

function parseWorkflowStage(raw?: RawWorkflowStageSummary): FolderWorkflowSummary['classification'] {
  return {
    status: raw?.status ?? raw?.status_text ?? 'not_run',
    workflow_run_id: raw?.workflow_run_id ?? raw?.workflowRunId ?? raw?.WorkflowRunID,
    job_id: raw?.job_id ?? raw?.jobId ?? raw?.JobID,
    updated_at: raw?.updated_at ?? raw?.updatedAt ?? raw?.UpdatedAt,
  }
}

function parseWorkflowSummary(raw?: RawWorkflowSummary): FolderWorkflowSummary {
  return {
    classification: parseWorkflowStage(raw?.classification ?? raw?.Classification),
    processing: parseWorkflowStage(raw?.processing ?? raw?.Processing),
  }
}

function parseFolder(raw: RawFolder): Folder {
  return {
    id: raw.id ?? raw.ID ?? '',
    path: raw.path ?? raw.Path ?? '',
    source_dir: raw.source_dir ?? raw.SourceDir ?? '',
    relative_path: raw.relative_path ?? raw.RelativePath ?? '',
    name: raw.name ?? raw.Name ?? '',
    category: raw.category ?? raw.Category ?? 'other',
    category_source: raw.category_source ?? raw.CategorySource ?? 'auto',
    status: raw.status ?? raw.Status ?? 'pending',
    image_count: raw.image_count ?? raw.ImageCount ?? 0,
    video_count: raw.video_count ?? raw.VideoCount ?? 0,
    other_file_count: raw.other_file_count ?? raw.OtherFileCount ?? 0,
    has_other_files: raw.has_other_files ?? raw.HasOtherFiles ?? false,
    total_files: raw.total_files ?? raw.TotalFiles ?? 0,
    total_size: raw.total_size ?? raw.TotalSize ?? 0,
    marked_for_move: raw.marked_for_move ?? raw.MarkedForMove ?? false,
    deleted_at: raw.deleted_at ?? raw.DeletedAt ?? null,
    delete_staging_path: raw.delete_staging_path ?? raw.DeleteStagingPath ?? null,
    scanned_at: raw.scanned_at ?? raw.ScannedAt ?? '',
    updated_at: raw.updated_at ?? raw.UpdatedAt ?? '',
    workflow_summary: parseWorkflowSummary(raw.workflow_summary ?? raw.WorkflowSummary),
  }
}

export async function listFolders(params: FolderQueryParams = {}): Promise<PaginatedResponse<Folder>> {
  const search = new URLSearchParams()

  if (params.status) search.set('status', params.status)
  if (params.category) search.set('category', params.category)
  if (params.q) search.set('q', params.q)
  if (params.page) search.set('page', String(params.page))
  if (params.limit) search.set('limit', String(params.limit))
  if (params.only_deleted) search.set('only_deleted', 'true')
  if (params.top_level_only) search.set('top_level_only', 'true')
  if (params.sort_by) search.set('sort_by', params.sort_by)
  if (params.sort_order) search.set('sort_order', params.sort_order)

  const suffix = search.toString() ? `?${search.toString()}` : ''
  const response = await request<PaginatedResponse<RawFolder>>(`/folders${suffix}`)
  return {
    ...response,
    data: (response.data ?? []).map(parseFolder),
  }
}

export async function getFolder(id: string): Promise<{ data: Folder }> {
  const response = await request<{ data: RawFolder }>(`/folders/${id}`)
  return { data: parseFolder(response.data) }
}

export async function getFolderClassificationTree(id: string): Promise<{ data: FolderClassificationTreeEntry }> {
  return request<{ data: FolderClassificationTreeEntry }>(`/folders/${id}/classification-tree`)
}

export function scanFolders() {
  return request<ScanStartResponse>('/folders/scan', { method: 'POST' })
}

export async function updateFolderCategory(id: string, category: Category): Promise<{ data: Folder }> {
  const response = await request<{ data: RawFolder }>(`/folders/${id}/category`, {
    method: 'PATCH',
    body: JSON.stringify({ category }),
  })
  return { data: parseFolder(response.data) }
}

export async function updateFolderStatus(id: string, status: FolderStatus): Promise<{ data: Folder }> {
  const response = await request<{ data: RawFolder }>(`/folders/${id}/status`, {
    method: 'PATCH',
    body: JSON.stringify({ status }),
  })
  return { data: parseFolder(response.data) }
}

export function suppressFolder(id: string) {
  return request<{ data: { deleted: boolean } }>(`/folders/${id}`, {
    method: 'DELETE',
  })
}

export async function unsuppressFolder(id: string): Promise<{ data: Folder }> {
  const response = await request<{ data: RawFolder }>(`/folders/${id}/restore`, {
    method: 'POST',
  })
  return { data: parseFolder(response.data) }
}

export function getFolderLineage(id: string) {
  return request<FolderLineageResponse>(`/folders/${id}/lineage`)
}
