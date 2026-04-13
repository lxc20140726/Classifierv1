import { request } from '@/api/client'
import type { Snapshot } from '@/types'

interface SnapshotListParams {
  folderId?: string
  jobId?: string
}

interface RawSnapshotRecord {
  original_path?: string
  current_path?: string
}

interface RawSnapshot {
  id?: string
  ID?: string
  job_id?: string
  JobID?: string
  folder_id?: string
  FolderID?: string
  operation_type?: string
  OperationType?: string
  before?: RawSnapshotRecord[]
  Before?: RawSnapshotRecord[]
  after?: RawSnapshotRecord[] | null
  After?: RawSnapshotRecord[] | null
  detail?: Record<string, unknown> | null
  Detail?: Record<string, unknown> | null
  status?: 'pending' | 'committed' | 'reverted'
  Status?: 'pending' | 'committed' | 'reverted'
  created_at?: string
  CreatedAt?: string
}

function parseRecord(raw: RawSnapshotRecord) {
  return {
    original_path: raw.original_path ?? '',
    current_path: raw.current_path ?? '',
  }
}

function parseRecordArray(raw: unknown): Array<{ original_path: string; current_path: string }> {
  if (!Array.isArray(raw)) {
    return []
  }

  const records: Array<{ original_path: string; current_path: string }> = []
  for (const item of raw) {
    if (item == null || typeof item !== 'object') {
      continue
    }
    records.push(parseRecord(item as RawSnapshotRecord))
  }

  return records
}

function objectField(raw: unknown, key: string): string {
  if (raw == null || typeof raw !== 'object') {
    return ''
  }
  const value = (raw as Record<string, unknown>)[key]
  return typeof value === 'string' ? value : ''
}

function parseSnapshot(raw: RawSnapshot): Snapshot {
  const rawBefore = raw.before ?? raw.Before
  const rawAfter = raw.after ?? raw.After
  const parsedBefore = parseRecordArray(rawBefore)
  const parsedAfter = parseRecordArray(rawAfter)
  const detailRecord =
    raw.detail != null && typeof raw.detail === 'object'
      ? { ...(raw.detail as Record<string, unknown>) }
      : raw.Detail != null && typeof raw.Detail === 'object'
        ? { ...(raw.Detail as Record<string, unknown>) }
        : null
  if (detailRecord != null) {
    if (!Array.isArray(rawBefore) && rawBefore != null && typeof rawBefore === 'object') {
      const category = objectField(rawBefore, 'category')
      const source = objectField(rawBefore, 'category_source')
      if (category !== '') detailRecord.before_category = category
      if (source !== '') detailRecord.before_category_source = source
    }
    if (!Array.isArray(rawAfter) && rawAfter != null && typeof rawAfter === 'object') {
      const category = objectField(rawAfter, 'category')
      const source = objectField(rawAfter, 'category_source')
      if (category !== '') detailRecord.after_category = category
      if (source !== '') detailRecord.after_category_source = source
    }
  }
  return {
    id: raw.id ?? raw.ID ?? '',
    job_id: raw.job_id ?? raw.JobID ?? '',
    folder_id: raw.folder_id ?? raw.FolderID ?? '',
    operation_type: raw.operation_type ?? raw.OperationType ?? 'move',
    before: parsedBefore,
    after: rawAfter != null ? parsedAfter : null,
    detail: detailRecord,
    status: raw.status ?? raw.Status ?? 'pending',
    created_at: raw.created_at ?? raw.CreatedAt ?? '',
  }
}

export async function listSnapshots(params: SnapshotListParams): Promise<Snapshot[]> {
  const query = new URLSearchParams()

  if (params.folderId) query.set('folder_id', params.folderId)
  if (params.jobId) query.set('job_id', params.jobId)

  const suffix = query.toString() ? `?${query.toString()}` : ''
  const response = await request<{ data: RawSnapshot[] }>(`/snapshots${suffix}`)
  return (response.data ?? []).map(parseSnapshot)
}

export interface RevertPathState {
  original_path: string
  current_path: string
}

export interface RevertResult {
  ok: boolean
  error_message?: string
  preflight_error?: string
  current_state: RevertPathState[]
}

export interface RevertResponse {
  reverted: boolean
  revert_result: RevertResult
}

export async function revertSnapshot(snapshotId: string): Promise<RevertResponse> {
  return request<RevertResponse>(`/snapshots/${snapshotId}/revert`, {
    method: 'POST',
  })
}
