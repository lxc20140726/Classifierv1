import { useEffect, useMemo, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { AlertTriangle, RotateCcw, X } from 'lucide-react'

import { listAuditLogs } from '@/api/auditLogs'
import { ApiRequestError } from '@/api/client'
import { revertSnapshot, type RevertResult } from '@/api/snapshots'
import { subscribeFolderActivityUpdated } from '@/lib/folderActivityEvents'
import { cn } from '@/lib/utils'
import { useSnapshotStore } from '@/store/snapshotStore'
import type { AuditLog, Snapshot } from '@/types'

export interface SnapshotDrawerProps {
  open: boolean
  folderId: string | null
  onClose: () => void
}

interface DrawerState {
  revertingId: string | null
  lastAttemptedId: string | null
  localError: string | null
  failureDetail: RevertResult | null
}

interface MetricPoint {
  files?: number
  sizeBytes?: number
}

interface MetricDisplay {
  before: MetricPoint | null
  after: MetricPoint
}

interface TimelineEvent {
  id: string
  createdAt: string
  title: string
  resultLabel: string
  resultClass: string
  sourceLabel: string | null
  keyChanges: Array<[string, string]>
  errorMsg: string | null
  metricCurrent: MetricPoint | null
  metricAfter: MetricPoint | null
  metricDisplay: MetricDisplay | null
  snapshot: Snapshot | null
}

const RESULT_LABELS: Record<string, string> = {
  success: '成功',
  succeeded: '成功',
  moved: '已移动',
  renamed: '已重命名',
  skipped: '已跳过',
  partial: '部分完成',
  failed: '失败',
  reverted: '已回退',
}

const RESULT_CLASSES: Record<string, string> = {
  success: 'bg-green-300 text-black border-2 border-black',
  succeeded: 'bg-green-300 text-black border-2 border-black',
  moved: 'bg-primary text-primary-foreground border-2 border-black',
  renamed: 'bg-primary text-primary-foreground border-2 border-black',
  skipped: 'bg-muted text-muted-foreground border-2 border-black',
  partial: 'bg-yellow-300 text-black border-2 border-black',
  failed: 'bg-red-300 text-red-950 border-2 border-black',
  reverted: 'bg-muted text-muted-foreground border-2 border-black',
}

const SNAPSHOT_STATUS_LABELS: Record<Snapshot['status'], string> = {
  pending: '待完成',
  committed: '已提交',
  reverted: '已回退',
}

const SNAPSHOT_STATUS_CLASSES: Record<Snapshot['status'], string> = {
  pending: 'bg-yellow-300 text-black border-2 border-black',
  committed: 'bg-primary text-primary-foreground border-2 border-black',
  reverted: 'bg-muted text-muted-foreground border-2 border-black',
}

function formatDate(value: string): string {
  if (!value) return '未知时间'
  return new Date(value).toLocaleString('zh-CN')
}

function parseTime(value: string): number {
  const ts = Date.parse(value)
  return Number.isNaN(ts) ? 0 : ts
}

function normalizeResult(value: string): string {
  return value.trim().toLowerCase()
}

function formatBytes(sizeBytes: number): string {
  if (!Number.isFinite(sizeBytes) || sizeBytes < 0) return '未知'
  if (sizeBytes === 0) return '0 B'

  const units = ['B', 'KB', 'MB', 'GB', 'TB']
  let size = sizeBytes
  let unitIndex = 0
  while (size >= 1024 && unitIndex < units.length - 1) {
    size /= 1024
    unitIndex += 1
  }
  const digits = size >= 100 || unitIndex === 0 ? 0 : 1
  return `${size.toFixed(digits)} ${units[unitIndex]}`
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (value == null || typeof value !== 'object' || Array.isArray(value)) {
    return null
  }
  return value as Record<string, unknown>
}

function asString(value: unknown): string {
  return typeof value === 'string' ? value.trim() : ''
}

function asNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value
  }
  if (typeof value === 'string') {
    const parsed = Number(value)
    if (Number.isFinite(parsed)) {
      return parsed
    }
  }
  return null
}

function asStringList(value: unknown): string[] {
  if (!Array.isArray(value)) return []
  const out: string[] = []
  for (const item of value) {
    if (typeof item !== 'string') continue
    const trimmed = item.trim()
    if (trimmed === '') continue
    out.push(trimmed)
  }
  return out
}

function metricHasValue(metric: MetricPoint | null): boolean {
  if (metric == null) return false
  return metric.files != null || metric.sizeBytes != null
}

function metricEquals(a: MetricPoint | null, b: MetricPoint | null): boolean {
  if (!metricHasValue(a) && !metricHasValue(b)) return true
  return a?.files === b?.files && a?.sizeBytes === b?.sizeBytes
}

function mergeMetric(base: MetricPoint | null, patch: MetricPoint | null): MetricPoint | null {
  if (!metricHasValue(base) && !metricHasValue(patch)) return null
  return {
    files: patch?.files ?? base?.files,
    sizeBytes: patch?.sizeBytes ?? base?.sizeBytes,
  }
}

function renderMetricLines(metric: MetricDisplay): string[] {
  const lines: string[] = []
  const beforeFiles = metric.before?.files
  const afterFiles = metric.after.files
  const beforeSize = metric.before?.sizeBytes
  const afterSize = metric.after.sizeBytes

  if (afterFiles != null) {
    if (beforeFiles != null && beforeFiles !== afterFiles) {
      lines.push(`文件数量：${beforeFiles} -> ${afterFiles}`)
    } else if (beforeFiles == null) {
      lines.push(`文件数量：${afterFiles}`)
    }
  }

  if (afterSize != null) {
    if (beforeSize != null && beforeSize !== afterSize) {
      lines.push(`总大小：${formatBytes(beforeSize)} -> ${formatBytes(afterSize)}`)
    } else if (beforeSize == null) {
      lines.push(`总大小：${formatBytes(afterSize)}`)
    }
  }

  return lines
}

function buildSnapshotEvent(snapshot: Snapshot): TimelineEvent {
  const detail = asRecord(snapshot.detail)
  const operation = snapshot.operation_type.trim().toLowerCase()

  let title = '业务操作'
  if (operation === 'classify') title = '扫描与分类'
  if (operation === 'move') title = '移动'
  if (operation === 'rename') title = '重命名'

  const keyChanges: Array<[string, string]> = []

  const category = asString(detail?.category)
  const beforeCategory = asString(detail?.before_category)
  const afterCategory = asString(detail?.after_category)
  if (beforeCategory && afterCategory && beforeCategory !== afterCategory) {
    keyChanges.push(['分类变化', `${beforeCategory} -> ${afterCategory}`])
  } else if (category) {
    keyChanges.push(['分类结果', category])
  }

  const sourceDir = asString(detail?.source_dir)
  if (sourceDir) {
    keyChanges.push(['扫描目录', sourceDir])
  }

  const relativePath = asString(detail?.relative_path)
  if (relativePath) {
    keyChanges.push(['相对路径', relativePath])
  }

  const pathChanges = (snapshot.after ?? [])
    .filter((record) => record.original_path !== record.current_path)
    .slice(0, 3)

  for (const change of pathChanges) {
    keyChanges.push(['路径变化', `${change.original_path} -> ${change.current_path}`])
  }

  const sourceLabelParts: string[] = []
  const workflowRunID = asString(detail?.workflow_run_id)
  if (workflowRunID) {
    sourceLabelParts.push('工作流')
  }
  const nodeType = asString(detail?.node_type)
  if (nodeType) {
    sourceLabelParts.push(`节点 ${nodeType}`)
  }

  const metricCurrent: MetricPoint = {}
  const totalFiles = asNumber(detail?.total_files)
  const totalSize = asNumber(detail?.total_size)
  if (totalFiles != null) metricCurrent.files = totalFiles
  if (totalSize != null) metricCurrent.sizeBytes = totalSize

  return {
    id: `snapshot-${snapshot.id}`,
    createdAt: snapshot.created_at,
    title,
    resultLabel: SNAPSHOT_STATUS_LABELS[snapshot.status],
    resultClass: SNAPSHOT_STATUS_CLASSES[snapshot.status],
    sourceLabel: sourceLabelParts.length > 0 ? sourceLabelParts.join(' / ') : null,
    keyChanges,
    errorMsg: null,
    metricCurrent: metricHasValue(metricCurrent) ? metricCurrent : null,
    metricAfter: null,
    metricDisplay: null,
    snapshot,
  }
}

function isPathRevertibleSnapshot(snapshot: Snapshot): boolean {
  const operation = snapshot.operation_type.trim().toLowerCase()
  if (operation !== 'move' && operation !== 'rename') {
    return false
  }

  const states = snapshot.after ?? snapshot.before
  return states.some((state) => state.original_path.trim() !== '' && state.current_path.trim() !== '')
}

function buildAuditEvent(audit: AuditLog): TimelineEvent | null {
  const action = audit.action.trim().toLowerCase()
  const result = normalizeResult(audit.result)
  const detail = asRecord(audit.detail)

  const isFailed = result === 'failed' || audit.error_msg.trim() !== ''

  let title: string | null = null
  if (action === 'scan') {
    title = isFailed ? '扫描' : null
  } else if (action === 'move') {
    title = isFailed ? '移动' : null
  } else if (action.startsWith('workflow.processing.review_approved')) {
    title = '审核决策（通过）'
  } else if (action.startsWith('workflow.processing.review_rolled_back')) {
    title = '审核决策（回退）'
  } else if (action.startsWith('workflow.processing.review_pending')) {
    title = '审核决策（待处理）'
  } else if (action.endsWith('.rollback')) {
    title = '回退'
  } else if (action.startsWith('workflow.compress-node')) {
    title = '压缩'
  } else if (action.startsWith('workflow.thumbnail-node')) {
    title = '缩略图生成'
  } else if (action.startsWith('workflow.move-node') || action.startsWith('phase4.processing.move-node')) {
    title = '移动'
  } else if (action.startsWith('workflow.rename-node')) {
    title = '重命名'
  } else if (action.startsWith('workflow.classification-writer') || action.includes('classifier')) {
    title = isFailed ? '分类' : null
  }

  if (title == null) {
    return null
  }

  const keyChanges: Array<[string, string]> = []

  const sourcePath = asString(detail?.source_path)
  const targetPath = asString(detail?.target_path)
  if (sourcePath && targetPath && sourcePath !== targetPath) {
    keyChanges.push(['路径变化', `${sourcePath} -> ${targetPath}`])
  }

  const beforeCategory = asString(detail?.before_category)
  const afterCategory = asString(detail?.after_category)
  if (beforeCategory && afterCategory && beforeCategory !== afterCategory) {
    keyChanges.push(['分类变化', `${beforeCategory} -> ${afterCategory}`])
  }

  const archives = asStringList(detail?.archive_paths)
  if (archives.length > 0) {
    keyChanges.push(['压缩产物', `${archives.length} 个`])
  }

  const thumbnails = asStringList(detail?.thumbnail_paths)
  if (thumbnails.length > 0) {
    keyChanges.push(['缩略图产物', `${thumbnails.length} 个`])
  }

  const diff = asRecord(detail?.diff)
  const pathChanged = diff?.path_changed === true
  const nameChanged = diff?.name_changed === true
  if (pathChanged || nameChanged) {
    keyChanges.push([
      'review 变化',
      [pathChanged ? '路径变更' : '', nameChanged ? '名称变更' : ''].filter(Boolean).join('，'),
    ])
  }

  const newArtifacts = asStringList(diff?.new_artifacts)
  if (newArtifacts.length > 0) {
    keyChanges.push(['新增产物', `${newArtifacts.length} 个`])
  }

  const sourceLabelParts: string[] = []
  if (audit.workflow_run_id.trim() !== '') {
    sourceLabelParts.push('工作流')
  }
  if (audit.node_type.trim() !== '') {
    sourceLabelParts.push(`节点 ${audit.node_type.trim()}`)
  } else {
    const detailNodeType = asString(detail?.node_type)
    if (detailNodeType) {
      sourceLabelParts.push(`节点 ${detailNodeType}`)
    }
  }

  const metricCurrent: MetricPoint = {}
  const metricAfter: MetricPoint = {}

  const directFiles = asNumber(detail?.total_files)
  const directSize = asNumber(detail?.total_size)
  if (directFiles != null) metricCurrent.files = directFiles
  if (directSize != null) metricCurrent.sizeBytes = directSize

  const before = asRecord(detail?.before)
  const after = asRecord(detail?.after)
  const beforeKeyFiles = asNumber(before?.key_files_count)
  const afterKeyFiles = asNumber(after?.key_files_count)
  if (afterKeyFiles != null) {
    metricAfter.files = afterKeyFiles
  } else if (beforeKeyFiles != null) {
    metricCurrent.files = beforeKeyFiles
  }

  if (archives.length > 0) {
    metricAfter.files = archives.length
  }

  const normalizedResult = result || 'success'
  const resultLabel = RESULT_LABELS[normalizedResult] ?? (audit.result.trim() || '已记录')
  const resultClass = RESULT_CLASSES[normalizedResult] ?? 'bg-muted text-muted-foreground border-2 border-black'

  return {
    id: `audit-${audit.id}`,
    createdAt: audit.created_at,
    title,
    resultLabel,
    resultClass,
    sourceLabel: sourceLabelParts.length > 0 ? sourceLabelParts.join(' / ') : null,
    keyChanges,
    errorMsg: audit.error_msg.trim() === '' ? null : audit.error_msg,
    metricCurrent: metricHasValue(metricCurrent) ? metricCurrent : null,
    metricAfter: metricHasValue(metricAfter) ? metricAfter : null,
    metricDisplay: null,
    snapshot: null,
  }
}

function RevertFailurePanel({ detail }: { detail: RevertResult }) {
  return (
    <div className="mt-4 space-y-3 border-2 border-foreground bg-red-100 p-4 shadow-hard">
      <div className="flex items-start gap-2">
        <AlertTriangle className="mt-0.5 h-5 w-5 shrink-0 text-red-600" />
        <div className="space-y-1">
          <p className="text-sm font-bold text-red-900">回退失败</p>
          {detail.preflight_error && (
            <p className="text-xs font-medium text-red-800">{detail.preflight_error}</p>
          )}
          {detail.error_message && !detail.preflight_error && (
            <p className="text-xs font-medium text-red-800">{detail.error_message}</p>
          )}
        </div>
      </div>

      {detail.current_state.length > 0 && (
        <div className="space-y-3">
          <p className="text-xs font-bold text-red-900">目前文件状态（未变动）：</p>
          {detail.current_state.map((s, i) => (
            <div key={i} className="border-2 border-red-900 bg-white px-3 py-2 text-xs">
              <p className="font-bold text-red-900">当前位置</p>
              <p className="break-all font-mono text-foreground">{s.current_path}</p>
              {s.current_path !== s.original_path && (
                <>
                  <p className="mt-2 font-bold text-red-900">目标位置（未到达）</p>
                  <p className="break-all font-mono text-foreground">{s.original_path}</p>
                </>
              )}
            </div>
          ))}
        </div>
      )}

      <p className="text-xs font-bold text-red-700">回退失败不会导致文件丢失，所有文件保持在回退前位置。</p>
    </div>
  )
}

export function SnapshotDrawer({ open, folderId, onClose }: SnapshotDrawerProps) {
  const snapshots = useSnapshotStore((store) => store.snapshots)
  const isLoading = useSnapshotStore((store) => store.isLoading)
  const storeError = useSnapshotStore((store) => store.error)
  const fetchSnapshots = useSnapshotStore((store) => store.fetchSnapshots)
  const handleRevertDone = useSnapshotStore((store) => store.handleRevertDone)
  const [auditLogs, setAuditLogs] = useState<AuditLog[]>([])
  const [isAuditLoading, setIsAuditLoading] = useState(false)
  const [auditError, setAuditError] = useState<string | null>(null)
  const [state, setState] = useState<DrawerState>({
    revertingId: null,
    lastAttemptedId: null,
    localError: null,
    failureDetail: null,
  })

  const prevKeyRef = useRef<string | null>(null)
  const openKey = open ? folderId : null
  if (prevKeyRef.current !== openKey) {
    prevKeyRef.current = openKey
    if (openKey !== null) {
      setState({ revertingId: null, lastAttemptedId: null, localError: null, failureDetail: null })
      setAuditLogs([])
      setIsAuditLoading(false)
      setAuditError(null)
    }
  }

  useEffect(() => {
    if (open && folderId) {
      void fetchSnapshots(folderId)
    }
  }, [fetchSnapshots, folderId, open])

  useEffect(() => {
    if (!open || !folderId) return

    let cancelled = false

    const loadAuditLogs = async () => {
      setIsAuditLoading(true)
      setAuditError(null)

      try {
        const response = await listAuditLogs({ folderId, page: 1, limit: 300 })
        if (cancelled) return
        setAuditLogs(response.data ?? [])
      } catch (error) {
        if (cancelled) return
        setAuditLogs([])
        setAuditError(error instanceof Error ? error.message : '处理记录加载失败')
      } finally {
        if (!cancelled) {
          setIsAuditLoading(false)
        }
      }
    }

    void loadAuditLogs()

    const unsubscribe = subscribeFolderActivityUpdated(() => {
      if (cancelled) return
      void fetchSnapshots(folderId)
      void loadAuditLogs()
    })

    return () => {
      cancelled = true
      unsubscribe()
    }
  }, [fetchSnapshots, folderId, open])

  async function handleRevert(snapshotId: string) {
    if (!folderId) return

    setState({ revertingId: snapshotId, lastAttemptedId: snapshotId, localError: null, failureDetail: null })

    try {
      await revertSnapshot(snapshotId)
      handleRevertDone(snapshotId)
      await fetchSnapshots(folderId)
      setState({ revertingId: null, lastAttemptedId: snapshotId, localError: null, failureDetail: null })
    } catch (error) {
      if (error instanceof ApiRequestError && error.status === 422 && error.body) {
        const revertResult = error.body.revert_result as RevertResult | undefined
        setState({
          revertingId: null,
          lastAttemptedId: snapshotId,
          localError: error.message,
          failureDetail: revertResult ?? null,
        })
      } else {
        setState({
          revertingId: null,
          lastAttemptedId: snapshotId,
          localError: error instanceof Error ? error.message : '回退失败',
          failureDetail: null,
        })
      }
    }
  }

  const error = state.localError ?? storeError

  useEffect(() => {
    if (!open) return undefined

    const { overflow } = document.body.style
    document.body.style.overflow = 'hidden'

    return () => {
      document.body.style.overflow = overflow
    }
  }, [open])

  const timelineItems = useMemo(() => {
    const rawEvents: TimelineEvent[] = []

    for (const snapshot of snapshots) {
      rawEvents.push(buildSnapshotEvent(snapshot))
    }

    for (const audit of auditLogs) {
      const event = buildAuditEvent(audit)
      if (event != null) {
        rawEvents.push(event)
      }
    }

    rawEvents.sort((a, b) => parseTime(a.createdAt) - parseTime(b.createdAt))

    let lastKnownMetric: MetricPoint | null = null
    const withMetricDisplay = rawEvents.map((event) => {
      let metricDisplay: MetricDisplay | null = null

      if (metricHasValue(event.metricCurrent)) {
        const merged = mergeMetric(lastKnownMetric, event.metricCurrent)
        if (!metricEquals(lastKnownMetric, merged)) {
          metricDisplay = { before: lastKnownMetric, after: merged ?? {} }
        } else if (lastKnownMetric == null) {
          metricDisplay = { before: null, after: merged ?? {} }
        }
        lastKnownMetric = merged
      } else if (metricHasValue(event.metricAfter)) {
        const merged = mergeMetric(lastKnownMetric, event.metricAfter)
        if (!metricEquals(lastKnownMetric, merged)) {
          metricDisplay = { before: lastKnownMetric, after: merged ?? {} }
        }
        lastKnownMetric = merged
      }

      return {
        ...event,
        metricDisplay,
      }
    })

    return withMetricDisplay.sort((a, b) => parseTime(b.createdAt) - parseTime(a.createdAt))
  }, [auditLogs, snapshots])

  const drawerContent = (
    <>
      <div
        className={cn(
          'fixed inset-0 z-40 bg-black/50 transition-opacity',
          open ? 'pointer-events-auto opacity-100' : 'pointer-events-none opacity-0',
        )}
        onClick={onClose}
        aria-hidden="true"
      />

        <aside
        className={cn(
          'fixed right-0 top-0 z-50 flex h-[100dvh] w-full max-w-xl flex-col border-l-4 border-foreground bg-background shadow-[-8px_0_0_rgba(0,0,0,1)] transition-transform duration-300 ease-out',
          open ? 'translate-x-0' : 'translate-x-full',
        )}
        aria-label="文件夹业务时间线"
      >
        <div className="border-b-2 border-foreground bg-primary px-6 py-5 text-primary-foreground">
          <div className="flex items-start justify-between gap-4">
            <div>
              <p className="text-xs font-bold uppercase tracking-[0.24em]">Timeline</p>
              <h2 className="mt-2 text-xl font-black tracking-tight">文件夹操作时间线</h2>
              <p className="mt-1 text-sm font-medium">按真实业务动作查看处理过程与关键变化。</p>
            </div>
            <button
              type="button"
              onClick={onClose}
              className="border-2 border-transparent p-2 transition-all hover:border-primary-foreground hover:bg-foreground hover:text-background"
              aria-label="关闭时间线抽屉"
            >
              <X className="h-5 w-5" />
            </button>
          </div>
        </div>

        <div className="min-h-0 flex-1 overflow-y-auto overscroll-contain bg-background px-6 pt-6 pb-[max(3rem,env(safe-area-inset-bottom))]">
          {error && !state.failureDetail && (
            <div className="mb-6 border-2 border-foreground bg-red-100 px-4 py-3 text-sm font-bold text-red-900 shadow-hard">
              {error}
            </div>
          )}
          {auditError && (
            <div className="mb-6 border-2 border-foreground bg-yellow-100 px-4 py-3 text-sm font-bold text-yellow-900 shadow-hard">
              处理记录加载失败：{auditError}
            </div>
          )}

          {(isLoading || isAuditLoading) && (
            <p className="text-sm font-bold text-muted-foreground">正在加载时间线记录...</p>
          )}

          {!isLoading && !isAuditLoading && !error && timelineItems.length === 0 && (
            <div className="border-2 border-dashed border-foreground px-4 py-12 text-center text-sm font-bold text-muted-foreground">
              这个文件夹还没有可展示的业务事件。
            </div>
          )}

          {timelineItems.length > 0 && (
            <ol className="relative space-y-8 pl-8 before:absolute before:left-[15px] before:top-2 before:h-[calc(100%-0.5rem)] before:w-0.5 before:bg-foreground">
              {timelineItems.map((item) => {
                const isReverting = item.snapshot != null && state.revertingId === item.snapshot.id
                const metricLines = item.metricDisplay != null ? renderMetricLines(item.metricDisplay) : []

                return (
                  <li key={item.id} className="relative">
                    <span className="absolute left-[-32px] top-1.5 h-4 w-4 rounded-full border-2 border-foreground bg-primary" />
                    <div className="border-2 border-foreground bg-card p-5 shadow-hard transition-all hover:-translate-y-1 hover:shadow-hard-hover">
                      <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
                        <div className="space-y-2">
                          <div className="flex flex-wrap items-center gap-3">
                            <span className="text-base font-black tracking-tight">{item.title}</span>
                            <span className={cn('px-2 py-0.5 text-xs font-bold', item.resultClass)}>{item.resultLabel}</span>
                          </div>
                          <p className="text-xs font-mono font-bold text-muted-foreground">{formatDate(item.createdAt)}</p>
                          {item.sourceLabel && (
                            <p className="text-xs font-bold text-muted-foreground">来源：{item.sourceLabel}</p>
                          )}
                        </div>

                        {item.snapshot?.status === 'committed' && isPathRevertibleSnapshot(item.snapshot) && (
                          <button
                            type="button"
                            disabled={state.revertingId !== null}
                            onClick={() => void handleRevert(item.snapshot.id)}
                            className="inline-flex items-center gap-1.5 border-2 border-foreground bg-background px-3 py-2 text-xs font-bold transition-all hover:-translate-y-0.5 hover:bg-foreground hover:text-background hover:shadow-hard disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:translate-y-0 disabled:hover:bg-background disabled:hover:text-foreground disabled:hover:shadow-none"
                          >
                            <RotateCcw className="h-4 w-4" />
                            {isReverting ? '回退中...' : '回退到此节点'}
                          </button>
                        )}
                      </div>

                      {state.failureDetail && item.snapshot != null && state.revertingId === null && item.snapshot.id === state.lastAttemptedId && (
                        <RevertFailurePanel detail={state.failureDetail} />
                      )}

                      {metricLines.length > 0 && (
                        <div className="mt-5 space-y-2 border-2 border-foreground bg-muted/30 p-4 text-xs font-bold text-foreground">
                          {metricLines.map((line) => (
                            <p key={`${item.id}-${line}`}>{line}</p>
                          ))}
                        </div>
                      )}

                      {item.keyChanges.length > 0 && (
                        <dl className="mt-5 grid gap-3 border-2 border-foreground bg-muted/30 p-4 text-xs sm:grid-cols-2">
                          {item.keyChanges.map(([label, value], index) => (
                            <div key={`${item.id}-${label}-${index}`}>
                              <dt className="font-bold text-muted-foreground">{label}</dt>
                              <dd className="mt-1 break-all font-mono font-medium text-foreground">{value}</dd>
                            </div>
                          ))}
                        </dl>
                      )}

                      {item.errorMsg && (
                        <div className="mt-5 border-2 border-foreground bg-red-100 px-3 py-2 text-xs font-bold text-red-900">
                          {item.errorMsg}
                        </div>
                      )}
                    </div>
                  </li>
                )
              })}
            </ol>
          )}
        </div>
      </aside>
    </>
  )

  if (typeof document === 'undefined') {
    return drawerContent
  }

  return createPortal(drawerContent, document.body)
}
