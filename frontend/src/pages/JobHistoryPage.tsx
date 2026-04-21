import { useEffect, useMemo, useRef, useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { Link, useSearchParams } from 'react-router-dom'

import { ClassificationPreviewInline } from '@/components/workflow-preview/ClassificationPreviewInline'
import { PathChangePreview } from '@/components/PathChangePreview'
import { ProcessingPreviewInline } from '@/components/workflow-preview/ProcessingPreviewInline'
import { isClassificationSummary, isProcessingSummary, parseNodePreviewSummary } from '@/components/workflow-preview/previewUtils'
import { useIsMobile } from '@/hooks/useIsMobile'
import { cn } from '@/lib/utils'
import { useJobStore } from '@/store/jobStore'
import { useNotificationStore } from '@/store/notificationStore'
import { useWorkflowDefStore } from '@/store/workflowDefStore'
import { useWorkflowRunStore } from '@/store/workflowRunStore'
import type {
  Job,
  JobStatus,
  NodeRun,
  NodeRunStatus,
  ProcessingReviewItem,
  WorkflowRun,
  WorkflowRunStatus,
} from '@/types'

function formatDate(dateStr: string | null) {
  if (!dateStr) return '鈥?
  return new Date(dateStr).toLocaleString('zh-CN')
}

function formatDuration(startedAt: string | null | undefined, finishedAt: string | null | undefined) {
  if (!startedAt) return '鈥?
  const end = finishedAt ? new Date(finishedAt) : new Date()
  const start = new Date(startedAt)
  const diffMs = Math.max(0, end.getTime() - start.getTime())
  if (diffMs < 1000) return '<1 绉?
  const secs = Math.floor(diffMs / 1000)
  if (secs < 60) return `${secs} 绉抈
  if (secs < 3600) return `${Math.floor(secs / 60)} 鍒?${secs % 60} 绉抈
  return `${Math.floor(secs / 3600)} 灏忔椂 ${Math.floor((secs % 3600) / 60)} 鍒哷
}

function buildFailedAuditLogsLink(params: Record<string, string>) {
  const search = new URLSearchParams({ ...params, result: 'failed' })
  return `/audit-logs?${search.toString()}`
}

function readTargetParam(value: string | null) {
  return value?.trim() ?? ''
}

function resolveRunFolderLabel(run: WorkflowRun) {
  const folderName = (run.folder_name ?? '').trim()
  if (folderName !== '') return folderName
  const folderID = (run.folder_id ?? '').trim()
  if (folderID === '') return '-'
  return folderID.slice(0, 8)
}

function resolveRunFolderTitle(run: WorkflowRun) {
  const folderPath = (run.folder_path ?? '').trim()
  if (folderPath !== '') return folderPath
  const folderName = (run.folder_name ?? '').trim()
  if (folderName !== '') return folderName
  return (run.folder_id ?? '').trim()
}

function resolveWorkflowName(
  job: Job,
  workflowNameMap: Record<string, string>,
) {
  const workflowDefId = (job.workflow_def_id ?? '').trim()
  if (job.type === 'scan') return '鎵弿浠诲姟'
  if (workflowDefId !== '') {
    const matched = workflowNameMap[workflowDefId]
    if (matched && matched.trim() !== '') return matched
    return `宸插垹闄ゅ伐浣滄祦锛?{workflowDefId}锛塦
  }
  if (job.type === 'workflow') return '宸插垹闄ゅ伐浣滄祦锛堢己灏戝畾涔塈D锛?
  return '鏈煡宸ヤ綔娴?
}

function resolveJobCategoryLabel(jobType: string) {
  if (jobType === 'scan') return '鎵弿'
  if (jobType === 'workflow') return '宸ヤ綔娴?
  return `鍏朵粬(${jobType})`
}

function resolveJobTargetNames(job: Job) {
  const targets = job.folder_targets ?? []
  if (targets.length > 0) {
    return targets.map((target) => target.name || target.id)
  }
  return job.folder_ids ?? []
}

function summarizeJobTargets(job: Job) {
  const names = resolveJobTargetNames(job)
  if (names.length === 0) return '-'
  if (names.length <= 2) return names.join('，')
  return `${names.slice(0, 2).join('，')} +${names.length - 2}`
}

const JOB_STATUS_LABELS: Record<JobStatus, string> = {
  pending: '绛夊緟涓?,
  running: '杩涜涓?,
  succeeded: '宸插畬鎴?,
  failed: '澶辫触',
  partial: '閮ㄥ垎瀹屾垚',
  cancelled: '宸插彇娑?,
  waiting_input: '寰呯‘璁?,
  rolled_back: '宸插洖閫€',
}

const JOB_STATUS_STYLES: Record<JobStatus, string> = {
  pending: 'bg-gray-200 text-gray-900 border-2 border-foreground',
  running: 'bg-blue-300 text-blue-900 border-2 border-foreground',
  succeeded: 'bg-green-300 text-green-900 border-2 border-foreground',
  failed: 'bg-red-300 text-red-900 border-2 border-foreground',
  partial: 'bg-yellow-300 text-yellow-900 border-2 border-foreground',
  cancelled: 'bg-gray-300 text-gray-900 border-2 border-foreground',
  waiting_input: 'bg-purple-300 text-purple-900 border-2 border-foreground',
  rolled_back: 'bg-orange-300 text-orange-900 border-2 border-foreground',
}

const WF_STATUS_LABELS: Record<WorkflowRunStatus, string> = {
  pending: '绛夊緟涓?,
  running: '杩涜涓?,
  succeeded: '宸插畬鎴?,
  failed: '澶辫触',
  partial: '閮ㄥ垎瀹屾垚',
  waiting_input: '寰呯‘璁?,
  rolled_back: '宸插洖閫€',
}

const WF_STATUS_STYLES: Record<WorkflowRunStatus, string> = {
  pending: 'bg-gray-200 text-gray-900 border-2 border-foreground',
  running: 'bg-blue-300 text-blue-900 border-2 border-foreground',
  succeeded: 'bg-green-300 text-green-900 border-2 border-foreground',
  failed: 'bg-red-300 text-red-900 border-2 border-foreground',
  partial: 'bg-yellow-300 text-yellow-900 border-2 border-foreground',
  waiting_input: 'bg-purple-300 text-purple-900 border-2 border-foreground',
  rolled_back: 'bg-orange-300 text-orange-900 border-2 border-foreground',
}

const NODE_STATUS_LABELS: Record<NodeRunStatus, string> = {
  pending: '绛夊緟涓?,
  running: '杩涜涓?,
  succeeded: '宸插畬鎴?,
  failed: '澶辫触',
  skipped: '宸茶烦杩?,
  waiting_input: '寰呯‘璁?,
}

const NODE_STATUS_STYLES: Record<NodeRunStatus, string> = {
  pending: 'bg-gray-200 text-gray-900 border-2 border-foreground',
  running: 'bg-blue-300 text-blue-900 border-2 border-foreground',
  succeeded: 'bg-green-300 text-green-900 border-2 border-foreground',
  failed: 'bg-red-300 text-red-900 border-2 border-foreground',
  skipped: 'bg-gray-300 text-gray-900 border-2 border-foreground',
  waiting_input: 'bg-purple-300 text-purple-900 border-2 border-foreground',
}

const JOB_HISTORY_PAGE_SIZE = 20
const WORKFLOW_RUN_PAGE_SIZE = 20

function StatusBadge({ status, labels, styles }: {
  status: string
  labels: Record<string, string>
  styles: Record<string, string>
}) {
  return (
    <span
      className={cn(
        'inline-flex items-center px-2 py-0.5 text-[10px] font-black',
        styles[status] ?? 'bg-muted text-muted-foreground border-2 border-foreground',
      )}
    >
      {labels[status] ?? status}
    </span>
  )
}

function ProgressBar({ done, total }: { done: number; total: number }) {
  const pct = total > 0 ? Math.round((done / total) * 100) : 0
  const progressClassName = pct >= 100
    ? 'w-full'
    : pct >= 75
      ? 'w-3/4'
      : pct >= 50
        ? 'w-1/2'
        : pct >= 25
          ? 'w-1/4'
          : pct > 0
            ? 'w-1/12'
            : 'w-0'

  return (
    <div className="flex items-center gap-3">
      <div className="h-2 flex-1 overflow-hidden border-2 border-foreground bg-muted">
        <div className={cn('h-full bg-foreground transition-all duration-300', progressClassName)} />
      </div>
      <span className="min-w-[3rem] text-right text-xs font-black tabular-nums">{done}/{total}</span>
    </div>
  )
}

function PaginationControls({
  page,
  totalPages,
  total,
  rowCount,
  isLoading,
  onPageChange,
}: {
  page: number
  totalPages: number
  total: number
  rowCount: number
  isLoading: boolean
  onPageChange: (nextPage: number) => void
}) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3 rounded border-2 border-foreground bg-muted/20 px-4 py-3">
      <p className="text-sm font-bold text-muted-foreground">
        绗?<span className="font-black text-foreground">{page}</span> / {totalPages} 椤碉紝鍏眥' '}
        <span className="font-black text-foreground">{total}</span> 鏉★紙褰撳墠 {rowCount} 鏉★級
      </p>
      <div className="flex items-center gap-2">
        <button
          type="button"
          disabled={page <= 1 || isLoading}
          onClick={() => onPageChange(Math.max(1, page - 1))}
          className="border-2 border-foreground bg-background px-3 py-1 text-xs font-bold hover:bg-foreground hover:text-background disabled:opacity-50"
        >
          涓婁竴椤?
        </button>
        <button
          type="button"
          disabled={page >= totalPages || isLoading}
          onClick={() => onPageChange(Math.min(totalPages, page + 1))}
          className="border-2 border-foreground bg-background px-3 py-1 text-xs font-bold hover:bg-foreground hover:text-background disabled:opacity-50"
        >
          涓嬩竴椤?
        </button>
      </div>
    </div>
  )
}

function NodeResultPreview({ node }: { node: NodeRun }) {
  const summary = parseNodePreviewSummary(node)

  if (node.node_type === 'classification-db-result-preview') {
    const classificationSummary = isClassificationSummary(summary) ? summary : null
    return (
      <div className="max-h-56 overflow-y-auto">
        <ClassificationPreviewInline summary={classificationSummary} compact />
      </div>
    )
  }

  if (node.node_type === 'processing-result-preview') {
    const processingSummary = isProcessingSummary(summary) ? summary : null
    return (
      <div className="max-h-56 overflow-y-auto">
        <ProcessingPreviewInline summary={processingSummary} compact />
      </div>
    )
  }

  return <span className="text-[10px] font-bold text-muted-foreground">鈥?/span>
}

function NodeRunsPanel({
  run,
  highlightFailedNodes,
  isMobile = false,
}: {
  run: WorkflowRun
  highlightFailedNodes?: boolean
  isMobile?: boolean
}) {
  const { nodesByRunId, fetchRunDetail } = useWorkflowRunStore()
  const nodes = nodesByRunId[run.id] ?? []

  useEffect(() => {
    void fetchRunDetail(run.id)
  }, [run.id, fetchRunDetail])

  if (nodes.length === 0) {
    return <p className="py-4 text-xs font-bold text-muted-foreground text-center">鏆傛棤鑺傜偣璁板綍</p>
  }

  if (isMobile) {
    return (
      <div className="space-y-2">
        {nodes.map((node) => (
          <article
            key={node.id || node.node_id}
            className={cn(
              'border-2 border-foreground bg-background p-3',
              highlightFailedNodes && node.status === 'failed' && 'bg-red-100/70',
            )}
          >
            <div className="flex flex-wrap items-start justify-between gap-2">
              <div className="min-w-0">
                <p className="break-all font-mono text-xs font-black">{node.node_id}</p>
                <p className="mt-1 break-all text-[11px] font-bold text-muted-foreground">{node.node_type}</p>
              </div>
              <StatusBadge status={node.status} labels={NODE_STATUS_LABELS} styles={NODE_STATUS_STYLES} />
            </div>
            <div className="mt-2 grid grid-cols-2 gap-2 text-[11px] font-bold text-muted-foreground">
              <p>搴忓彿锛?span className="text-foreground">{node.sequence}</span></p>
              <p>鑰楁椂锛?span className="text-foreground">{formatDuration(node.started_at, node.finished_at)}</span></p>
            </div>
            <div className="mt-2 border-2 border-foreground bg-muted/10 px-2 py-2">
              {typeof node.progress_percent === 'number' ? (
                <div className="space-y-1">
                  <p className="font-black tabular-nums text-foreground">{node.progress_percent}%</p>
                  <p className="text-[11px] font-bold text-muted-foreground">{node.progress_stage || node.progress_message || '杩涜涓?}</p>
                  {node.progress_source_path && (
                    <p className="break-all font-mono text-[10px] text-muted-foreground">{node.progress_source_path}</p>
                  )}
                </div>
              ) : (
                <p className="text-[11px] font-bold text-muted-foreground">鏈紑濮?/p>
              )}
            </div>
            <div className="mt-2">
              <NodeResultPreview node={node} />
              {(node.status === 'failed' || node.status === 'waiting_input') && node.error && (
                <div className="mt-2 rounded border-2 border-red-900 bg-red-50 p-2">
                  <p className="break-all text-[11px] font-bold text-red-900">{node.error}</p>
                  <Link
                    to={buildFailedAuditLogsLink({
                      job_id: run.job_id,
                      workflow_run_id: run.id,
                      node_run_id: node.id,
                      node_id: node.node_id,
                      node_type: node.node_type,
                    })}
                    className="mt-2 inline-flex border-2 border-red-900 bg-white px-2 py-1 text-[10px] font-bold text-red-900 hover:bg-red-900 hover:text-white"
                  >
                    鏌ョ湅瀹¤鏃ュ織
                  </Link>
                </div>
              )}
            </div>
          </article>
        ))}
      </div>
    )
  }

  return (
    <div className="pl-4 py-2">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b-2 border-foreground bg-muted/30">
            <th className="py-2 pr-4 text-left font-black tracking-widest">鑺傜偣ID</th>
            <th className="py-2 pr-4 text-left font-black tracking-widest">绫诲瀷</th>
            <th className="py-2 pr-4 text-left font-black tracking-widest">搴忓彿</th>
            <th className="py-2 pr-4 text-left font-black tracking-widest">鐘舵€?/th>
            <th className="py-2 pr-4 text-left font-black tracking-widest">杩涘害</th>
            <th className="py-2 text-left font-black tracking-widest">鑰楁椂</th>
            <th className="py-2 text-left font-black tracking-widest">缁撴灉棰勮</th>
          </tr>
        </thead>
        <tbody>
          {nodes.map((node) => (
            <tr
              key={node.id || node.node_id}
              className={cn(
                'border-b-2 border-foreground/20 last:border-0 hover:bg-muted/10 align-top',
                highlightFailedNodes && node.status === 'failed' && 'bg-red-100/70',
              )}
            >
              <td className="py-3 pr-4 font-mono font-bold">{node.node_id}</td>
              <td className="py-3 pr-4 font-bold">{node.node_type}</td>
              <td className="py-3 pr-4 font-black">{node.sequence}</td>
              <td className="py-3 pr-4">
                <StatusBadge status={node.status} labels={NODE_STATUS_LABELS} styles={NODE_STATUS_STYLES} />
              </td>
              <td className="py-3 pr-4">
                {typeof node.progress_percent === 'number' ? (
                  <div className="space-y-1">
                    <p className="font-black tabular-nums">{node.progress_percent}%</p>
                    <p className="text-[11px] font-bold text-muted-foreground">{node.progress_stage || node.progress_message || '杩涜涓?}</p>
                    {node.progress_source_path && (
                      <p className="max-w-[20rem] truncate font-mono text-[10px] text-muted-foreground">{node.progress_source_path}</p>
                    )}
                  </div>
                ) : (
                  <span className="text-[11px] font-bold text-muted-foreground">鏈紑濮?/span>
                )}
              </td>
              <td className="py-3 font-mono font-bold">{formatDuration(node.started_at, node.finished_at)}</td>
              <td className="py-3">
                <NodeResultPreview node={node} />
                {(node.status === 'failed' || node.status === 'waiting_input') && node.error && (
                  <div className="mt-2 rounded border-2 border-red-900 bg-red-50 p-2">
                    <p className="text-[11px] font-bold text-red-900 break-all">{node.error}</p>
                    <Link
                      to={buildFailedAuditLogsLink({
                        job_id: run.job_id,
                        workflow_run_id: run.id,
                        node_run_id: node.id,
                        node_id: node.node_id,
                        node_type: node.node_type,
                      })}
                      className="mt-2 inline-flex border-2 border-red-900 bg-white px-2 py-1 text-[10px] font-bold text-red-900 hover:bg-red-900 hover:text-white"
                    >
                      鏌ョ湅瀹¤鏃ュ織
                    </Link>
                  </div>
                )}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

function WorkflowRunRow({
  run,
  forceExpanded,
  highlightFailedNodes,
  isMobile = false,
  onRefreshRuns,
}: {
  run: WorkflowRun
  forceExpanded?: boolean
  highlightFailedNodes?: boolean
  isMobile?: boolean
  onRefreshRuns: () => Promise<void>
}) {
  const [expanded, setExpanded] = useState(false)
  const [isActing, setIsActing] = useState(false)
  const actionLockRef = useRef(false)
  const isExpanded = !!forceExpanded || expanded
  const {
    rollbackRun,
    fetchRunDetail,
    fetchRunReviews,
    approveReview,
    rollbackReview,
    approveAllPendingReviews,
    rollbackAllPendingReviews,
    reviewsByRunId,
    reviewSummaryByRunId,
  } = useWorkflowRunStore()
  const pushNotification = useNotificationStore((s) => s.pushNotification)
  const reviews = reviewsByRunId[run.id] ?? []
  const reviewSummary = reviewSummaryByRunId[run.id]

  useEffect(() => {
    if (run.status === 'waiting_input') {
      void fetchRunDetail(run.id)
      void fetchRunReviews(run.id)
    }
  }, [run.id, run.status, fetchRunDetail, fetchRunReviews])

  function beginAction() {
    if (actionLockRef.current) return false
    actionLockRef.current = true
    setIsActing(true)
    return true
  }

  function endAction() {
    actionLockRef.current = false
    setIsActing(false)
  }

  async function handleRollback() {
    if (!beginAction()) return
    try {
      await rollbackRun(run.id)
      await onRefreshRuns()
      pushNotification({
        level: 'success',
        title: '鍥炴粴瀹屾垚',
        message: `宸ヤ綔娴佽繍琛?${run.id.slice(0, 8)} 宸插洖閫€銆俙,
        jobId: run.job_id,
      })
    } catch (rollbackError) {
      const message = rollbackError instanceof Error ? rollbackError.message : '鍥炴粴澶辫触'
      pushNotification({ level: 'error', title: '鍥炴粴澶辫触', message, jobId: run.job_id })
    } finally {
      endAction()
    }
  }

  async function handleApproveReview(reviewId: string) {
    if (!beginAction()) return
    try {
      await approveReview(run.id, reviewId)
      await onRefreshRuns()
      pushNotification({
        level: 'success',
        title: '纭宸查€氳繃',
        message: `纭椤?${reviewId.slice(0, 8)} 宸叉爣璁颁负閫氳繃銆俙,
        jobId: run.job_id,
      })
    } catch (approveError) {
      const message = approveError instanceof Error ? approveError.message : '纭閫氳繃澶辫触'
      pushNotification({
        level: 'error',
        title: '纭閫氳繃澶辫触',
        message,
        jobId: run.job_id,
      })
    } finally {
      endAction()
    }
  }

  async function handleRollbackReview(reviewId: string) {
    if (!beginAction()) return
    try {
      await rollbackReview(run.id, reviewId)
      await onRefreshRuns()
      pushNotification({
        level: 'success',
        title: '宸插洖閫€纭椤?,
        message: `纭椤?${reviewId.slice(0, 8)} 宸叉墽琛屽洖閫€銆俙,
        jobId: run.job_id,
      })
    } catch (rollbackError) {
      const message = rollbackError instanceof Error ? rollbackError.message : '鍥為€€纭椤瑰け璐?
      pushNotification({
        level: 'error',
        title: '鍥為€€纭椤瑰け璐?,
        message,
        jobId: run.job_id,
      })
    } finally {
      endAction()
    }
  }

  async function handleApproveAllPendingReviews() {
    if (!beginAction()) return
    try {
      const approved = await approveAllPendingReviews(run.id)
      await onRefreshRuns()
      pushNotification({
        level: 'success',
        title: '鎵归噺閫氳繃瀹屾垚',
        message: `宸叉壒閲忛€氳繃 ${approved} 涓‘璁ら」銆俙,
        jobId: run.job_id,
      })
    } catch (approveError) {
      const message = approveError instanceof Error ? approveError.message : '鎵归噺纭閫氳繃澶辫触'
      pushNotification({
        level: 'error',
        title: '鎵归噺纭閫氳繃澶辫触',
        message,
        jobId: run.job_id,
      })
    } finally {
      endAction()
    }
  }

  async function handleRollbackAllPendingReviews() {
    if (!beginAction()) return
    try {
      const rolledBack = await rollbackAllPendingReviews(run.id)
      await onRefreshRuns()
      pushNotification({
        level: 'success',
        title: '鎵归噺鍥為€€瀹屾垚',
        message: `宸叉壒閲忓洖閫€ ${rolledBack} 涓‘璁ら」銆俙,
        jobId: run.job_id,
      })
    } catch (rollbackError) {
      const message = rollbackError instanceof Error ? rollbackError.message : '鎵归噺鍥為€€澶辫触'
      pushNotification({
        level: 'error',
        title: '鎵归噺鍥為€€澶辫触',
        message,
        jobId: run.job_id,
      })
    } finally {
      endAction()
    }
  }

  const failedRunPanel = (run.status === 'failed' || run.status === 'partial') && run.error ? (
    <div className="rounded border-2 border-red-900 bg-red-50 p-3">
      <p className="text-xs font-bold text-red-900 break-all">{run.error}</p>
      <Link
        to={buildFailedAuditLogsLink({ job_id: run.job_id, workflow_run_id: run.id })}
        className="mt-2 inline-flex border-2 border-red-900 bg-white px-2 py-1 text-[10px] font-bold text-red-900 hover:bg-red-900 hover:text-white"
      >
        鏌ョ湅瀹¤鏃ュ織
      </Link>
    </div>
  ) : null

  const reviewPanel = run.status === 'waiting_input' ? (
    <div className="space-y-3 border-2 border-purple-900 bg-purple-50 p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h4 className="text-sm font-black text-purple-900">鐩綍纭闈㈡澘</h4>
        {reviewSummary && (
          <p className="text-xs font-bold text-purple-900">
            寰呯‘璁?{reviewSummary.pending} / 鎬绘暟 {reviewSummary.total}锛堥€氳繃 {reviewSummary.approved}锛屽洖閫€ {reviewSummary.rolled_back}锛?          </p>
        )}
        {(reviewSummary?.pending ?? 0) > 0 && (
          <div className="flex w-full flex-wrap items-center gap-2 sm:w-auto">
            <button
              type="button"
              disabled={isActing}
              onClick={(event) => {
                event.stopPropagation()
                void handleApproveAllPendingReviews()
              }}
              className="border-2 border-green-900 bg-green-200 px-2 py-1 text-xs font-bold text-green-900 hover:bg-green-900 hover:text-green-100 disabled:opacity-50"
            >
              鍏ㄩ儴纭閫氳繃
            </button>
            <button
              type="button"
              disabled={isActing}
              onClick={(event) => {
                event.stopPropagation()
                void handleRollbackAllPendingReviews()
              }}
              className="border-2 border-red-900 bg-red-200 px-2 py-1 text-xs font-bold text-red-900 hover:bg-red-900 hover:text-red-100 disabled:opacity-50"
            >
              鍏ㄩ儴涓嶉€氳繃骞跺洖閫€
            </button>
          </div>
        )}
      </div>
      {reviews.length === 0 ? (
        <p className="text-xs font-bold text-muted-foreground">鏆傛棤纭椤?/p>
      ) : (
        <div className="space-y-2">
          {reviews.map((review: ProcessingReviewItem) => (
            <div key={review.id} className="flex flex-wrap items-center justify-between gap-2 border-2 border-foreground bg-background px-3 py-2">
              <div className="min-w-0">
                <p className="truncate text-xs font-black">{review.after?.name ?? review.before?.name ?? review.folder_id}</p>
                <PathChangePreview
                  fromPath={review.before?.path}
                  toPath={review.after?.path}
                  fromLabel="鍙樻洿鍓?
                  toLabel="鍙樻洿鍚?
                  className="mt-1"
                />
              </div>
              <div className="flex w-full flex-wrap items-center gap-2 sm:w-auto sm:justify-end">
                <StatusBadge
                  status={review.status}
                  labels={{ pending: '寰呯‘璁?, approved: '宸查€氳繃', rolled_back: '宸插洖閫€' }}
                  styles={{
                    pending: 'bg-purple-300 text-purple-900 border-2 border-foreground',
                    approved: 'bg-green-300 text-green-900 border-2 border-foreground',
                    rolled_back: 'bg-orange-300 text-orange-900 border-2 border-foreground',
                  }}
                />
                {review.status === 'pending' && (
                  <>
                    <button
                      type="button"
                      disabled={isActing}
                      onClick={(event) => {
                        event.stopPropagation()
                        void handleApproveReview(review.id)
                      }}
                      className="border-2 border-green-900 bg-green-200 px-2 py-1 text-xs font-bold text-green-900 hover:bg-green-900 hover:text-green-100 disabled:opacity-50"
                    >
                      纭閫氳繃
                    </button>
                    <button
                      type="button"
                      disabled={isActing}
                      onClick={(event) => {
                        event.stopPropagation()
                        void handleRollbackReview(review.id)
                      }}
                      className="border-2 border-red-900 bg-red-200 px-2 py-1 text-xs font-bold text-red-900 hover:bg-red-900 hover:text-red-100 disabled:opacity-50"
                    >
                      涓嶉€氳繃骞跺洖閫€
                    </button>
                  </>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  ) : null

  if (isMobile) {
    return (
      <article
        className={cn(
          'border-2 border-foreground bg-card shadow-hard',
          forceExpanded && 'bg-blue-50/60',
        )}
      >
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          className="flex w-full items-start justify-between gap-3 px-3 py-3 text-left"
        >
          <div className="min-w-0">
            <p className="font-mono text-xs font-black">鐩綍ID锛歿resolveRunFolderLabel(run)}</p>
            <p className="mt-1 text-[11px] font-bold text-muted-foreground">{formatDate(run.created_at)}</p>
          </div>
          <div className="flex items-center gap-2">
            <StatusBadge status={run.status} labels={WF_STATUS_LABELS} styles={WF_STATUS_STYLES} />
            <span className="inline-flex h-6 w-6 items-center justify-center border-2 border-foreground bg-background">
              {isExpanded ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
            </span>
          </div>
        </button>
        <div className="px-3 pb-3">
          <div className="mb-3 flex flex-wrap items-center justify-between gap-2 text-[11px] font-bold text-muted-foreground">
            <p>鑰楁椂锛?span className="text-foreground">{formatDuration(run.started_at, run.finished_at)}</span></p>
            {(run.status === 'failed' || run.status === 'partial') && (
              <button
                type="button"
                disabled={isActing}
                onClick={() => void handleRollback()}
                className="border-2 border-red-900 bg-red-200 px-2 py-1 text-[11px] font-bold text-red-900 transition-all hover:bg-red-900 hover:text-red-100 disabled:opacity-50"
              >
                {isActing ? '鍥炴粴涓?..' : '鍥炴粴'}
              </button>
            )}
          </div>
          {isExpanded && (
            <div className="space-y-3 border-t-2 border-foreground pt-3">
              {failedRunPanel}
              {reviewPanel}
              <NodeRunsPanel run={run} highlightFailedNodes={highlightFailedNodes} isMobile />
            </div>
          )}
        </div>
      </article>
    )
  }

  return (
    <>
      <tr
        className={cn(
          'cursor-pointer border-b-2 border-foreground transition-colors hover:bg-muted/20',
          forceExpanded && 'bg-blue-50/60',
        )}
        onClick={() => setExpanded((v) => !v)}
      >
        <td className="py-3 pl-4 pr-3">
          <div className="flex items-center justify-center w-6 h-6 border-2 border-foreground bg-background">
            {isExpanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
          </div>
        </td>
        <td className="py-3 pr-4 font-mono text-xs font-bold">{resolveRunFolderLabel(run)}</td>
        <td className="py-3 pr-4">
          <StatusBadge status={run.status} labels={WF_STATUS_LABELS} styles={WF_STATUS_STYLES} />
        </td>
        <td className="py-3 pr-4 text-xs font-mono font-bold text-muted-foreground">{formatDate(run.created_at)}</td>
        <td className="py-3" onClick={(event) => event.stopPropagation()}>
          {(run.status === 'failed' || run.status === 'partial') && (
            <button
              type="button"
              disabled={isActing}
              onClick={() => void handleRollback()}
              className="border-2 border-red-900 bg-red-200 px-3 py-1 text-xs font-bold text-red-900 transition-all hover:bg-red-900 hover:text-red-100 disabled:opacity-50"
            >
              {isActing ? '鍥炴粴涓?..' : '鍥炴粴'}
            </button>
          )}
        </td>
      </tr>
      {isExpanded && (
        <tr className="border-b-2 border-foreground bg-muted/10">
          <td colSpan={5} className="px-6 py-4 space-y-4">
            {failedRunPanel}
            {reviewPanel}
            <NodeRunsPanel run={run} highlightFailedNodes={highlightFailedNodes} />
          </td>
        </tr>
      )}
    </>
  )
}

function WorkflowRunsPanel({
  job,
  targetWorkflowRunId,
  isMobile = false,
}: {
  job: Job
  targetWorkflowRunId?: string
  isMobile?: boolean
}) {
  const {
    runsByJobId,
    runsTotalByJobId,
    runsPageByJobId,
    fetchRunsForJob,
    fetchingJobIds,
  } = useWorkflowRunStore()
  const [page, setPage] = useState(1)
  const runs = runsByJobId[job.id] ?? []
  const total = runsTotalByJobId[job.id] ?? 0
  const currentPage = runsPageByJobId[job.id] ?? page
  const isLoading = fetchingJobIds.has(job.id)
  const totalPages = Math.max(1, Math.ceil(total / WORKFLOW_RUN_PAGE_SIZE))

  useEffect(() => {
    void fetchRunsForJob(job.id, { page, limit: WORKFLOW_RUN_PAGE_SIZE })
  }, [fetchRunsForJob, job.id, page])

  return (
    <div className="space-y-3">
      {(job.status === 'failed' || job.status === 'partial') && job.error && (
        <div className="rounded border-2 border-red-900 bg-red-50 p-3">
          <p className="text-xs font-bold text-red-900 break-all">{job.error}</p>
          <Link
            to={buildFailedAuditLogsLink({ job_id: job.id })}
            className="mt-2 inline-flex border-2 border-red-900 bg-white px-2 py-1 text-[10px] font-bold text-red-900 hover:bg-red-900 hover:text-white"
          >
            鏌ョ湅瀹¤鏃ュ織
          </Link>
        </div>
      )}
      {runs.length === 0 ? (
        <p className="text-xs font-bold text-muted-foreground py-4 text-center">鏆傛棤宸ヤ綔娴佽繍琛岃褰?/p>
      ) : isMobile ? (
        <div className="space-y-2">
          {runs.map((run) => (
            <WorkflowRunRow
              key={run.id}
              run={run}
              isMobile
              onRefreshRuns={() => fetchRunsForJob(job.id, { page, limit: WORKFLOW_RUN_PAGE_SIZE })}
              forceExpanded={targetWorkflowRunId !== '' && run.id === targetWorkflowRunId}
              highlightFailedNodes={targetWorkflowRunId !== '' && run.id === targetWorkflowRunId}
            />
          ))}
        </div>
      ) : (
        <div className="border-2 border-foreground bg-card shadow-hard">
          <div className="bg-muted/30 px-4 py-2 border-b-2 border-foreground">
            <p className="text-xs font-black tracking-widest">WORKFLOW RUNS ({runs.length})</p>
          </div>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b-2 border-foreground bg-muted/10">
                <th className="w-12" />
                <th className="py-2 pr-4 text-left font-black tracking-widest">鐩綍ID</th>
                <th className="py-2 pr-4 text-left font-black tracking-widest">鐘舵€?/th>
                <th className="py-2 pr-4 text-left font-black tracking-widest">鍒涘缓鏃堕棿</th>
                <th className="py-2 text-left font-black tracking-widest">鎿嶄綔</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <WorkflowRunRow
                  key={run.id}
                  run={run}
                  onRefreshRuns={() => fetchRunsForJob(job.id, { page, limit: WORKFLOW_RUN_PAGE_SIZE })}
                  forceExpanded={targetWorkflowRunId !== '' && run.id === targetWorkflowRunId}
                  highlightFailedNodes={targetWorkflowRunId !== '' && run.id === targetWorkflowRunId}
                />
              ))}
            </tbody>
          </table>
        </div>
      )}
      {total > 0 && (
        <PaginationControls
          page={currentPage}
          totalPages={totalPages}
          total={total}
          rowCount={runs.length}
          isLoading={isLoading}
          onPageChange={setPage}
        />
      )}
    </div>
  )
}

function JobRow({
  job,
  workflowName,
  categoryLabel,
  forceExpanded,
  targetWorkflowRunId,
  isMobile = false,
}: {
  job: Job
  workflowName: string
  categoryLabel: string
  forceExpanded?: boolean
  targetWorkflowRunId?: string
  isMobile?: boolean
}) {
  const [expanded, setExpanded] = useState(false)
  const isExpanded = !!forceExpanded || expanded

  if (isMobile) {
    return (
      <article
        className={cn(
          'border-2 border-foreground bg-card shadow-hard',
          forceExpanded && 'bg-blue-50/60',
        )}
      >
        <button
          type="button"
          onClick={() => setExpanded((v) => !v)}
          className="flex w-full items-start justify-between gap-3 px-4 py-3 text-left"
        >
          <div className="min-w-0">
            <p className="break-all text-sm font-black">{workflowName}</p>
            <div className="mt-1 flex flex-wrap items-center gap-2 text-[10px] font-bold text-muted-foreground">
              <span className="inline-flex items-center border-2 border-foreground/70 bg-muted px-1.5 py-0.5">
                {categoryLabel}
              </span>
              <span className="font-mono">{job.id.slice(0, 8)}</span>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <StatusBadge status={job.status} labels={JOB_STATUS_LABELS} styles={JOB_STATUS_STYLES} />
            <span className="inline-flex h-6 w-6 items-center justify-center border-2 border-foreground bg-background">
              {isExpanded ? <ChevronDown className="h-3.5 w-3.5" /> : <ChevronRight className="h-3.5 w-3.5" />}
            </span>
          </div>
        </button>
        <div className="grid grid-cols-2 gap-x-3 gap-y-2 px-4 pb-3 text-[11px] font-bold text-muted-foreground">
          <p>鐩綍鏁帮細<span className="text-foreground">{summarizeJobTargets(job)}</span></p>
          <p>鑰楁椂锛?span className="text-foreground">{formatDuration(job.started_at, job.finished_at)}</span></p>
          <p className="col-span-2">鍒涘缓锛?span className="font-mono text-foreground">{formatDate(job.created_at)}</span></p>
          <div className="col-span-2">
            <ProgressBar done={job.done} total={job.total} />
          </div>
        </div>
        {isExpanded && (
          <div className="border-t-2 border-foreground px-4 py-3">
            <WorkflowRunsPanel job={job} targetWorkflowRunId={targetWorkflowRunId} isMobile />
          </div>
        )}
      </article>
    )
  }

  return (
    <>
      <tr
        className={cn(
          'cursor-pointer border-b-2 border-foreground transition-colors hover:bg-muted/30',
          forceExpanded && 'bg-blue-50/60',
        )}
        onClick={() => setExpanded((v) => !v)}
      >
        <td className="px-4 py-4">
          <div className="flex items-center justify-center w-6 h-6 border-2 border-foreground bg-background">
            {isExpanded ? <ChevronDown className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
          </div>
        </td>
        <td className="px-4 py-4 font-mono text-xs font-bold">{job.id.slice(0, 8)}</td>
        <td className="px-4 py-4">
          <div className="flex flex-col gap-1">
            <span className="text-sm font-black">{workflowName}</span>
            <span className="inline-flex w-fit items-center border-2 border-foreground/70 bg-muted px-1.5 py-0.5 text-[10px] font-bold text-muted-foreground">
              {categoryLabel}
            </span>
          </div>
        </td>
        <td className="px-4 py-4">
          <StatusBadge status={job.status} labels={JOB_STATUS_LABELS} styles={JOB_STATUS_STYLES} />
        </td>
        <td className="w-48 px-4 py-4">
          <ProgressBar done={job.done} total={job.total} />
        </td>
        <td className="px-4 py-4 text-sm font-black">{summarizeJobTargets(job)}</td>
        <td className="px-4 py-4 text-xs font-mono font-bold text-muted-foreground">{formatDate(job.created_at)}</td>
        <td className="px-4 py-4 text-xs font-mono font-bold text-muted-foreground">{formatDuration(job.started_at, job.finished_at)}</td>
      </tr>
      {isExpanded && (
        <tr className="border-b-2 border-foreground bg-muted/10">
          <td colSpan={8} className="px-8 py-6">
            <WorkflowRunsPanel job={job} targetWorkflowRunId={targetWorkflowRunId} />
          </td>
        </tr>
      )}
    </>
  )
}

export default function JobHistoryPage() {
  const isMobile = useIsMobile(1024)
  const { jobs, total, isLoading, error, fetchJobs } = useJobStore()
  const { defs, fetchDefs } = useWorkflowDefStore()
  const runsByJobId = useWorkflowRunStore((state) => state.runsByJobId)
  const [searchParams] = useSearchParams()
  const [page, setPage] = useState(1)

  const targetJobId = readTargetParam(searchParams.get('job_id'))
  const targetWorkflowRunId = readTargetParam(searchParams.get('workflow_run_id'))
  const totalPages = Math.max(1, Math.ceil(total / JOB_HISTORY_PAGE_SIZE))

  useEffect(() => {
    if (targetJobId) {
      void fetchJobs({ page: 1, limit: 100 })
      return
    }
    void fetchJobs({ page, limit: JOB_HISTORY_PAGE_SIZE })
  }, [fetchJobs, page, targetJobId])

  useEffect(() => {
    void fetchDefs()
  }, [fetchDefs])

  const workflowNameMap = useMemo(
    () => Object.fromEntries(defs.map((def) => [def.id, def.name])),
    [defs],
  )

  const targetNotFoundMessage = useMemo(() => {
    if (!targetJobId || isLoading) return null

    const targetJob = jobs.find((job) => job.id === targetJobId)
    if (!targetJob) return `鏈壘鍒颁换鍔¤褰曪細${targetJobId}`

    if (!targetWorkflowRunId) return null
    if (!(targetJobId in runsByJobId)) return null

    const targetRun = (runsByJobId[targetJobId] ?? []).find((run) => run.id === targetWorkflowRunId)
    if (!targetRun) return `浠诲姟宸插畾浣嶏紝浣嗘湭鎵惧埌瀵瑰簲宸ヤ綔娴佽繍琛岋細${targetWorkflowRunId}`

    return null
  }, [jobs, isLoading, runsByJobId, targetJobId, targetWorkflowRunId])

  return (
    <div className="flex flex-col gap-8 p-6">
      <div className="flex items-end justify-between border-b-2 border-foreground pb-4">
        <div>
          <h1 className="text-3xl font-black tracking-tight uppercase">鎵ц鍘嗗彶</h1>
          <p className="mt-1 text-sm font-bold text-muted-foreground">浠诲姟銆佸伐浣滄祦杩愯銆佽妭鐐规墽琛屼笁绾у巻鍙蹭笌澶辫触鍘熷洜銆?/p>
        </div>
      </div>

      {error && (
        <div className="border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900 shadow-hard">{error}</div>
      )}

      {targetNotFoundMessage && (
        <div className="border-2 border-amber-900 bg-amber-100 px-4 py-3 text-sm font-bold text-amber-900 shadow-hard">
          {targetNotFoundMessage}
        </div>
      )}

      <div className="space-y-4">
        <div>
          <h2 className="text-xl font-black tracking-tight">浠诲姟鍘嗗彶</h2>
          <p className="mt-1 text-sm font-medium text-muted-foreground">鍏?<span className="text-foreground font-bold">{total}</span> 鏉′换鍔¤褰曘€?/p>
        </div>
        {isMobile ? (
          <div className="space-y-3">
            {isLoading && jobs.length === 0 ? (
              <div className="border-2 border-foreground bg-card px-4 py-16 text-center font-bold text-muted-foreground shadow-hard">
                姝ｅ湪鍔犺浇浠诲姟...
              </div>
            ) : jobs.length === 0 ? (
              <div className="border-2 border-dashed border-foreground bg-card px-4 py-16 text-center font-bold text-muted-foreground shadow-hard">
                鏆傛棤浠诲姟璁板綍銆?              </div>
            ) : (
              jobs.map((job) => (
                <JobRow
                  key={job.id}
                  job={job}
                  isMobile
                  workflowName={resolveWorkflowName(job, workflowNameMap)}
                  categoryLabel={resolveJobCategoryLabel(job.type)}
                  forceExpanded={targetJobId !== '' && job.id === targetJobId}
                  targetWorkflowRunId={targetJobId === job.id ? targetWorkflowRunId : ''}
                />
              ))
            )}
          </div>
        ) : (
          <div className="overflow-hidden border-2 border-foreground bg-card shadow-hard">
            <table className="w-full">
              <thead>
                <tr className="border-b-2 border-foreground bg-muted/50">
                  <th className="w-12 px-4 py-4" />
                  <th className="px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">ID</th>
                  <th className="px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">宸ヤ綔娴佸悕绉?/th>
                  <th className="px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">鐘舵€?/th>
                  <th className="w-48 px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">杩涘害</th>
                  <th className="px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">鐩綍鏁?/th>
                  <th className="px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">鍒涘缓鏃堕棿</th>
                  <th className="px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">鑰楁椂</th>
                </tr>
              </thead>
              <tbody>
                {isLoading && jobs.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="px-4 py-16 text-center font-bold text-muted-foreground">姝ｅ湪鍔犺浇浠诲姟...</td>
                  </tr>
                ) : jobs.length === 0 ? (
                  <tr>
                    <td colSpan={8} className="px-4 py-16 text-center font-bold text-muted-foreground border-2 border-dashed border-foreground m-4">鏆傛棤浠诲姟璁板綍銆?/td>
                  </tr>
                ) : (
                  jobs.map((job) => (
                    <JobRow
                      key={job.id}
                      job={job}
                      workflowName={resolveWorkflowName(job, workflowNameMap)}
                      categoryLabel={resolveJobCategoryLabel(job.type)}
                      forceExpanded={targetJobId !== '' && job.id === targetJobId}
                      targetWorkflowRunId={targetJobId === job.id ? targetWorkflowRunId : ''}
                    />
                  ))
                )}
              </tbody>
            </table>
          </div>
        )}
        {!targetJobId && total > 0 && (
          <PaginationControls
            page={page}
            totalPages={totalPages}
            total={total}
            rowCount={jobs.length}
            isLoading={isLoading}
            onPageChange={setPage}
          />
        )}
      </div>
    </div>
  )
}
