import { useEffect, useMemo, useState } from 'react'
import { ChevronDown, ChevronRight } from 'lucide-react'
import { Link, useSearchParams } from 'react-router-dom'

import { ClassificationPreviewInline } from '@/components/workflow-preview/ClassificationPreviewInline'
import { PathChangePreview } from '@/components/PathChangePreview'
import { ProcessingPreviewInline } from '@/components/workflow-preview/ProcessingPreviewInline'
import { isClassificationSummary, isProcessingSummary, parseNodePreviewSummary } from '@/components/workflow-preview/previewUtils'
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
  if (!dateStr) return '—'
  return new Date(dateStr).toLocaleString('zh-CN')
}

function formatDuration(startedAt: string | null | undefined, finishedAt: string | null | undefined) {
  if (!startedAt) return '—'
  const end = finishedAt ? new Date(finishedAt) : new Date()
  const start = new Date(startedAt)
  const diffMs = Math.max(0, end.getTime() - start.getTime())
  if (diffMs < 1000) return '<1 秒'
  const secs = Math.floor(diffMs / 1000)
  if (secs < 60) return `${secs} 秒`
  if (secs < 3600) return `${Math.floor(secs / 60)} 分 ${secs % 60} 秒`
  return `${Math.floor(secs / 3600)} 小时 ${Math.floor((secs % 3600) / 60)} 分`
}

function buildFailedAuditLogsLink(params: Record<string, string>) {
  const search = new URLSearchParams({ ...params, result: 'failed' })
  return `/audit-logs?${search.toString()}`
}

function readTargetParam(value: string | null) {
  return value?.trim() ?? ''
}

function resolveWorkflowName(
  job: Job,
  workflowNameMap: Record<string, string>,
) {
  const workflowDefId = (job.workflow_def_id ?? '').trim()
  if (job.type === 'scan') return '扫描任务'
  if (workflowDefId !== '') {
    const matched = workflowNameMap[workflowDefId]
    if (matched && matched.trim() !== '') return matched
    return `已删除工作流（${workflowDefId}）`
  }
  if (job.type === 'workflow') return '已删除工作流（缺少定义ID）'
  return '未知工作流'
}

function resolveJobCategoryLabel(jobType: string) {
  if (jobType === 'scan') return '扫描'
  if (jobType === 'workflow') return '工作流'
  return `其他(${jobType})`
}

const JOB_STATUS_LABELS: Record<JobStatus, string> = {
  pending: '等待中',
  running: '进行中',
  succeeded: '已完成',
  failed: '失败',
  partial: '部分完成',
  cancelled: '已取消',
  waiting_input: '待确认',
  rolled_back: '已回退',
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
  pending: '等待中',
  running: '进行中',
  succeeded: '已完成',
  failed: '失败',
  partial: '部分完成',
  waiting_input: '待确认',
  rolled_back: '已回退',
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
  pending: '等待中',
  running: '进行中',
  succeeded: '已完成',
  failed: '失败',
  skipped: '已跳过',
  waiting_input: '待确认',
}

const NODE_STATUS_STYLES: Record<NodeRunStatus, string> = {
  pending: 'bg-gray-200 text-gray-900 border-2 border-foreground',
  running: 'bg-blue-300 text-blue-900 border-2 border-foreground',
  succeeded: 'bg-green-300 text-green-900 border-2 border-foreground',
  failed: 'bg-red-300 text-red-900 border-2 border-foreground',
  skipped: 'bg-gray-300 text-gray-900 border-2 border-foreground',
  waiting_input: 'bg-purple-300 text-purple-900 border-2 border-foreground',
}

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

  return <span className="text-[10px] font-bold text-muted-foreground">—</span>
}

function NodeRunsPanel({ run, highlightFailedNodes }: { run: WorkflowRun; highlightFailedNodes?: boolean }) {
  const { nodesByRunId, fetchRunDetail } = useWorkflowRunStore()
  const nodes = nodesByRunId[run.id] ?? []

  useEffect(() => {
    void fetchRunDetail(run.id)
  }, [run.id, fetchRunDetail])

  if (nodes.length === 0) {
    return <p className="py-4 text-xs font-bold text-muted-foreground text-center">暂无节点记录</p>
  }

  return (
    <div className="pl-4 py-2">
      <table className="w-full text-xs">
        <thead>
          <tr className="border-b-2 border-foreground bg-muted/30">
            <th className="py-2 pr-4 text-left font-black tracking-widest">节点ID</th>
            <th className="py-2 pr-4 text-left font-black tracking-widest">类型</th>
            <th className="py-2 pr-4 text-left font-black tracking-widest">序号</th>
            <th className="py-2 pr-4 text-left font-black tracking-widest">状态</th>
            <th className="py-2 text-left font-black tracking-widest">耗时</th>
            <th className="py-2 text-left font-black tracking-widest">结果预览</th>
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
                      查看审计日志
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
}: {
  run: WorkflowRun
  forceExpanded?: boolean
  highlightFailedNodes?: boolean
}) {
  const [expanded, setExpanded] = useState(false)
  const [isActing, setIsActing] = useState(false)
  const isExpanded = !!forceExpanded || expanded
  const {
    rollbackRun,
    fetchRunDetail,
    fetchRunsForJob,
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

  async function handleRollback() {
    setIsActing(true)
    try {
      await rollbackRun(run.id)
      await fetchRunsForJob(run.job_id)
      pushNotification({
        level: 'success',
        title: '回滚完成',
        message: `工作流运行 ${run.id.slice(0, 8)} 已回退。`,
        jobId: run.job_id,
      })
    } catch (rollbackError) {
      const message = rollbackError instanceof Error ? rollbackError.message : '回滚失败'
      pushNotification({ level: 'error', title: '回滚失败', message, jobId: run.job_id })
    } finally {
      setIsActing(false)
    }
  }

  async function handleApproveReview(reviewId: string) {
    setIsActing(true)
    try {
      await approveReview(run.id, reviewId)
      await fetchRunsForJob(run.job_id)
      pushNotification({
        level: 'success',
        title: '确认已通过',
        message: `确认项 ${reviewId.slice(0, 8)} 已标记为通过。`,
        jobId: run.job_id,
      })
    } catch (approveError) {
      const message = approveError instanceof Error ? approveError.message : '确认通过失败'
      pushNotification({
        level: 'error',
        title: '确认通过失败',
        message,
        jobId: run.job_id,
      })
    } finally {
      setIsActing(false)
    }
  }

  async function handleRollbackReview(reviewId: string) {
    setIsActing(true)
    try {
      await rollbackReview(run.id, reviewId)
      await fetchRunsForJob(run.job_id)
      pushNotification({
        level: 'success',
        title: '已回退确认项',
        message: `确认项 ${reviewId.slice(0, 8)} 已执行回退。`,
        jobId: run.job_id,
      })
    } catch (rollbackError) {
      const message = rollbackError instanceof Error ? rollbackError.message : '回退确认项失败'
      pushNotification({
        level: 'error',
        title: '回退确认项失败',
        message,
        jobId: run.job_id,
      })
    } finally {
      setIsActing(false)
    }
  }

  async function handleApproveAllPendingReviews() {
    setIsActing(true)
    try {
      const approved = await approveAllPendingReviews(run.id)
      await fetchRunsForJob(run.job_id)
      pushNotification({
        level: 'success',
        title: '批量通过完成',
        message: `已批量通过 ${approved} 个确认项。`,
        jobId: run.job_id,
      })
    } catch (approveError) {
      const message = approveError instanceof Error ? approveError.message : '批量确认通过失败'
      pushNotification({
        level: 'error',
        title: '批量确认通过失败',
        message,
        jobId: run.job_id,
      })
    } finally {
      setIsActing(false)
    }
  }

  async function handleRollbackAllPendingReviews() {
    setIsActing(true)
    try {
      const rolledBack = await rollbackAllPendingReviews(run.id)
      await fetchRunsForJob(run.job_id)
      pushNotification({
        level: 'success',
        title: '批量回退完成',
        message: `已批量回退 ${rolledBack} 个确认项。`,
        jobId: run.job_id,
      })
    } catch (rollbackError) {
      const message = rollbackError instanceof Error ? rollbackError.message : '批量回退失败'
      pushNotification({
        level: 'error',
        title: '批量回退失败',
        message,
        jobId: run.job_id,
      })
    } finally {
      setIsActing(false)
    }
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
        <td className="py-3 pr-4 font-mono text-xs font-bold">{run.folder_id.slice(0, 8)}</td>
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
              {isActing ? '回滚中...' : '回滚'}
            </button>
          )}
        </td>
      </tr>
      {isExpanded && (
        <tr className="border-b-2 border-foreground bg-muted/10">
          <td colSpan={5} className="px-6 py-4 space-y-4">
            {(run.status === 'failed' || run.status === 'partial') && run.error && (
              <div className="rounded border-2 border-red-900 bg-red-50 p-3">
                <p className="text-xs font-bold text-red-900 break-all">{run.error}</p>
                <Link
                  to={buildFailedAuditLogsLink({ job_id: run.job_id, workflow_run_id: run.id })}
                  className="mt-2 inline-flex border-2 border-red-900 bg-white px-2 py-1 text-[10px] font-bold text-red-900 hover:bg-red-900 hover:text-white"
                >
                  查看审计日志
                </Link>
              </div>
            )}
            {run.status === 'waiting_input' && (
              <div className="space-y-3 border-2 border-purple-900 bg-purple-50 p-3">
                <div className="flex items-center justify-between">
                  <h4 className="text-sm font-black text-purple-900">目录确认面板</h4>
                  {reviewSummary && (
                    <p className="text-xs font-bold text-purple-900">
                      待确认 {reviewSummary.pending} / 总数 {reviewSummary.total}（通过 {reviewSummary.approved}，回退 {reviewSummary.rolled_back}）
                    </p>
                  )}
                  {(reviewSummary?.pending ?? 0) > 0 && (
                    <div className="ml-3 flex items-center gap-2">
                      <button
                        type="button"
                        disabled={isActing}
                        onClick={(event) => {
                          event.stopPropagation()
                          void handleApproveAllPendingReviews()
                        }}
                        className="border-2 border-green-900 bg-green-200 px-2 py-1 text-xs font-bold text-green-900 hover:bg-green-900 hover:text-green-100 disabled:opacity-50"
                      >
                        全部确认通过
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
                        全部不通过并回退
                      </button>
                    </div>
                  )}
                </div>
                {reviews.length === 0 ? (
                  <p className="text-xs font-bold text-muted-foreground">暂无确认项</p>
                ) : (
                  <div className="space-y-2">
                    {reviews.map((review: ProcessingReviewItem) => (
                      <div key={review.id} className="flex items-center justify-between border-2 border-foreground bg-background px-3 py-2">
                        <div className="min-w-0">
                          <p className="truncate text-xs font-black">{review.after?.name ?? review.before?.name ?? review.folder_id}</p>
                          <PathChangePreview
                            fromPath={review.before?.path}
                            toPath={review.after?.path}
                            fromLabel="变更前"
                            toLabel="变更后"
                            className="mt-1"
                          />
                        </div>
                        <div className="ml-3 flex items-center gap-2">
                          <StatusBadge
                            status={review.status}
                            labels={{ pending: '待确认', approved: '已通过', rolled_back: '已回退' }}
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
                                确认通过
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
                                不通过并回退
                              </button>
                            </>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}
            <NodeRunsPanel run={run} highlightFailedNodes={highlightFailedNodes} />
          </td>
        </tr>
      )}
    </>
  )
}

function WorkflowRunsPanel({ job, targetWorkflowRunId }: { job: Job; targetWorkflowRunId?: string }) {
  const { runsByJobId, fetchRunsForJob } = useWorkflowRunStore()
  const runs = runsByJobId[job.id] ?? []

  useEffect(() => {
    void fetchRunsForJob(job.id)
  }, [job.id, fetchRunsForJob])

  return (
    <div className="space-y-3">
      {(job.status === 'failed' || job.status === 'partial') && job.error && (
        <div className="rounded border-2 border-red-900 bg-red-50 p-3">
          <p className="text-xs font-bold text-red-900 break-all">{job.error}</p>
          <Link
            to={buildFailedAuditLogsLink({ job_id: job.id })}
            className="mt-2 inline-flex border-2 border-red-900 bg-white px-2 py-1 text-[10px] font-bold text-red-900 hover:bg-red-900 hover:text-white"
          >
            查看审计日志
          </Link>
        </div>
      )}
      {runs.length === 0 ? (
        <p className="text-xs font-bold text-muted-foreground py-4 text-center">暂无工作流运行记录</p>
      ) : (
        <div className="border-2 border-foreground bg-card shadow-hard">
          <div className="bg-muted/30 px-4 py-2 border-b-2 border-foreground">
            <p className="text-xs font-black tracking-widest">WORKFLOW RUNS ({runs.length})</p>
          </div>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b-2 border-foreground bg-muted/10">
                <th className="w-12" />
                <th className="py-2 pr-4 text-left font-black tracking-widest">目录ID</th>
                <th className="py-2 pr-4 text-left font-black tracking-widest">状态</th>
                <th className="py-2 pr-4 text-left font-black tracking-widest">创建时间</th>
                <th className="py-2 text-left font-black tracking-widest">操作</th>
              </tr>
            </thead>
            <tbody>
              {runs.map((run) => (
                <WorkflowRunRow
                  key={run.id}
                  run={run}
                  forceExpanded={targetWorkflowRunId !== '' && run.id === targetWorkflowRunId}
                  highlightFailedNodes={targetWorkflowRunId !== '' && run.id === targetWorkflowRunId}
                />
              ))}
            </tbody>
          </table>
        </div>
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
}: {
  job: Job
  workflowName: string
  categoryLabel: string
  forceExpanded?: boolean
  targetWorkflowRunId?: string
}) {
  const [expanded, setExpanded] = useState(false)
  const isExpanded = !!forceExpanded || expanded

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
        <td className="px-4 py-4 text-sm font-black">{(job.folder_ids ?? []).length}</td>
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
  const { jobs, total, isLoading, error, fetchJobs } = useJobStore()
  const { defs, fetchDefs } = useWorkflowDefStore()
  const runsByJobId = useWorkflowRunStore((state) => state.runsByJobId)
  const [searchParams] = useSearchParams()

  const targetJobId = readTargetParam(searchParams.get('job_id'))
  const targetWorkflowRunId = readTargetParam(searchParams.get('workflow_run_id'))

  useEffect(() => {
    void fetchJobs(targetJobId ? { page: 1, limit: 100 } : undefined)
  }, [fetchJobs, targetJobId])

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
    if (!targetJob) return `未找到任务记录：${targetJobId}`

    if (!targetWorkflowRunId) return null
    if (!(targetJobId in runsByJobId)) return null

    const targetRun = (runsByJobId[targetJobId] ?? []).find((run) => run.id === targetWorkflowRunId)
    if (!targetRun) return `任务已定位，但未找到对应工作流运行：${targetWorkflowRunId}`

    return null
  }, [jobs, isLoading, runsByJobId, targetJobId, targetWorkflowRunId])

  return (
    <div className="flex flex-col gap-8 p-6">
      <div className="flex items-end justify-between border-b-2 border-foreground pb-4">
        <div>
          <h1 className="text-3xl font-black tracking-tight uppercase">执行历史</h1>
          <p className="mt-1 text-sm font-bold text-muted-foreground">任务、工作流运行、节点执行三级历史与失败原因。</p>
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
          <h2 className="text-xl font-black tracking-tight">任务历史</h2>
          <p className="mt-1 text-sm font-medium text-muted-foreground">共 <span className="text-foreground font-bold">{total}</span> 条任务记录。</p>
        </div>
        <div className="overflow-hidden border-2 border-foreground bg-card shadow-hard">
          <table className="w-full">
            <thead>
              <tr className="border-b-2 border-foreground bg-muted/50">
                <th className="w-12 px-4 py-4" />
                <th className="px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">ID</th>
                <th className="px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">工作流名称</th>
                <th className="px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">状态</th>
                <th className="w-48 px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">进度</th>
                <th className="px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">目录数</th>
                <th className="px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">创建时间</th>
                <th className="px-4 py-4 text-left text-xs font-black uppercase tracking-widest text-foreground">耗时</th>
              </tr>
            </thead>
            <tbody>
              {isLoading && jobs.length === 0 ? (
                <tr>
                  <td colSpan={8} className="px-4 py-16 text-center font-bold text-muted-foreground">正在加载任务...</td>
                </tr>
              ) : jobs.length === 0 ? (
                <tr>
                  <td colSpan={8} className="px-4 py-16 text-center font-bold text-muted-foreground border-2 border-dashed border-foreground m-4">暂无任务记录。</td>
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
      </div>
    </div>
  )
}
