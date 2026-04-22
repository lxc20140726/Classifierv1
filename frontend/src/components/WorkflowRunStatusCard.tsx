import { cn } from '@/lib/utils'
import type { WorkflowRunCardView } from '@/store/workflowRunStore'
import type { WorkflowRunStatus } from '@/types'

export interface WorkflowRunStatusCardProps {
  title?: string
  view: WorkflowRunCardView | null
  className?: string
  onOpenJobs?: () => void
  onApproveAllPending?: () => void
  onRollbackAllPending?: () => void
  actionLoading?: boolean
}

const RUN_STATUS_LABELS: Record<WorkflowRunStatus, string> = {
  pending: '等待中',
  running: '进行中',
  succeeded: '已完成',
  failed: '失败',
  partial: '部分完成',
  waiting_input: '待确认',
  rolled_back: '已回退',
}

const RUN_STATUS_BADGE_CLS: Record<WorkflowRunStatus, string> = {
  pending: 'bg-gray-200 text-gray-900 border-2 border-foreground',
  running: 'bg-blue-300 text-blue-900 border-2 border-foreground',
  succeeded: 'bg-green-300 text-green-900 border-2 border-foreground',
  failed: 'bg-red-300 text-red-900 border-2 border-foreground',
  partial: 'bg-yellow-300 text-yellow-900 border-2 border-foreground',
  waiting_input: 'bg-purple-300 text-purple-900 border-2 border-foreground',
  rolled_back: 'bg-orange-300 text-orange-900 border-2 border-foreground',
}

export function WorkflowRunStatusCard({
  title = '最近一次运行状态',
  view,
  className,
  onOpenJobs,
  onApproveAllPending,
  onRollbackAllPending,
  actionLoading = false,
}: WorkflowRunStatusCardProps) {
  if (!view) return null

  const progressPercent = typeof view.currentNodeProgressPercent === 'number'
    ? Math.max(0, Math.min(100, view.currentNodeProgressPercent))
    : 0
  const progressPercentText = typeof view.currentNodeProgressPercent === 'number'
    ? `${Math.round(progressPercent)}%`
    : '-'

  return (
    <div className={cn('border-2 border-foreground bg-background p-3 shadow-hard', className)}>
      <div className="mb-2 flex items-center justify-between gap-3">
        <h3 className="text-sm font-black tracking-wide">{title}</h3>
        <span className={cn('px-2 py-0.5 text-xs font-black', RUN_STATUS_BADGE_CLS[view.status])}>
          {RUN_STATUS_LABELS[view.status]}
        </span>
      </div>

      {view.isBinding ? (
        <p className="text-xs font-bold text-muted-foreground">等待运行记录创建中...</p>
      ) : (
        <div className="space-y-2 text-xs">
          <div className="flex items-start justify-between gap-3">
            <span className="font-black text-muted-foreground">当前节点</span>
            <span className="text-right font-mono font-bold">
              {view.currentNodeId || '-'}
              {view.currentNodeType ? ` (${view.currentNodeType})` : ''}
            </span>
          </div>

          <div className="flex items-start justify-between gap-3">
            <span className="font-black text-muted-foreground">完成进度</span>
            <span className="font-black tabular-nums">
              {view.completedNodes} / {view.totalNodes > 0 ? view.totalNodes : '-'}
            </span>
          </div>

          <div className="space-y-1">
            <div className="flex items-start justify-between gap-3">
              <span className="font-black text-muted-foreground">节点进度</span>
              <span className="font-black tabular-nums">
                {view.currentNodeProgressDone ?? '-'} / {view.currentNodeProgressTotal ?? '-'} · {progressPercentText}
              </span>
            </div>
            <div className="h-2 w-full overflow-hidden border-2 border-foreground bg-muted">
              <svg
                className="h-full w-full"
                viewBox="0 0 100 8"
                preserveAspectRatio="none"
                role="img"
                aria-label={`节点进度 ${progressPercentText}`}
              >
                <rect x="0" y="0" width={progressPercent} height="8" className="fill-foreground transition-all duration-300" />
              </svg>
            </div>
            <div className="flex items-start justify-between gap-3 text-[11px] font-bold text-muted-foreground">
              <p className="min-w-0 break-all">{view.currentNodeProgressText}</p>
              <p className="shrink-0 tabular-nums">耗时 {view.currentNodeDurationText}</p>
            </div>
            {view.progressSourcePath && (
              <p className="truncate font-mono text-[10px] text-muted-foreground" title={view.progressSourcePath}>
                源：{view.progressSourcePath}
              </p>
            )}
            {view.progressTargetPath && (
              <p className="truncate font-mono text-[10px] text-muted-foreground" title={view.progressTargetPath}>
                目标：{view.progressTargetPath}
              </p>
            )}
          </div>

          {view.status === 'waiting_input' && view.reviewSummary && (
            <>
              <div className="flex items-start justify-between gap-3">
                <span className="font-black text-muted-foreground">确认进度</span>
                <span className="font-black tabular-nums">{view.reviewProgressText}</span>
              </div>
              <div className="rounded border-2 border-purple-900 bg-purple-50 px-2 py-1 text-[11px] font-bold text-purple-900">
                待确认 {view.reviewSummary.pending}，已通过 {view.reviewSummary.approved}，已回退 {view.reviewSummary.rolled_back}
              </div>
              {view.pendingReviewCount > 0 && onApproveAllPending && onRollbackAllPending && (
                <div className="flex flex-wrap gap-2">
                  <button
                    type="button"
                    disabled={actionLoading}
                    onClick={onApproveAllPending}
                    className="border-2 border-green-900 bg-green-200 px-2 py-1 text-[11px] font-bold text-green-900 hover:bg-green-900 hover:text-green-100 disabled:opacity-50"
                  >
                    全部确认通过
                  </button>
                  <button
                    type="button"
                    disabled={actionLoading}
                    onClick={onRollbackAllPending}
                    className="border-2 border-red-900 bg-red-200 px-2 py-1 text-[11px] font-bold text-red-900 hover:bg-red-900 hover:text-red-100 disabled:opacity-50"
                  >
                    全部不通过并回退
                  </button>
                </div>
              )}
            </>
          )}

          {view.failureSummary && (
            <div className="rounded border-2 border-red-900 bg-red-50 px-2 py-1 text-[11px] font-bold text-red-900 break-all">
              {view.failureSummary}
            </div>
          )}

          <p className="font-mono text-[10px] font-bold text-muted-foreground">
            job={view.jobId} run={view.workflowRunId || '-'}
          </p>
        </div>
      )}

      {onOpenJobs && (
        <button
          type="button"
          onClick={onOpenJobs}
          className="mt-3 border-2 border-foreground bg-background px-3 py-1.5 text-xs font-bold transition-all hover:bg-foreground hover:text-background"
        >
          查看完整明细
        </button>
      )}
    </div>
  )
}
