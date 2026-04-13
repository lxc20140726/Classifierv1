import { cn } from '@/lib/utils'
import type { WorkflowRunStatus } from '@/types'
import type { WorkflowRunCardView } from '@/store/workflowRunStore'

export interface WorkflowRunStatusCardProps {
  title?: string
  view: WorkflowRunCardView | null
  className?: string
  onOpenJobs?: () => void
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
  title = '当前运行卡片',
  view,
  className,
  onOpenJobs,
}: WorkflowRunStatusCardProps) {
  if (!view) return null

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
              {view.currentNodeId || '—'}
              {view.currentNodeType ? ` (${view.currentNodeType})` : ''}
            </span>
          </div>

          <div className="flex items-start justify-between gap-3">
            <span className="font-black text-muted-foreground">完成进度</span>
            <span className="font-black tabular-nums">
              {view.completedNodes} / {view.totalNodes > 0 ? view.totalNodes : '—'}
            </span>
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
            </>
          )}

          {view.failureSummary && (
            <div className="rounded border-2 border-red-900 bg-red-50 px-2 py-1 text-[11px] font-bold text-red-900 break-all">
              {view.failureSummary}
            </div>
          )}

          <p className="font-mono text-[10px] font-bold text-muted-foreground">
            job={view.jobId} run={view.workflowRunId || '—'}
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
