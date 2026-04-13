import { useMemo, useState } from 'react'

import { cn } from '@/lib/utils'
import type { DirStepResult, ProcessingPreviewSummary } from '@/components/workflow-preview/previewTypes'
import { CATEGORY_LABELS, inferCategoryFromPath, normalizeCategory } from '@/components/workflow-preview/previewUtils'

export interface ProcessingPreviewInlineProps {
  summary: ProcessingPreviewSummary | null
  compact?: boolean
}

function normalizeStepStatus(status: string): 'ok' | 'err' {
  const normalized = status.trim().toLowerCase()
  if (normalized === 'moved' || normalized === 'succeeded' || normalized === 'success') return 'ok'
  return 'err'
}

function stepDisplayName(step: DirStepResult): string {
  const label = step.node_label.trim()
  if (label !== '') return label
  const nodeType = step.node_type.trim()
  if (nodeType !== '') return nodeType
  return '未知节点'
}

function buildStepTitle(step: DirStepResult): string {
  const status = step.status.trim() || 'unknown'
  const err = step.error?.trim() ?? ''
  if (err === '') return `${stepDisplayName(step)} · ${status}`
  return `${stepDisplayName(step)} · ${status} · ${err}`
}

export function ProcessingPreviewInline({ summary, compact = false }: ProcessingPreviewInlineProps) {
  const [expandedErrors, setExpandedErrors] = useState<Record<string, boolean>>({})

  const rows = useMemo(() => {
    if (!summary) return []
    return Array.isArray(summary.by_directory) ? summary.by_directory : []
  }, [summary])

  if (!summary) {
    return <p className="text-[10px] font-bold text-muted-foreground">无处理预览数据</p>
  }
  if (rows.length === 0) {
    return <p className="text-[10px] font-bold text-muted-foreground">无处理目录</p>
  }

  return (
    <div className={cn('space-y-2 border-2 border-foreground bg-muted/10', compact ? 'p-2' : 'p-2.5')}>
      {rows.map((dir, dirIndex) => {
        const category = normalizeCategory(inferCategoryFromPath(dir.source_path))
        return (
          <div
            key={`${dir.source_path}-${dirIndex}`}
            className={cn(
              'border-b border-dashed border-foreground/30 pb-2',
              dirIndex === rows.length - 1 ? 'border-b-0 pb-0' : '',
            )}
          >
            <div className={cn('mb-1 flex items-start gap-1 break-all font-bold', compact ? 'text-[10px]' : 'text-[11px]')}>
              <span className="shrink-0 border-2 border-foreground bg-background px-1 py-0.5">{`【${CATEGORY_LABELS[category] ?? '其他'}】`}</span>
              <span className="shrink-0 text-muted-foreground">:</span>
              <span className="min-w-0 font-mono text-muted-foreground">{dir.source_path}</span>
            </div>
            <div className="flex flex-wrap items-center gap-x-1 gap-y-1">
              {dir.steps.map((step, stepIndex) => {
                const statusKind = normalizeStepStatus(step.status)
                const key = `${dir.source_path}-${stepIndex}`
                const err = step.error?.trim() ?? ''
                const expanded = expandedErrors[key] ?? false
                return (
                  <div key={key} className="flex min-w-0 items-center gap-1">
                    <span
                      title={buildStepTitle(step)}
                      className={cn(
                        'border-2 border-foreground px-1.5 py-0.5 text-[10px] font-bold break-all',
                        statusKind === 'ok'
                          ? 'bg-green-200 text-green-900'
                          : 'bg-red-200 text-red-900',
                      )}
                    >
                      {stepDisplayName(step)}
                    </span>
                    {stepIndex < dir.steps.length - 1 && (
                      <span className="text-[10px] font-bold text-muted-foreground">——</span>
                    )}
                    {err !== '' && (
                      <button
                        type="button"
                        onClick={() => {
                          setExpandedErrors((current) => ({ ...current, [key]: !expanded }))
                        }}
                        className="border-2 border-red-900 bg-red-100 px-1 py-0.5 text-[10px] font-bold text-red-900 transition-colors hover:bg-red-900 hover:text-red-100"
                      >
                        {expanded ? '收起错误' : '查看错误'}
                      </button>
                    )}
                  </div>
                )
              })}
            </div>
            {dir.steps.map((step, stepIndex) => {
              const err = step.error?.trim() ?? ''
              if (err === '') return null
              const key = `${dir.source_path}-${stepIndex}`
              const expanded = expandedErrors[key] ?? false
              return (
                <p
                  key={`err-${key}`}
                  className={cn(
                    'mt-1 border-2 border-red-900 bg-red-50 px-2 py-1 font-mono text-[10px] font-bold text-red-900',
                    expanded ? 'max-h-none overflow-visible whitespace-pre-wrap break-all' : 'max-h-10 overflow-hidden whitespace-pre-wrap break-all',
                  )}
                >
                  {err}
                </p>
              )
            })}
          </div>
        )
      })}
    </div>
  )
}

