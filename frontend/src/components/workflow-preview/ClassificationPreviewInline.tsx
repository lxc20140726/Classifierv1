import { Fragment } from 'react'

import { cn } from '@/lib/utils'
import type { ClassificationPreviewSummary, EntryPreviewItem } from '@/components/workflow-preview/previewTypes'
import { CATEGORY_LABELS, normalizeCategory } from '@/components/workflow-preview/previewUtils'

export interface ClassificationPreviewInlineProps {
  summary: ClassificationPreviewSummary | null
  compact?: boolean
}

const CATEGORY_TAG_CLS: Record<string, string> = {
  video: 'bg-blue-100 text-blue-900',
  manga: 'bg-purple-100 text-purple-900',
  photo: 'bg-green-100 text-green-900',
  mixed: 'bg-amber-100 text-amber-900',
  other: 'bg-slate-200 text-slate-900',
}

const DEPTH_PADDING_CLS: Record<number, string> = {
  0: 'pl-0',
  1: 'pl-[14px]',
  2: 'pl-[28px]',
  3: 'pl-[42px]',
  4: 'pl-[56px]',
  5: 'pl-[70px]',
}

const DEPTH_LINE_CLS: Record<number, string> = {
  1: 'left-[6px]',
  2: 'left-[20px]',
  3: 'left-[34px]',
  4: 'left-[48px]',
  5: 'left-[62px]',
}

interface EntryTreeRowsProps {
  entries: EntryPreviewItem[]
  depth: number
  compact: boolean
}

function EntryTreeRows({ entries, depth, compact }: EntryTreeRowsProps) {
  if (entries.length === 0) {
    return null
  }

  return (
    <>
      {entries.map((entry) => {
        const category = normalizeCategory(entry.category)
        const confidence = Number.isFinite(entry.confidence) ? entry.confidence.toFixed(2) : '0.00'
        return (
          <Fragment key={`${entry.path}-${entry.name}-${depth}`}>
            <div
              className={cn(
                'grid grid-cols-[1fr_auto] items-start gap-2 border-b border-dashed border-foreground/20 py-1.5',
                compact ? 'text-[10px]' : 'text-[11px]',
              )}
            >
              <div className={cn('relative min-w-0 break-all', DEPTH_PADDING_CLS[Math.min(depth, 5)] ?? DEPTH_PADDING_CLS[5])}>
                {depth > 0 && (
                  <span
                    className={cn(
                      'pointer-events-none absolute bottom-0 top-0 border-l border-dashed border-foreground/35',
                      DEPTH_LINE_CLS[Math.min(depth, 5)] ?? DEPTH_LINE_CLS[5],
                    )}
                  />
                )}
                <span className="font-bold text-foreground">{entry.name || entry.path}</span>
                {entry.path && entry.name && (
                  <p className="font-mono text-[10px] text-muted-foreground break-all">{entry.path}</p>
                )}
              </div>
              <span
                className={cn(
                  'shrink-0 border-2 border-foreground px-1.5 py-0.5 font-bold tabular-nums',
                  CATEGORY_TAG_CLS[category] ?? CATEGORY_TAG_CLS.other,
                )}
              >
                {CATEGORY_LABELS[category] ?? category} {confidence}
              </span>
            </div>
            <EntryTreeRows entries={entry.subdirs ?? []} depth={depth + 1} compact={compact} />
          </Fragment>
        )
      })}
    </>
  )
}

export function ClassificationPreviewInline({ summary, compact = false }: ClassificationPreviewInlineProps) {
  if (!summary) {
    return <p className="text-[10px] font-bold text-muted-foreground">无分类预览数据</p>
  }

  const entries = Array.isArray(summary.entries) ? summary.entries : []
  if (entries.length === 0) {
    return <p className="text-[10px] font-bold text-muted-foreground">无分类条目</p>
  }

  return (
    <div className={cn('border-2 border-foreground bg-muted/10', compact ? 'p-2' : 'p-2.5')}>
      <div className={cn('max-h-56 overflow-y-auto', compact ? 'space-y-0' : 'space-y-0.5')}>
        <EntryTreeRows entries={entries} depth={0} compact={compact} />
      </div>
    </div>
  )
}

