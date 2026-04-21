import { useMemo, useState } from 'react'
import { Check, ChevronDown, ChevronUp, AlertCircle } from 'lucide-react'
import cronstrue from 'cronstrue'
import 'cronstrue/locales/zh_CN.js'
import { CronExpressionParser } from 'cron-parser'

import { cn } from '@/lib/utils'

export interface CronExpressionFieldProps {
  value: string
  onChange: (value: string) => void
  className?: string
}

interface Preset {
  label: string
  value: string
}

const PRESETS: Preset[] = [
  { label: '每分钟', value: '* * * * *' },
  { label: '每小时', value: '0 * * * *' },
  { label: '每天 0 点', value: '0 0 * * *' },
  { label: '每天 9 点', value: '0 9 * * *' },
  { label: '每周一', value: '0 9 * * 1' },
  { label: '每月 1 日', value: '0 9 1 * *' },
]

const NEXT_RUNS_COUNT = 3

function describeExpression(expr: string): { text: string; error: boolean } {
  const trimmed = expr.trim()
  if (!trimmed) return { text: '', error: false }
  try {
    const text = cronstrue.toString(trimmed, {
      locale: 'zh_CN',
      use24HourTimeFormat: true,
      throwExceptionOnParseError: true,
    })
    return { text, error: false }
  } catch {
    return { text: '无效的 cron 表达式', error: true }
  }
}

function getNextRuns(expr: string): Date[] {
  const trimmed = expr.trim()
  if (!trimmed) return []
  try {
    const interval = CronExpressionParser.parse(trimmed)
    const results: Date[] = []
    for (let i = 0; i < NEXT_RUNS_COUNT; i++) {
      results.push(interval.next().toDate())
    }
    return results
  } catch {
    return []
  }
}

export function CronExpressionField({ value, onChange, className }: CronExpressionFieldProps) {
  const [showPresets, setShowPresets] = useState(false)

  const description = useMemo(() => describeExpression(value), [value])
  const nextRuns = useMemo(() => getNextRuns(value), [value])

  const isValid = value.trim() === '' || !description.error
  const hasValue = value.trim() !== ''

  return (
    <div className={cn('space-y-2', className)}>
      {/* Input */}
      <div className="relative">
        <input
          value={value}
          onChange={(e) => onChange(e.target.value)}
          placeholder="0 * * * *"
          spellCheck={false}
          className={cn(
            'w-full border-2 bg-muted/30 px-4 py-3 font-mono text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background',
            isValid ? 'border-foreground' : 'border-red-700',
          )}
        />
      </div>

      {/* Description */}
      {hasValue && (
        <div
          className={cn(
            'flex items-start gap-2 border-2 px-3 py-2 text-xs font-bold',
            description.error
              ? 'border-red-700 bg-red-50 text-red-800'
              : 'border-foreground bg-muted/20 text-foreground',
          )}
        >
          {description.error ? (
            <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0 text-red-700" />
          ) : (
            <Check className="mt-0.5 h-3.5 w-3.5 shrink-0 text-foreground" />
          )}
          <span>{description.text}</span>
        </div>
      )}

      {/* Next runs */}
      {hasValue && !description.error && nextRuns.length > 0 && (
        <div className="border-2 border-foreground bg-background px-3 py-2">
          <p className="mb-1.5 text-xs font-black tracking-widest text-muted-foreground">下次执行时间</p>
          <ul className="space-y-0.5">
            {nextRuns.map((date, i) => (
              <li key={i} className="font-mono text-xs font-bold text-foreground">
                {new Date(date).toLocaleString('zh-CN', {
                  year: 'numeric',
                  month: '2-digit',
                  day: '2-digit',
                  hour: '2-digit',
                  minute: '2-digit',
                  hour12: false,
                })}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Presets toggle */}
      <button
        type="button"
        onClick={() => setShowPresets((prev) => !prev)}
        className="flex items-center gap-1 text-xs font-bold text-muted-foreground transition-colors hover:text-foreground"
      >
        {showPresets ? (
          <ChevronUp className="h-3.5 w-3.5" />
        ) : (
          <ChevronDown className="h-3.5 w-3.5" />
        )}
        快速选择
      </button>

      {/* Presets grid */}
      {showPresets && (
        <div className="grid grid-cols-1 gap-1.5 sm:grid-cols-2 lg:grid-cols-3">
          {PRESETS.map((preset) => (
            <button
              key={preset.value}
              type="button"
              onClick={() => {
                onChange(preset.value)
                setShowPresets(false)
              }}
              className={cn(
                'border-2 px-2 py-1.5 text-xs font-bold transition-all hover:-translate-y-0.5 hover:shadow-hard',
                value === preset.value
                  ? 'border-foreground bg-foreground text-background'
                  : 'border-foreground bg-background text-foreground hover:bg-foreground hover:text-background',
              )}
            >
              {preset.label}
              <span className="mt-0.5 block font-mono text-[10px] opacity-60">{preset.value}</span>
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
