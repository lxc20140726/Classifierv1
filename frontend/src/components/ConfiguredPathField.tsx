import { useEffect, useMemo, useState } from 'react'
import { FolderOpen } from 'lucide-react'

import { DirPicker } from '@/components/DirPicker'
import { cn } from '@/lib/utils'
import { useConfigStore } from '@/store/configStore'

export type PathRefType = 'output' | 'custom'

export interface ConfiguredPathFieldValue {
  pathRefType: PathRefType
  pathRefKey: string
  pathSuffix: string
}

export interface ConfiguredPathFieldProps {
  value: ConfiguredPathFieldValue
  placeholder?: string
  pickerTitle?: string
  defaultOutputKey?: 'video' | 'manga' | 'photo' | 'other' | 'mixed'
  onChange: (next: ConfiguredPathFieldValue) => void
}

const OUTPUT_KEYS: Array<{ key: 'video' | 'manga' | 'photo' | 'other' | 'mixed'; label: string }> = [
  { key: 'video', label: '视频' },
  { key: 'manga', label: '漫画' },
  { key: 'photo', label: '写真' },
  { key: 'other', label: '其他' },
  { key: 'mixed', label: '混合' },
]

interface OutputOption {
  value: string
  label: string
  path: string
}

function parseOutputRefKey(raw: string): { key: string; index: number } {
  const trimmed = raw.trim()
  if (trimmed === '') {
    return { key: '', index: 0 }
  }
  const [key, indexPart] = trimmed.split(':', 2)
  if (!indexPart) {
    return { key, index: 0 }
  }
  const parsed = Number.parseInt(indexPart, 10)
  if (!Number.isInteger(parsed) || parsed < 0) {
    return { key: '', index: 0 }
  }
  return { key, index: parsed }
}

export function ConfiguredPathField({
  value,
  placeholder,
  pickerTitle,
  defaultOutputKey = 'mixed',
  onChange,
}: ConfiguredPathFieldProps) {
  const [open, setOpen] = useState(false)
  const { outputDirs, load } = useConfigStore()

  useEffect(() => {
    void load()
  }, [load])

  const outputOptions = useMemo<OutputOption[]>(() => {
    return OUTPUT_KEYS.flatMap((option) => {
      const paths = outputDirs[option.key] ?? []
      return paths.map((path, index) => ({
        value: `${option.key}:${index}`,
        label: `${option.label} / ${index + 1} - ${path}`,
        path,
      }))
    })
  }, [outputDirs])

  const resolvedOutputValue = useMemo(() => {
    const parsed = parseOutputRefKey(value.pathRefKey)
    if (parsed.key !== '' && OUTPUT_KEYS.some((item) => item.key === parsed.key)) {
      return `${parsed.key}:${parsed.index}`
    }
    return ''
  }, [value.pathRefKey])

  const resolvedOutputPath = useMemo(() => {
    if (resolvedOutputValue !== '') {
      const option = outputOptions.find((item) => item.value === resolvedOutputValue)
      if (option) {
        return option.path
      }
    }
    const parsed = parseOutputRefKey(value.pathRefKey)
    if (parsed.key === '' || !OUTPUT_KEYS.some((item) => item.key === parsed.key)) {
      return ''
    }
    const paths = outputDirs[parsed.key as keyof typeof outputDirs] ?? []
    return paths[parsed.index] ?? ''
  }, [outputDirs, outputOptions, resolvedOutputValue, value.pathRefKey])

  return (
    <div className="space-y-2">
      <div className="grid grid-cols-2 gap-2">
        {(['output', 'custom'] as const).map((mode) => (
          <button
            key={mode}
            type="button"
            onClick={() => {
              if (mode === 'output') {
                onChange({ pathRefType: 'output', pathRefKey: `${defaultOutputKey}:0`, pathSuffix: value.pathSuffix })
                return
              }
              onChange({ pathRefType: 'custom', pathRefKey: value.pathRefKey, pathSuffix: value.pathSuffix })
            }}
            className={cn(
              'border-2 px-3 py-2 text-xs font-bold transition-all',
              value.pathRefType === mode
                ? 'border-foreground bg-foreground text-background'
                : 'border-foreground bg-background text-foreground',
            )}
          >
            {mode === 'output' ? '输出目录' : '自定义路径'}
          </button>
        ))}
      </div>

      {value.pathRefType === 'output' && (
        <div className="space-y-2">
          <select
            value={resolvedOutputValue}
            onChange={(event) => onChange({ ...value, pathRefKey: event.target.value })}
            className="w-full border-2 border-foreground bg-background px-3 py-2 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-1"
          >
            {outputOptions.length === 0 && <option value="">暂无输出目录，请先去系统配置填写</option>}
            {outputOptions.map((option) => (
              <option key={option.value} value={option.value}>{option.label}</option>
            ))}
          </select>
          <input
            type="text"
            value={resolvedOutputPath}
            readOnly
            className="w-full border-2 border-foreground bg-muted/20 px-3 py-2 text-xs font-mono font-bold"
          />
        </div>
      )}

      {value.pathRefType === 'custom' && (
        <div className="flex gap-2">
          <input
            type="text"
            value={value.pathRefKey}
            onChange={(event) => onChange({ ...value, pathRefKey: event.target.value })}
            placeholder={placeholder ?? '/data/path'}
            className="w-full border-2 border-foreground bg-background px-3 py-2 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-1"
          />
          <button
            type="button"
            onClick={() => setOpen(true)}
            className="shrink-0 border-2 border-foreground bg-background px-3 py-2 text-foreground transition-all hover:bg-foreground hover:text-background hover:-translate-y-0.5"
          >
            <FolderOpen className="h-4 w-4" />
          </button>
          <DirPicker
            open={open}
            initialPath={value.pathRefKey || '/'}
            title={pickerTitle}
            onConfirm={(path) => {
              onChange({ ...value, pathRefKey: path })
              setOpen(false)
            }}
            onCancel={() => setOpen(false)}
          />
        </div>
      )}

      <input
        type="text"
        value={value.pathSuffix}
        onChange={(event) => onChange({ ...value, pathSuffix: event.target.value })}
        placeholder="可选：相对子目录（例如 .processed）"
        className="w-full border-2 border-foreground bg-background px-3 py-2 text-xs font-mono font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-1"
      />
    </div>
  )
}
