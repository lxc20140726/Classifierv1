import { cn } from '@/lib/utils'

export interface PathChangePreviewProps {
  fromPath?: string | null
  toPath?: string | null
  fromLabel?: string
  toLabel?: string
  unchangedLabel?: string
  className?: string
}

interface SegmentToken {
  text: string
  changed: boolean
  muted: boolean
}

function normalizePath(value?: string | null) {
  if (!value) return ''
  return value.trim().replaceAll('\\', '/')
}

function splitPathSegments(path: string) {
  const normalized = normalizePath(path)
  if (normalized === '') return []
  const parts = normalized.split('/').filter(Boolean)
  if (normalized.startsWith('/')) {
    return ['/', ...parts]
  }
  return parts
}

function segmentKey(value: string) {
  if (/^[A-Za-z]:$/.test(value)) {
    return value.toLowerCase()
  }
  return value
}

function calcSharedPrefixLength(left: string[], right: string[]) {
  let index = 0
  const max = Math.min(left.length, right.length)
  while (index < max && segmentKey(left[index]) === segmentKey(right[index])) {
    index += 1
  }
  return index
}

function buildSegmentTokens(path: string, sharedPrefixLength: number): SegmentToken[] {
  const segments = splitPathSegments(path)
  if (segments.length === 0) return []

  const rootPrefixCount = segments[0] === '/' || /^[A-Za-z]:$/.test(segments[0]) ? 1 : 0
  const shouldCollapseMiddle = segments.length > 8 && sharedPrefixLength - rootPrefixCount > 2
  const tokens: SegmentToken[] = []

  if (!shouldCollapseMiddle) {
    return segments.map((segment, index) => ({
      text: segment,
      changed: index >= sharedPrefixLength,
      muted: index < sharedPrefixLength,
    }))
  }

  for (let index = 0; index < rootPrefixCount; index += 1) {
    tokens.push({
      text: segments[index],
      changed: false,
      muted: true,
    })
  }

  tokens.push({
    text: '...',
    changed: false,
    muted: true,
  })

  const startIndex = Math.max(rootPrefixCount, sharedPrefixLength - 1)
  for (let index = startIndex; index < segments.length; index += 1) {
    tokens.push({
      text: segments[index],
      changed: index >= sharedPrefixLength,
      muted: index < sharedPrefixLength,
    })
  }
  return tokens
}

function PathSegments({ tokens }: { tokens: SegmentToken[] }) {
  if (tokens.length === 0) {
    return <span className="font-mono text-[11px] font-bold text-muted-foreground">—</span>
  }

  return (
    <span className="flex flex-wrap items-center gap-1">
      {tokens.map((token, index) => (
        <span key={`${token.text}-${index}`} className="flex items-center gap-1">
          <span
            className={cn(
              'border px-1 py-0.5 font-mono text-[11px] font-bold',
              token.changed && 'border-blue-700 bg-blue-100 text-blue-900',
              !token.changed && token.muted && 'border-foreground/40 bg-muted/40 text-muted-foreground',
              !token.changed && !token.muted && 'border-foreground bg-background text-foreground',
            )}
          >
            {token.text}
          </span>
          {index < tokens.length - 1 && <span className="text-muted-foreground">/</span>}
        </span>
      ))}
    </span>
  )
}

export function PathChangePreview({
  fromPath,
  toPath,
  fromLabel = '原路径',
  toLabel = '新路径',
  unchangedLabel = '路径未变化',
  className,
}: PathChangePreviewProps) {
  const normalizedFrom = normalizePath(fromPath)
  const normalizedTo = normalizePath(toPath)
  const samePath = normalizedFrom !== '' && normalizedFrom === normalizedTo

  if (samePath) {
    const tokens = buildSegmentTokens(normalizedFrom, splitPathSegments(normalizedFrom).length)
    return (
      <div className={cn('space-y-2 rounded border border-foreground/30 bg-muted/20 p-2', className)} title={normalizedFrom}>
        <p className="text-[11px] font-black text-muted-foreground">{unchangedLabel}</p>
        <PathSegments tokens={tokens} />
      </div>
    )
  }

  const sharedPrefixLength = calcSharedPrefixLength(splitPathSegments(normalizedFrom), splitPathSegments(normalizedTo))
  const fromTokens = buildSegmentTokens(normalizedFrom, sharedPrefixLength)
  const toTokens = buildSegmentTokens(normalizedTo, sharedPrefixLength)
  const titleText = [normalizedFrom, normalizedTo].filter((item) => item !== '').join('\n')

  return (
    <div className={cn('space-y-2 rounded border border-foreground/30 bg-muted/20 p-2', className)} title={titleText}>
      <div className="space-y-1">
        <p className="text-[11px] font-black text-muted-foreground">{fromLabel}</p>
        <PathSegments tokens={fromTokens} />
      </div>
      <div className="space-y-1">
        <p className="text-[11px] font-black text-muted-foreground">{toLabel}</p>
        <PathSegments tokens={toTokens} />
      </div>
    </div>
  )
}
