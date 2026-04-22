import { useEffect, useMemo, useRef, useState } from 'react'
import { AlertCircle, ArrowLeft, Clock3, Link2, Sparkles } from 'lucide-react'
import { Link, useParams } from 'react-router-dom'

import { ApiRequestError } from '@/api/client'
import { getFolderLineage } from '@/api/folders'
import { PathChangePreview } from '@/components/PathChangePreview'
import { useIsMobile } from '@/hooks/useIsMobile'
import { cn } from '@/lib/utils'
import type {
  FolderLineageDirectory,
  FolderLineageFile,
  FolderLineageFlow,
  FolderLineageResponse,
  FolderLineageSourceFile,
  FolderLineageTimelineEvent,
} from '@/types'

type SelectedFile = { kind: 'source' | 'target'; id: string } | null
type TargetArtifactKind = 'archive' | 'final' | 'thumbnail' | 'derived'

interface LinkGeometry {
  id: string
  d: string
}

interface FlowViewport {
  height: number
  scrollTop: number
  width: number
}

interface SourceVirtualRow {
  id: string
  top: number
  height: number
  centerY: number
  file: FolderLineageSourceFile
}

interface TargetDirectoryVirtualRow {
  kind: 'directory'
  key: string
  top: number
  height: number
  centerY: number
  directory: FolderLineageDirectory
  files: FolderLineageFile[]
  artifactMeta: ReturnType<typeof resolveTargetArtifactMeta>
  linkCount: number
}

interface TargetFileVirtualRow {
  kind: 'file'
  key: string
  top: number
  height: number
  centerY: number
  file: FolderLineageFile
  artifactMeta: ReturnType<typeof resolveTargetArtifactMeta>
}

type TargetVirtualRow = TargetDirectoryVirtualRow | TargetFileVirtualRow

interface DirectoryBundleGeometry {
  id: string
  d: string
  glowClassName: string
  strokeClassName: string
  isHighlighted: boolean
  strokeWidth: number
}

const EMPTY_SOURCE_FILES: FolderLineageSourceFile[] = []
const EMPTY_TARGET_FILES: FolderLineageFile[] = []
const EMPTY_LINKS: FolderLineageFlow['links'] = []
const FLOW_MIN_WIDTH = 1120
const FLOW_VIEWPORT_HEIGHT = 620
const FLOW_OVERSCAN = 220
const FLOW_TOP_PADDING = 96
const FLOW_BOTTOM_PADDING = 32
const SOURCE_RAIL_WIDTH = 272
const TARGET_RAIL_WIDTH = 340
const SOURCE_ROW_HEIGHT = 44
const TARGET_ROW_HEIGHT = 44
const TARGET_DIRECTORY_HEIGHT = 92
const TARGET_SECTION_GAP = 14
const FLOW_SIDE_PADDING = 18

const TIMELINE_EVENT_COLOR: Record<FolderLineageTimelineEvent['type'], string> = {
  scan_discovered: 'border-foreground bg-background text-foreground',
  move: 'border-blue-900 bg-blue-100 text-blue-900',
  rename: 'border-indigo-900 bg-indigo-100 text-indigo-900',
  rollback: 'border-amber-900 bg-amber-100 text-amber-900',
  artifact_created: 'border-green-900 bg-green-100 text-green-900',
  processing_failed: 'border-red-900 bg-red-100 text-red-900',
}

const ARTIFACT_TYPE_LABEL: Record<string, string> = {
  video: '视频目录',
  image: '图片目录',
  photo: '图片目录',
  thumbnail: '缩略图目录',
  output: '输出目录',
  archive: '中间归档产物',
  primary: '最终输出产物',
}

const TARGET_ARTIFACT_META: Record<
  TargetArtifactKind,
  {
    badge: string
    label: string
    description: string
    panelClassName: string
    headingClassName: string
    countClassName: string
    badgeClassName: string
    itemIdleClassName: string
    itemActiveClassName: string
    iconClassName: string
    linkCountClassName: string
  }
> = {
  archive: {
    badge: '归档',
    label: '中间归档产物',
    description: '压缩节点生成的中间结果，后续通常还会继续移动或分发。',
    panelClassName: 'border-amber-900/20 bg-amber-50/90',
    headingClassName: 'text-amber-900',
    countClassName: 'border-amber-900/20 bg-amber-100 text-amber-900',
    badgeClassName: 'border-amber-900/20 bg-amber-100 text-amber-900',
    itemIdleClassName: 'border-amber-900/20 bg-amber-50/60 text-foreground hover:border-amber-900/45 hover:bg-amber-100/60',
    itemActiveClassName: 'border-amber-900 bg-amber-950 text-white shadow-hard -translate-y-0.5',
    iconClassName: 'border-amber-900/20 bg-amber-100 text-amber-900',
    linkCountClassName: 'bg-amber-900 text-amber-50',
  },
  final: {
    badge: '最终',
    label: '最终输出产物',
    description: '当前流程最终落位的主输出，通常就是最后保留的结果。',
    panelClassName: 'border-emerald-900/20 bg-emerald-50/90',
    headingClassName: 'text-emerald-900',
    countClassName: 'border-emerald-900/20 bg-emerald-100 text-emerald-900',
    badgeClassName: 'border-emerald-900/20 bg-emerald-100 text-emerald-900',
    itemIdleClassName: 'border-emerald-900/20 bg-emerald-50/60 text-foreground hover:border-emerald-900/45 hover:bg-emerald-100/60',
    itemActiveClassName: 'border-emerald-900 bg-emerald-950 text-white shadow-hard -translate-y-0.5',
    iconClassName: 'border-emerald-900/20 bg-emerald-100 text-emerald-900',
    linkCountClassName: 'bg-emerald-900 text-emerald-50',
  },
  thumbnail: {
    badge: '缩略',
    label: '缩略图产物',
    description: '由缩略图节点派生，属于附加输出而不是主结果。',
    panelClassName: 'border-sky-900/20 bg-sky-50/90',
    headingClassName: 'text-sky-900',
    countClassName: 'border-sky-900/20 bg-sky-100 text-sky-900',
    badgeClassName: 'border-sky-900/20 bg-sky-100 text-sky-900',
    itemIdleClassName: 'border-sky-900/20 bg-sky-50/60 text-foreground hover:border-sky-900/45 hover:bg-sky-100/60',
    itemActiveClassName: 'border-sky-900 bg-sky-950 text-white shadow-hard -translate-y-0.5',
    iconClassName: 'border-sky-900/20 bg-sky-100 text-sky-900',
    linkCountClassName: 'bg-sky-900 text-sky-50',
  },
  derived: {
    badge: '派生',
    label: '派生输出产物',
    description: '流程中的辅助输出或未明确标注阶段的结果。',
    panelClassName: 'border-violet-900/20 bg-violet-50/90',
    headingClassName: 'text-violet-900',
    countClassName: 'border-violet-900/20 bg-violet-100 text-violet-900',
    badgeClassName: 'border-violet-900/20 bg-violet-100 text-violet-900',
    itemIdleClassName: 'border-violet-900/20 bg-violet-50/60 text-foreground hover:border-violet-900/45 hover:bg-violet-100/60',
    itemActiveClassName: 'border-violet-900 bg-violet-950 text-white shadow-hard -translate-y-0.5',
    iconClassName: 'border-violet-900/20 bg-violet-100 text-violet-900',
    linkCountClassName: 'bg-violet-900 text-violet-50',
  },
}

function formatTime(value?: string) {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return '—'
  return date.toLocaleString('zh-CN')
}

function formatDirectoryTitle(directory: FolderLineageDirectory, isSource: boolean) {
  return `${directory.label}${isSource ? '（扫描目录）' : '（目标目录）'}`
}

function resolveArtifactTypeLabel(value?: string) {
  const key = (value ?? '').trim().toLowerCase()
  if (!key) return ''
  return ARTIFACT_TYPE_LABEL[key] ?? ''
}

function resolveTargetArtifactKind({
  artifactType,
  nodeType,
  path,
}: {
  artifactType?: string
  nodeType?: string
  path?: string
}): TargetArtifactKind {
  const normalizedArtifactType = (artifactType ?? '').trim().toLowerCase()
  const normalizedNodeType = (nodeType ?? '').trim().toLowerCase()
  const normalizedPath = (path ?? '').trim().replaceAll('\\', '/').toLowerCase()

  if (normalizedArtifactType === 'archive') return 'archive'
  if (normalizedArtifactType === 'primary') return 'final'
  if (normalizedArtifactType === 'thumbnail') return 'thumbnail'
  if (['image', 'photo', 'video', 'output'].includes(normalizedArtifactType)) return 'derived'

  if (normalizedNodeType === 'thumbnail-node') return 'thumbnail'
  if (normalizedNodeType === 'compress-node') return 'archive'
  if (normalizedNodeType === 'move-node') return 'final'
  if (normalizedPath.startsWith('.archives/') || normalizedPath.includes('/.archives/')) return 'archive'

  return 'derived'
}

function resolveTargetArtifactMeta(input: {
  artifactType?: string
  nodeType?: string
  path?: string
}) {
  const kind = resolveTargetArtifactKind(input)
  const baseMeta = TARGET_ARTIFACT_META[kind]
  const label = resolveArtifactTypeLabel(input.artifactType) || baseMeta.label
  return { ...baseMeta, label }
}

function compactFileName(name: string) {
  const trimmed = name.trim()
  if (trimmed.length <= 18) return trimmed

  const extensionIndex = trimmed.lastIndexOf('.')
  const hasExtension = extensionIndex > 0 && extensionIndex < trimmed.length - 1
  const extension = hasExtension ? trimmed.slice(extensionIndex) : ''
  const base = hasExtension ? trimmed.slice(0, extensionIndex) : trimmed

  if (base.length <= 12) return trimmed
  return `${base.slice(0, 8)}…${base.slice(-4)}${extension}`
}

function getFileExtension(name: string) {
  const trimmed = name.trim()
  const extensionIndex = trimmed.lastIndexOf('.')
  if (extensionIndex <= 0 || extensionIndex === trimmed.length - 1) return ''
  return trimmed.slice(extensionIndex + 1).slice(0, 6).toUpperCase()
}

function getDirectoryKey(directory: FolderLineageDirectory) {
  return directory.id ?? directory.path
}

function buildCurvePath(x1: number, y1: number, x2: number, y2: number) {
  const deltaX = Math.max(54, (x2 - x1) * 0.38)
  const c1x = x1 + deltaX
  const c2x = x2 - deltaX
  return `M ${x1} ${y1} C ${c1x} ${y1}, ${c2x} ${y2}, ${x2} ${y2}`
}

function getVisibleRows<T extends { top: number; height: number }>(rows: T[], viewportTop: number, viewportBottom: number) {
  return rows.filter((row) => row.top + row.height >= viewportTop && row.top <= viewportBottom)
}

function resolveBundleTone(kind: TargetArtifactKind) {
  switch (kind) {
    case 'archive':
      return { glowClassName: 'stroke-amber-300/35', strokeClassName: 'stroke-amber-700/75' }
    case 'final':
      return { glowClassName: 'stroke-emerald-300/35', strokeClassName: 'stroke-emerald-700/75' }
    case 'thumbnail':
      return { glowClassName: 'stroke-sky-300/35', strokeClassName: 'stroke-sky-700/75' }
    default:
      return { glowClassName: 'stroke-violet-300/35', strokeClassName: 'stroke-violet-700/75' }
  }
}

function FileItem({
  file,
  isActive,
  isMuted,
  linkCount,
  tone,
  artifactMeta,
  onClick,
  onMouseEnter,
  onMouseLeave,
  refCallback,
}: {
  file: FolderLineageSourceFile | FolderLineageFile
  isActive: boolean
  isMuted: boolean
  linkCount: number
  tone: 'source' | 'target'
  artifactMeta?: (typeof TARGET_ARTIFACT_META)[TargetArtifactKind]
  onClick: () => void
  onMouseEnter: () => void
  onMouseLeave: () => void
  refCallback: (node: HTMLButtonElement | null) => void
}) {
  const extension = getFileExtension(file.name)
  const iconText = tone === 'source' ? '源' : artifactMeta?.badge.slice(0, 1) ?? '产'
  return (
    <button
      ref={refCallback}
      type="button"
      onClick={onClick}
      onMouseEnter={onMouseEnter}
      onMouseLeave={onMouseLeave}
      className={cn(
        'group flex h-10 w-full items-center gap-2 overflow-hidden border px-2.5 text-left text-[11px] font-black tracking-[0.01em] transition-all',
        isMuted && !isActive && 'opacity-40',
        isActive
          ? tone === 'source'
            ? 'border-blue-900 bg-blue-950 text-white shadow-hard -translate-y-0.5'
            : artifactMeta?.itemActiveClassName ?? 'border-emerald-900 bg-emerald-950 text-white shadow-hard -translate-y-0.5'
          : tone === 'source'
            ? 'border-foreground/20 bg-background/90 text-foreground hover:border-foreground/50 hover:bg-muted/50'
            : artifactMeta?.itemIdleClassName ?? 'border-emerald-900/20 bg-emerald-50/60 text-foreground hover:border-emerald-900/45 hover:bg-emerald-100/60',
      )}
      title={file.path}
    >
      <span
        className={cn(
          'inline-flex h-5 w-5 shrink-0 items-center justify-center rounded-full border text-[10px]',
          isActive
            ? 'border-white/40 bg-white/10 text-white'
            : tone === 'source'
              ? 'border-blue-900/20 bg-blue-100 text-blue-900'
              : artifactMeta?.iconClassName ?? 'border-emerald-900/20 bg-emerald-100 text-emerald-900',
        )}
      >
        {iconText}
      </span>
      <span className="min-w-0 flex-1 truncate">{compactFileName(file.name)}</span>
      {tone === 'target' && artifactMeta && (
        <span
          className={cn(
            'shrink-0 rounded-full border px-1.5 py-0.5 text-[10px] leading-none',
            isActive ? 'border-white/30 bg-white/10 text-white/90' : artifactMeta.badgeClassName,
          )}
        >
          {artifactMeta.badge}
        </span>
      )}
      {extension && (
        <span
          className={cn(
            'shrink-0 rounded-full border px-1.5 py-0.5 text-[10px] leading-none',
            isActive ? 'border-white/30 bg-white/10 text-white/90' : 'border-foreground/15 bg-muted/40 text-muted-foreground',
          )}
        >
          {extension}
        </span>
      )}
      <span
        className={cn(
          'inline-flex shrink-0 items-center gap-1 rounded-full px-1.5 py-0.5 text-[10px] leading-none',
          isActive ? 'bg-white/10 text-white/90' : tone === 'source' ? 'bg-foreground text-background' : artifactMeta?.linkCountClassName ?? 'bg-foreground text-background',
        )}
      >
        <Link2 className="h-3 w-3" />
        {linkCount}
      </span>
    </button>
  )
}

function DirectoryCard({
  row,
  isActive,
  isMuted,
}: {
  row: TargetDirectoryVirtualRow
  isActive: boolean
  isMuted: boolean
}) {
  return (
    <div
      className={cn(
        'w-full rounded-sm border px-3 py-2 shadow-hard backdrop-blur transition-all duration-200',
        row.artifactMeta.panelClassName,
        isActive && 'ring-2 ring-foreground/15',
        isMuted && !isActive && 'opacity-35',
      )}
      title={row.directory.path}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <p className={cn('truncate text-[11px] font-black', row.artifactMeta.headingClassName)}>
              {formatDirectoryTitle(row.directory, false)}
            </p>
            <span className={cn('rounded-full border px-1.5 py-0.5 text-[10px] font-black', row.artifactMeta.badgeClassName)}>
              {row.artifactMeta.label}
            </span>
          </div>
          <p className="mt-1 truncate font-mono text-[10px] font-bold text-muted-foreground">{row.directory.path || '--'}</p>
          <p className="mt-1 text-[10px] font-bold text-muted-foreground">{row.artifactMeta.description}</p>
        </div>
        <div className="shrink-0 space-y-1 text-right text-[10px] font-black">
          <p className={cn('border px-1.5 py-0.5', row.artifactMeta.countClassName)}>{row.files.length} 文件</p>
          <p className="inline-flex items-center gap-1 rounded-full bg-foreground px-1.5 py-0.5 text-background">
            <Link2 className="h-3 w-3" />
            {row.linkCount}
          </p>
        </div>
      </div>
    </div>
  )
}

export default function FolderLineagePage() {
  const isMobile = useIsMobile(1024)
  const { id } = useParams<{ id: string }>()
  const [data, setData] = useState<FolderLineageResponse | null>(null)
  const [selectedFile, setSelectedFile] = useState<SelectedFile>(null)
  const [hoveredFile, setHoveredFile] = useState<SelectedFile>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [flowViewport, setFlowViewport] = useState<FlowViewport>({
    height: FLOW_VIEWPORT_HEIGHT,
    scrollTop: 0,
    width: FLOW_MIN_WIDTH,
  })

  const flowScrollRef = useRef<HTMLDivElement | null>(null)
  const flowCanvasRef = useRef<HTMLDivElement | null>(null)
  const scrollFrameRef = useRef<number | null>(null)
  const sourceFileRefs = useRef<Record<string, HTMLButtonElement | null>>({})
  const targetFileRefs = useRef<Record<string, HTMLButtonElement | null>>({})
  const linkGeometries: LinkGeometry[] = []

  useEffect(() => {
    if (!id) return
    let cancelled = false
    void getFolderLineage(id)
      .then((resp) => {
        if (cancelled) return
        setError(null)
        setSelectedFile(null)
        setHoveredFile(null)
        setData(resp)
      })
      .catch((err: unknown) => {
        if (cancelled) return
        if (err instanceof ApiRequestError) {
          setError(err.message)
          return
        }
        setError('加载文件夹溯源数据失败')
      })
      .finally(() => {
        if (cancelled) return
        setIsLoading(false)
      })
    return () => {
      cancelled = true
    }
  }, [id])

  const flow = data?.flow
  const sourceFiles = flow?.source_files ?? EMPTY_SOURCE_FILES
  const targetFiles = flow?.target_files ?? EMPTY_TARGET_FILES
  const links = flow?.links ?? EMPTY_LINKS

  const targetFileByID = useMemo(() => {
    const nextMap = new Map<string, FolderLineageFile>()
    for (const file of targetFiles) {
      nextMap.set(file.id, file)
    }
    return nextMap
  }, [targetFiles])

  const sourceFileByID = useMemo(() => {
    const nextMap = new Map<string, FolderLineageSourceFile>()
    for (const file of sourceFiles) {
      nextMap.set(file.id, file)
    }
    return nextMap
  }, [sourceFiles])

  const targetFilesByDirectoryID = useMemo(() => {
    const nextMap = new Map<string, FolderLineageFile[]>()
    for (const file of targetFiles) {
      const existing = nextMap.get(file.directory_id) ?? []
      existing.push(file)
      nextMap.set(file.directory_id, existing)
    }
    return nextMap
  }, [targetFiles])

  const targetDirectoryByID = useMemo(() => {
    const nextMap = new Map<string, FolderLineageDirectory>()
    for (const directory of flow?.target_directories ?? []) {
      if (directory.id != null) {
        nextMap.set(directory.id, directory)
      }
    }
    return nextMap
  }, [flow?.target_directories])

  const targetFileMetaByID = useMemo(() => {
    const nextMap = new Map<string, ReturnType<typeof resolveTargetArtifactMeta>>()
    for (const file of targetFiles) {
      nextMap.set(
        file.id,
        resolveTargetArtifactMeta({
          artifactType: file.artifact_type,
          nodeType: file.node_type,
          path: file.path,
        }),
      )
    }
    return nextMap
  }, [targetFiles])

  const sourceLinkCountByID = useMemo(() => {
    const nextMap = new Map<string, number>()
    for (const link of links) {
      nextMap.set(link.source_file_id, (nextMap.get(link.source_file_id) ?? 0) + 1)
    }
    return nextMap
  }, [links])

  const targetLinkCountByID = useMemo(() => {
    const nextMap = new Map<string, number>()
    for (const link of links) {
      nextMap.set(link.target_file_id, (nextMap.get(link.target_file_id) ?? 0) + 1)
    }
    return nextMap
  }, [links])

  const directoryLinkCountByID = useMemo(() => {
    const nextMap = new Map<string, number>()
    for (const link of links) {
      const targetFile = targetFileByID.get(link.target_file_id)
      if (targetFile == null) continue
      nextMap.set(targetFile.directory_id, (nextMap.get(targetFile.directory_id) ?? 0) + 1)
    }
    return nextMap
  }, [links, targetFileByID])

  const linksBySourceID = useMemo(() => {
    const nextMap = new Map<string, FolderLineageFlow['links']>()
    for (const link of links) {
      const existing = nextMap.get(link.source_file_id) ?? []
      existing.push(link)
      nextMap.set(link.source_file_id, existing)
    }
    return nextMap
  }, [links])

  const linksByTargetID = useMemo(() => {
    const nextMap = new Map<string, FolderLineageFlow['links']>()
    for (const link of links) {
      const existing = nextMap.get(link.target_file_id) ?? []
      existing.push(link)
      nextMap.set(link.target_file_id, existing)
    }
    return nextMap
  }, [links])

  const activeFile = hoveredFile ?? selectedFile
  const activeLinks = useMemo(() => {
    if (activeFile == null) return EMPTY_LINKS
    return activeFile.kind === 'source'
      ? (linksBySourceID.get(activeFile.id) ?? EMPTY_LINKS)
      : (linksByTargetID.get(activeFile.id) ?? EMPTY_LINKS)
  }, [activeFile, linksBySourceID, linksByTargetID])

  const relatedLinkIDs = useMemo(() => {
    const ids = new Set<string>()
    for (const link of activeLinks) {
      ids.add(link.id)
    }
    return ids
  }, [activeLinks])

  const highlightedSourceIDs = useMemo(() => {
    const ids = new Set<string>()
    if (activeFile?.kind === 'source') {
      ids.add(activeFile.id)
    }
    for (const link of activeLinks) {
      if (relatedLinkIDs.has(link.id)) {
        ids.add(link.source_file_id)
      }
    }
    return ids
  }, [activeFile, activeLinks, relatedLinkIDs])

  const highlightedTargetIDs = useMemo(() => {
    const ids = new Set<string>()
    if (activeFile?.kind === 'target') {
      ids.add(activeFile.id)
    }
    for (const link of activeLinks) {
      if (relatedLinkIDs.has(link.id)) {
        ids.add(link.target_file_id)
      }
    }
    return ids
  }, [activeFile, activeLinks, relatedLinkIDs])

  const highlightedDirectoryIDs = useMemo(() => {
    const ids = new Set<string>()
    if (activeFile?.kind === 'target') {
      const targetFile = targetFileByID.get(activeFile.id)
      if (targetFile != null) {
        ids.add(targetFile.directory_id)
      }
    }
    for (const link of activeLinks) {
      const targetFile = targetFileByID.get(link.target_file_id)
      if (targetFile != null) {
        ids.add(targetFile.directory_id)
      }
    }
    return ids
  }, [activeFile, activeLinks, targetFileByID])

  const selectedDetails = useMemo(() => {
    if (selectedFile == null) return null
    if (selectedFile.kind === 'source') {
      const sourceFile = sourceFileByID.get(selectedFile.id)
      if (sourceFile == null) return null
      const relatedTargets = (linksBySourceID.get(sourceFile.id) ?? EMPTY_LINKS)
        .map((link) => targetFileByID.get(link.target_file_id))
        .filter((item): item is FolderLineageFile => item != null)
      return {
        title: sourceFile.name,
        path: sourceFile.path,
        directory: flow?.source_directory.path ?? '',
        nodeType: relatedTargets[0]?.node_type ?? '',
        workflowRunID: relatedTargets[0]?.workflow_run_id ?? '',
        jobID: relatedTargets[0]?.job_id ?? '',
        artifactLabel: '',
      }
    }

    const targetFile = targetFileByID.get(selectedFile.id)
    if (targetFile == null) return null
    const directoryPath = targetDirectoryByID.get(targetFile.directory_id)?.path ?? ''
    const artifactMeta = targetFileMetaByID.get(targetFile.id) ?? resolveTargetArtifactMeta({
      artifactType: targetFile.artifact_type,
      nodeType: targetFile.node_type,
      path: targetFile.path,
    })
    return {
      title: targetFile.name,
      path: targetFile.path,
      directory: directoryPath,
      nodeType: targetFile.node_type ?? '',
      workflowRunID: targetFile.workflow_run_id ?? '',
      jobID: targetFile.job_id ?? '',
      artifactLabel: artifactMeta.label,
    }
  }, [flow, linksBySourceID, selectedFile, sourceFileByID, targetDirectoryByID, targetFileByID, targetFileMetaByID])

  const sourceRows = useMemo(() => {
    let top = FLOW_TOP_PADDING
    const rows: SourceVirtualRow[] = []

    for (const file of sourceFiles) {
      rows.push({
        id: file.id,
        top,
        height: SOURCE_ROW_HEIGHT,
        centerY: top + (SOURCE_ROW_HEIGHT / 2),
        file,
      })
      top += SOURCE_ROW_HEIGHT
    }

    return rows
  }, [sourceFiles])

  const targetRows = useMemo(() => {
    let top = FLOW_TOP_PADDING
    const rows: TargetVirtualRow[] = []

    for (const directory of flow?.target_directories ?? []) {
      const files = directory.id == null ? [] : (targetFilesByDirectoryID.get(directory.id) ?? [])

      rows.push({
        kind: 'directory',
        key: `directory:${getDirectoryKey(directory)}`,
        top,
        height: TARGET_DIRECTORY_HEIGHT,
        centerY: top + (TARGET_DIRECTORY_HEIGHT / 2),
        directory,
        files,
        artifactMeta: resolveTargetArtifactMeta({
          artifactType: directory.artifact_type,
          path: directory.path,
        }),
        linkCount: directory.id == null ? 0 : (directoryLinkCountByID.get(directory.id) ?? 0),
      })

      top += TARGET_DIRECTORY_HEIGHT

      for (const file of files) {
        rows.push({
          kind: 'file',
          key: file.id,
          top,
          height: TARGET_ROW_HEIGHT,
          centerY: top + (TARGET_ROW_HEIGHT / 2),
          file,
          artifactMeta: targetFileMetaByID.get(file.id) ?? resolveTargetArtifactMeta({
            artifactType: file.artifact_type,
            nodeType: file.node_type,
            path: file.path,
          }),
        })

        top += TARGET_ROW_HEIGHT
      }

      top += TARGET_SECTION_GAP
    }

    return rows
  }, [directoryLinkCountByID, flow?.target_directories, targetFileMetaByID, targetFilesByDirectoryID])

  const sourceContentHeight =
    sourceRows.length === 0 ? FLOW_TOP_PADDING + FLOW_BOTTOM_PADDING : sourceRows[sourceRows.length - 1].top + SOURCE_ROW_HEIGHT + FLOW_BOTTOM_PADDING

  const targetContentHeight =
    targetRows.length === 0
      ? FLOW_TOP_PADDING + FLOW_BOTTOM_PADDING
      : targetRows[targetRows.length - 1].top + targetRows[targetRows.length - 1].height + FLOW_BOTTOM_PADDING

  const contentHeight = Math.max(flowViewport.height, sourceContentHeight, targetContentHeight)

  useEffect(() => {
    return () => {
      if (scrollFrameRef.current != null) {
        cancelAnimationFrame(scrollFrameRef.current)
      }
    }
  }, [])

  useEffect(() => {
    const container = flowScrollRef.current
    const canvas = flowCanvasRef.current
    if (container == null || canvas == null) {
      return
    }

    const updateViewport = () => {
      const nextHeight = container.clientHeight || FLOW_VIEWPORT_HEIGHT
      const nextWidth = Math.max(canvas.clientWidth, FLOW_MIN_WIDTH)

      setFlowViewport((current) =>
        current.height === nextHeight && current.width === nextWidth
          ? current
          : {
              ...current,
              height: nextHeight,
              width: nextWidth,
            },
      )
    }

    updateViewport()

    const observer = new ResizeObserver(updateViewport)
    observer.observe(container)
    observer.observe(canvas)

    return () => {
      observer.disconnect()
    }
  }, [contentHeight, flow])

  useEffect(() => {
    const container = flowScrollRef.current
    if (container == null) return

    container.scrollTop = 0
  }, [id, flow?.source_directory.path])

  const viewportTop = Math.max(0, flowViewport.scrollTop - FLOW_OVERSCAN)
  const viewportBottom = flowViewport.scrollTop + flowViewport.height + FLOW_OVERSCAN

  const visibleSourceRows = useMemo(
    () => getVisibleRows(sourceRows, viewportTop, viewportBottom),
    [sourceRows, viewportBottom, viewportTop],
  )

  const visibleTargetRows = useMemo(
    () => getVisibleRows(targetRows, viewportTop, viewportBottom),
    [targetRows, viewportBottom, viewportTop],
  )

  const visibleTargetFileCount = useMemo(
    () => visibleTargetRows.filter((row): row is TargetFileVirtualRow => row.kind === 'file').length,
    [visibleTargetRows],
  )

  const visibleTargetFileCenterByID = useMemo(() => {
    const nextMap = new Map<string, number>()
    for (const row of visibleTargetRows) {
      if (row.kind === 'file') {
        nextMap.set(row.file.id, row.centerY)
      }
    }
    return nextMap
  }, [visibleTargetRows])

  const visibleDirectoryRows = useMemo(
    () => visibleTargetRows.filter((row): row is TargetDirectoryVirtualRow => row.kind === 'directory'),
    [visibleTargetRows],
  )

  const shouldUseBundledOverview = activeFile == null && (links.length > 1800 || sourceFiles.length + targetFiles.length > 1200)
  const stageWidth = Math.max(flowViewport.width, FLOW_MIN_WIDTH)
  const sourceAnchorX = SOURCE_RAIL_WIDTH - FLOW_SIDE_PADDING
  const targetAnchorX = stageWidth - TARGET_RAIL_WIDTH + FLOW_SIDE_PADDING
  const bundleStartX = SOURCE_RAIL_WIDTH + 18
  const bundleEndX = stageWidth - TARGET_RAIL_WIDTH - 18

  const exactLinkGeometries = useMemo(() => {
    if (shouldUseBundledOverview) {
      return []
    }

    const onlyRelated = activeFile != null
    const geometries: LinkGeometry[] = []

    for (const sourceRow of visibleSourceRows) {
      const rowLinks = linksBySourceID.get(sourceRow.file.id) ?? EMPTY_LINKS

      for (const link of rowLinks) {
        if (onlyRelated && !relatedLinkIDs.has(link.id)) {
          continue
        }

        const targetY = visibleTargetFileCenterByID.get(link.target_file_id)
        if (targetY == null) {
          continue
        }

        geometries.push({
          id: link.id,
          d: buildCurvePath(sourceAnchorX, sourceRow.centerY, targetAnchorX, targetY),
        })
      }
    }

    return geometries
  }, [
    activeFile,
    linksBySourceID,
    relatedLinkIDs,
    shouldUseBundledOverview,
    sourceAnchorX,
    targetAnchorX,
    visibleSourceRows,
    visibleTargetFileCenterByID,
  ])

  const maxVisibleDirectoryLinks = useMemo(() => {
    if (visibleDirectoryRows.length === 0) return 1
    return Math.max(...visibleDirectoryRows.map((row) => row.linkCount), 1)
  }, [visibleDirectoryRows])

  const bundleGeometries = useMemo(() => {
    const geometries: DirectoryBundleGeometry[] = []
    const bundleSourceY = flowViewport.scrollTop + 54

    for (const row of visibleDirectoryRows) {
      if (row.linkCount === 0) continue

      const tone = resolveBundleTone(resolveTargetArtifactKind({
        artifactType: row.directory.artifact_type,
        path: row.directory.path,
      }))
      const emphasis = row.linkCount / maxVisibleDirectoryLinks

      geometries.push({
        id: row.key,
        d: buildCurvePath(bundleStartX, bundleSourceY, bundleEndX, row.centerY),
        glowClassName: tone.glowClassName,
        strokeClassName: tone.strokeClassName,
        isHighlighted: row.directory.id != null && highlightedDirectoryIDs.has(row.directory.id),
        strokeWidth: 6 + (emphasis * 16),
      })
    }

    return geometries
  }, [bundleEndX, bundleStartX, flowViewport.scrollTop, highlightedDirectoryIDs, maxVisibleDirectoryLinks, visibleDirectoryRows])

  const renderedFlowSection = flow == null ? (
    <section className="border-2 border-dashed border-foreground bg-card px-4 py-10 text-center shadow-hard">
      <p className="text-sm font-black">暂无文件级去向数据</p>
      <p className="mt-2 text-xs font-bold text-muted-foreground">当前仅展示摘要与时间线信息</p>
    </section>
  ) : (
    <section className="space-y-3 border-2 border-foreground bg-card p-3 shadow-hard">
      <div className="mb-1 flex flex-wrap items-start justify-between gap-3">
        <div>
          <h2 className="text-sm font-black">目录分组文件流向图</h2>
          <p className="mt-1 text-[11px] font-bold text-muted-foreground">
            把左右文件压成轻量轨道，默认先看目录级流带；点击文件后再展开当前映射，滚动时只渲染视口内节点。
          </p>
        </div>
        <div className="flex flex-wrap items-center gap-2 text-[11px] font-black">
          <span className="inline-flex items-center gap-1.5 border border-blue-900/20 bg-blue-100 px-2 py-1 text-blue-900">
            <Sparkles className="h-3.5 w-3.5" />
            源文件 {sourceFiles.length}
          </span>
          <span className="inline-flex items-center gap-1.5 border border-emerald-900/20 bg-emerald-100 px-2 py-1 text-emerald-900">
            <Sparkles className="h-3.5 w-3.5" />
            目标文件 {targetFiles.length}
          </span>
          <span className="inline-flex items-center gap-1.5 border border-foreground/20 bg-muted/40 px-2 py-1 text-foreground">
            <Link2 className="h-3.5 w-3.5" />
            映射 {links.length}
          </span>
          <span className="inline-flex items-center gap-1.5 border border-foreground/20 bg-background px-2 py-1 text-muted-foreground">
            <Sparkles className="h-3.5 w-3.5" />
            视口 源 {visibleSourceRows.length} / 目标 {visibleTargetFileCount}
          </span>
          {shouldUseBundledOverview && (
            <span className="inline-flex items-center gap-1.5 border border-amber-900/20 bg-amber-100 px-2 py-1 text-amber-900">
              <Sparkles className="h-3.5 w-3.5" />
              高密度聚合
            </span>
          )}
          <button
            type="button"
            onClick={() => {
              setSelectedFile(null)
              setHoveredFile(null)
            }}
            className="text-xs font-bold text-muted-foreground underline-offset-4 hover:underline"
          >
            清除高亮
          </button>
        </div>
      </div>

      <div className="border-2 border-foreground bg-background">
        <div
          ref={flowScrollRef}
          onScroll={(event) => {
            const nextScrollTop = event.currentTarget.scrollTop

            setHoveredFile(null)

            if (scrollFrameRef.current != null) {
              cancelAnimationFrame(scrollFrameRef.current)
            }

            scrollFrameRef.current = window.requestAnimationFrame(() => {
              setFlowViewport((current) =>
                current.scrollTop === nextScrollTop
                  ? current
                  : {
                      ...current,
                      scrollTop: nextScrollTop,
                    },
              )
            })
          }}
          className="max-h-[620px] overflow-auto"
        >
          <div
            ref={flowCanvasRef}
            className="relative min-w-[1120px]"
            style={{ height: contentHeight }}
          >
            <div
              className="absolute inset-y-0 left-0 border-r border-blue-900/10 bg-[linear-gradient(180deg,rgba(37,99,235,0.09)_0,rgba(37,99,235,0.03)_35%,transparent_100%)]"
              style={{ width: SOURCE_RAIL_WIDTH }}
            />
            <div
              className="absolute inset-y-0 border-x border-foreground/10 bg-[radial-gradient(circle_at_center,rgba(255,255,255,0.14)_0,rgba(255,255,255,0.03)_45%,transparent_72%)]"
              style={{
                left: SOURCE_RAIL_WIDTH,
                width: stageWidth - SOURCE_RAIL_WIDTH - TARGET_RAIL_WIDTH,
              }}
            />
            <div
              className="absolute inset-y-0 right-0 border-l border-emerald-900/10 bg-[linear-gradient(180deg,rgba(5,150,105,0.09)_0,rgba(5,150,105,0.03)_35%,transparent_100%)]"
              style={{ width: TARGET_RAIL_WIDTH }}
            />

            <svg className="pointer-events-none absolute inset-0 h-full w-full" aria-hidden="true">
              <g>
                {bundleGeometries.map((geometry) => (
                  <g key={geometry.id}>
                    <path
                      d={geometry.d}
                      fill="none"
                      strokeLinecap="round"
                      className={cn(
                        'transition-all duration-200',
                        activeFile == null
                          ? geometry.glowClassName
                          : geometry.isHighlighted
                            ? geometry.glowClassName
                            : 'stroke-muted-foreground/8',
                      )}
                      strokeWidth={geometry.strokeWidth}
                    />
                    <path
                      d={geometry.d}
                      fill="none"
                      strokeLinecap="round"
                      className={cn(
                        'transition-all duration-200',
                        activeFile == null
                          ? geometry.strokeClassName
                          : geometry.isHighlighted
                            ? geometry.strokeClassName
                            : 'stroke-muted-foreground/18',
                      )}
                      strokeWidth={Math.max(1.6, geometry.strokeWidth * 0.24)}
                    />
                  </g>
                ))}
              </g>
              <g>
                {exactLinkGeometries.map((geometry) => (
                  <g key={geometry.id}>
                    <path
                      d={geometry.d}
                      fill="none"
                      strokeLinecap="round"
                      className={cn('transition-all duration-200', activeFile == null ? 'stroke-sky-300/26' : 'stroke-blue-300/45')}
                      strokeWidth={activeFile == null ? 4 : 6}
                    />
                    <path
                      d={geometry.d}
                      fill="none"
                      strokeLinecap="round"
                      className={cn('transition-all duration-200', activeFile == null ? 'stroke-blue-950/45' : 'stroke-blue-700')}
                      strokeWidth={activeFile == null ? 1.4 : 2.4}
                    />
                  </g>
                ))}
              </g>
            </svg>

            <div
              className="pointer-events-none absolute left-0 z-20 px-4"
              style={{ top: flowViewport.scrollTop + 16, width: SOURCE_RAIL_WIDTH }}
            >
              <div className="ml-auto w-full max-w-[236px] rounded-sm border border-blue-900/20 bg-white/92 px-3 py-2 shadow-hard backdrop-blur">
                <div className="flex items-center justify-between gap-2">
                  <p className="truncate text-[11px] font-black text-blue-900">{formatDirectoryTitle(flow.source_directory, true)}</p>
                  <span className="border border-blue-900/20 bg-blue-100 px-1.5 py-0.5 text-[10px] font-black text-blue-900">
                    {sourceFiles.length} 项
                  </span>
                </div>
                <p className="mt-1 truncate font-mono text-[10px] font-bold text-muted-foreground">{flow.source_directory.path || '--'}</p>
              </div>
            </div>

            <div
              className="pointer-events-none absolute z-20 px-5"
              style={{
                top: flowViewport.scrollTop + 18,
                left: SOURCE_RAIL_WIDTH,
                width: stageWidth - SOURCE_RAIL_WIDTH - TARGET_RAIL_WIDTH,
              }}
            >
              <div className="mx-auto flex max-w-[520px] items-center justify-center gap-2 rounded-full border border-foreground/15 bg-background/92 px-4 py-2 text-[11px] font-black shadow-hard backdrop-blur">
                <Link2 className="h-3.5 w-3.5" />
                {activeFile != null
                  ? `当前聚焦 ${relatedLinkIDs.size} 条映射，优先显示精确连线`
                  : shouldUseBundledOverview
                    ? '高密度目录默认显示目录级流带，点击文件查看精确连线'
                    : '仅渲染视口内连线，滚动时保持轻量不卡顿'}
              </div>
            </div>

            {visibleSourceRows.map((row) => (
              <div
                key={row.id}
                className="absolute left-0 px-4"
                style={{ top: row.top, width: SOURCE_RAIL_WIDTH }}
              >
                <div className="ml-auto w-full max-w-[236px]">
                  <FileItem
                    file={row.file}
                    isActive={highlightedSourceIDs.has(row.file.id)}
                    isMuted={relatedLinkIDs.size > 0 && !highlightedSourceIDs.has(row.file.id)}
                    linkCount={sourceLinkCountByID.get(row.file.id) ?? 0}
                    tone="source"
                    onClick={() => setSelectedFile({ kind: 'source', id: row.file.id })}
                    onMouseEnter={() => setHoveredFile({ kind: 'source', id: row.file.id })}
                    onMouseLeave={() => setHoveredFile((current) => (current?.kind === 'source' && current.id === row.file.id ? null : current))}
                    refCallback={() => {}}
                  />
                </div>
              </div>
            ))}

            {visibleTargetRows.map((row) => (
              <div
                key={row.key}
                className="absolute right-0 px-4"
                style={{ top: row.top, width: TARGET_RAIL_WIDTH }}
              >
                <div className="w-full max-w-[304px]">
                  {row.kind === 'directory' ? (
                    <DirectoryCard
                      row={row}
                      isActive={row.directory.id != null && highlightedDirectoryIDs.has(row.directory.id)}
                      isMuted={relatedLinkIDs.size > 0 && row.directory.id != null && !highlightedDirectoryIDs.has(row.directory.id)}
                    />
                  ) : (
                    <FileItem
                      file={row.file}
                      isActive={highlightedTargetIDs.has(row.file.id)}
                      isMuted={relatedLinkIDs.size > 0 && !highlightedTargetIDs.has(row.file.id)}
                      linkCount={targetLinkCountByID.get(row.file.id) ?? 0}
                      tone="target"
                      artifactMeta={row.artifactMeta}
                      onClick={() => setSelectedFile({ kind: 'target', id: row.file.id })}
                      onMouseEnter={() => setHoveredFile({ kind: 'target', id: row.file.id })}
                      onMouseLeave={() => setHoveredFile((current) => (current?.kind === 'target' && current.id === row.file.id ? null : current))}
                      refCallback={() => {}}
                    />
                  )}
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>

      <div className="grid grid-cols-1 gap-3 lg:grid-cols-[minmax(0,1fr)_300px]">
        <div className="border-2 border-foreground bg-card p-3 shadow-hard">
          <h3 className="mb-2 text-sm font-black">映射说明</h3>
          <div className="flex flex-wrap items-center gap-2 text-[11px] font-bold text-muted-foreground">
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-8 rounded-full bg-blue-700" />
              点击文件后切换到精确映射，只保留当前关联连线
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="h-2.5 w-8 rounded-full bg-amber-500" />
              高密度目录默认看目录级流带，先看走向再钻取文件
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="inline-flex h-5 items-center rounded-full border border-foreground/15 bg-muted/40 px-1.5 text-[10px] font-black">JPG</span>
              左右文件压缩成轨道展示，完整路径放到右侧详情
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="inline-flex h-5 items-center rounded-full border border-foreground/15 bg-background px-1.5 text-[10px] font-black">视口</span>
              滚动时只挂载当前可见节点和连线，数千文件也不会一次性挤满 DOM
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="inline-flex h-5 items-center rounded-full border border-amber-900/20 bg-amber-100 px-1.5 text-[10px] font-black text-amber-900">归档</span>
              中间归档产物，后续通常还会继续流转
            </span>
            <span className="inline-flex items-center gap-1.5">
              <span className="inline-flex h-5 items-center rounded-full border border-emerald-900/20 bg-emerald-100 px-1.5 text-[10px] font-black text-emerald-900">最终</span>
              最终输出产物，表示当前流程的主落位结果
            </span>
          </div>
        </div>

        <div className="border-2 border-foreground bg-card p-3 shadow-hard">
          <h3 className="mb-2 text-sm font-black">详情面板</h3>
          {selectedDetails == null ? (
            <p className="text-xs font-bold text-muted-foreground">点击任意文件条目查看完整路径和运行信息</p>
          ) : (
            <div className="space-y-1.5 text-xs font-bold">
              <p className="inline-flex items-center gap-1 border-2 border-foreground bg-background px-2 py-1">
                <Link2 className="h-3.5 w-3.5" />
                {selectedDetails.title}
              </p>
              <p className="inline-flex w-fit items-center border border-foreground/40 bg-muted/30 px-2 py-0.5 text-[11px] font-black text-muted-foreground">
                当前路径
              </p>
              <p>
                完整路径：<span className="break-all font-mono">{selectedDetails.path || '--'}</span>
              </p>
              <p>
                目录：<span className="break-all font-mono">{selectedDetails.directory || '--'}</span>
              </p>
              <p>节点类型：{selectedDetails.nodeType || '--'}</p>
              {selectedDetails.artifactLabel && <p>产物阶段：{selectedDetails.artifactLabel}</p>}
              <p>Workflow Run：{selectedDetails.workflowRunID || '--'}</p>
              <p>Job：{selectedDetails.jobID || '--'}</p>
            </div>
          )}
        </div>
      </div>
    </section>
  )

  const shouldRenderLegacyFlow = !isMobile && selectedFile?.id === '__legacy_flow__'

  if (isLoading) {
    return (
      <div className="flex items-center justify-center py-24">
        <p className="text-sm font-bold text-muted-foreground">文件流向数据加载中...</p>
      </div>
    )
  }

  if (error != null || data == null) {
    return (
      <div className="space-y-4">
        <Link
          to="/"
          className="inline-flex items-center gap-2 border-2 border-foreground bg-background px-3 py-2 text-sm font-bold transition-all hover:bg-foreground hover:text-background"
        >
          <ArrowLeft className="h-4 w-4" />
          返回文件夹列表
        </Link>
        <div className="border-2 border-red-900 bg-red-100 px-4 py-4 text-sm font-bold text-red-900 shadow-hard">
          {error ?? '文件流向数据不存在'}
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-2xl font-black tracking-tight">文件夹文件流向</h1>
          <p className="mt-1 break-all font-mono text-xs font-bold text-muted-foreground">{data.folder.path}</p>
        </div>
        <Link
          to="/"
          className="inline-flex items-center gap-2 border-2 border-foreground bg-background px-3 py-2 text-sm font-bold transition-all hover:bg-foreground hover:text-background"
        >
          <ArrowLeft className="h-4 w-4" />
          返回列表
        </Link>
      </div>

      <section className="grid grid-cols-1 gap-3 md:grid-cols-2 xl:grid-cols-5">
        <div className="border-2 border-foreground bg-card p-3 shadow-hard xl:col-span-2">
          <p className="text-xs font-black text-muted-foreground">当前路径</p>
          <PathChangePreview
            fromPath={data.summary.original_path}
            toPath={data.summary.current_path}
            fromLabel="原路径"
            toLabel="当前路径"
            unchangedLabel="当前路径未变化"
            className="mt-1"
          />
        </div>
        <div className="border-2 border-foreground bg-card p-3 shadow-hard xl:col-span-2">
          <p className="text-xs font-black text-muted-foreground">原始路径</p>
          <p className="mt-1 break-all font-mono text-xs font-bold">{data.summary.original_path || '—'}</p>
        </div>
        <div className="border-2 border-foreground bg-card p-3 shadow-hard">
          <p className="text-xs font-black text-muted-foreground">分类 / 状态</p>
          <p className="mt-1 text-sm font-black">{data.summary.category} / {data.summary.status}</p>
          <p className="mt-1 text-[11px] font-bold text-muted-foreground">最近处理：{formatTime(data.summary.last_processed_at)}</p>
        </div>
      </section>

      {isMobile ? (
        flow == null ? (
          <section className="border-2 border-dashed border-foreground bg-card px-4 py-10 text-center shadow-hard">
            <p className="text-sm font-black">暂无文件级去向数据</p>
            <p className="mt-2 text-xs font-bold text-muted-foreground">当前仅展示摘要与时间线信息</p>
          </section>
        ) : (
          <section className="space-y-3 border-2 border-foreground bg-card p-3 shadow-hard">
            <div>
              <h2 className="text-sm font-black">目录分组文件流向摘要</h2>
              <p className="mt-1 text-[11px] font-bold text-muted-foreground">
                小屏幕以卡片摘要展示来源、目标目录和映射关系，避免横向滚动。
              </p>
            </div>
            <div className="space-y-2 border-2 border-foreground bg-muted/10 p-3">
              <p className="text-xs font-black text-muted-foreground">源目录</p>
              <p className="break-all font-mono text-xs font-bold">{flow.source_directory.path || '—'}</p>
            </div>
            <div className="space-y-2">
              {flow.target_directories.map((directory) => {
                const files = directory.id == null ? [] : (targetFilesByDirectoryID.get(directory.id) ?? [])
                return (
                  <div key={directory.id ?? directory.path} className="border-2 border-foreground bg-muted/10 p-3">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                      <p className="text-xs font-black">{formatDirectoryTitle(directory, false)}</p>
                      <span className="rounded-full border border-foreground px-2 py-0.5 text-[10px] font-black">{files.length}</span>
                    </div>
                    <p className="mt-1 break-all font-mono text-[11px] font-bold text-muted-foreground">{directory.path || '—'}</p>
                  </div>
                )
              })}
            </div>
            <div className="space-y-2 border-2 border-foreground bg-muted/10 p-3">
              <p className="text-xs font-black text-muted-foreground">文件映射</p>
              {links.length === 0 ? (
                <p className="text-xs font-bold text-muted-foreground">暂无映射数据</p>
              ) : (
                <ul className="space-y-1">
                  {links.slice(0, 30).map((link) => {
                    const sourceFile = sourceFileByID.get(link.source_file_id)
                    const targetFile = targetFileByID.get(link.target_file_id)
                    return (
                      <li key={link.id} className="border border-foreground/30 bg-background px-2 py-1.5 text-[11px] font-bold">
                        <p className="break-all font-mono text-muted-foreground">{sourceFile?.path || link.source_file_id}</p>
                        <p className="mt-1 break-all font-mono">{targetFile?.path || link.target_file_id}</p>
                      </li>
                    )
                  })}
                </ul>
              )}
            </div>
          </section>
        )
      ) : (
        renderedFlowSection
      )}

      {shouldRenderLegacyFlow && (
        flow == null ? (
        <section className="border-2 border-dashed border-foreground bg-card px-4 py-10 text-center shadow-hard">
          <p className="text-sm font-black">暂无文件级去向数据</p>
          <p className="mt-2 text-xs font-bold text-muted-foreground">当前仅展示摘要与时间线信息</p>
        </section>
      ) : (
        <section className="space-y-3 border-2 border-foreground bg-card p-3 shadow-hard">
          <div className="mb-1 flex flex-wrap items-start justify-between gap-3">
            <div>
              <h2 className="text-sm font-black">目录分组文件流向图</h2>
              <p className="mt-1 text-[11px] font-bold text-muted-foreground">
                将文件名压缩为紧凑标签，重点查看来源文件与目标目录之间的映射连线
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-2 text-[11px] font-black">
              <span className="inline-flex items-center gap-1.5 border border-blue-900/20 bg-blue-100 px-2 py-1 text-blue-900">
                <Sparkles className="h-3.5 w-3.5" />
                源文件 {sourceFiles.length}
              </span>
              <span className="inline-flex items-center gap-1.5 border border-emerald-900/20 bg-emerald-100 px-2 py-1 text-emerald-900">
                <Sparkles className="h-3.5 w-3.5" />
                目标文件 {targetFiles.length}
              </span>
              <span className="inline-flex items-center gap-1.5 border border-foreground/20 bg-muted/40 px-2 py-1 text-foreground">
                <Link2 className="h-3.5 w-3.5" />
                映射 {links.length}
              </span>
              <button
                type="button"
                onClick={() => {
                  setSelectedFile(null)
                  setHoveredFile(null)
                }}
                className="text-xs font-bold text-muted-foreground underline-offset-4 hover:underline"
              >
                清除高亮
              </button>
            </div>
          </div>

          <div className="max-h-[560px] overflow-auto border-2 border-foreground bg-background">
            <div ref={flowCanvasRef} className="relative min-w-[960px] bg-[linear-gradient(90deg,rgba(37,99,235,0.06)_0,rgba(37,99,235,0.06)_23%,transparent_36%,transparent_62%,rgba(5,150,105,0.06)_78%,rgba(5,150,105,0.06)_100%)] p-4">
              <svg className="pointer-events-none absolute inset-0 h-full w-full" aria-hidden="true">
                <g>
                  {linkGeometries.map((geometry) => (
                    <g key={geometry.id}>
                      <path
                        d={geometry.d}
                        fill="none"
                        strokeLinecap="round"
                        className={cn(
                          'transition-all duration-200',
                          relatedLinkIDs.size === 0
                            ? 'stroke-blue-200/60'
                            : relatedLinkIDs.has(geometry.id)
                              ? 'stroke-blue-300/70'
                              : 'stroke-muted-foreground/10',
                        )}
                        strokeWidth={relatedLinkIDs.size === 0 ? 4 : relatedLinkIDs.has(geometry.id) ? 6 : 2}
                      />
                      <path
                        d={geometry.d}
                        fill="none"
                        strokeLinecap="round"
                        className={cn(
                          'transition-all duration-200',
                          relatedLinkIDs.size === 0
                            ? 'stroke-muted-foreground/55'
                            : relatedLinkIDs.has(geometry.id)
                              ? 'stroke-blue-700'
                              : 'stroke-muted-foreground/20',
                        )}
                        strokeWidth={relatedLinkIDs.size === 0 ? 1.5 : relatedLinkIDs.has(geometry.id) ? 2.5 : 1.2}
                      />
                    </g>
                  ))}
                </g>
              </svg>

              <div className="relative z-10 grid grid-cols-[280px_minmax(640px,1fr)] gap-5">
                <div className="space-y-3">
                  <div className="border border-blue-900/15 bg-white/90 p-3 shadow-hard backdrop-blur">
                    <div className="flex items-center justify-between gap-2">
                      <p className="text-xs font-black text-blue-900">{formatDirectoryTitle(flow.source_directory, true)}</p>
                      <span className="border border-blue-900/20 bg-blue-100 px-1.5 py-0.5 text-[10px] font-black text-blue-900">
                        {sourceFiles.length} 项
                      </span>
                    </div>
                    <p className="mt-1 break-all font-mono text-[11px] font-bold text-muted-foreground">{flow.source_directory.path || '—'}</p>
                  </div>

                  <div className="space-y-1.5">
                    {sourceFiles.map((sourceFile) => (
                      <FileItem
                        key={sourceFile.id}
                        file={sourceFile}
                        isActive={highlightedSourceIDs.has(sourceFile.id)}
                        isMuted={relatedLinkIDs.size > 0 && !highlightedSourceIDs.has(sourceFile.id)}
                        linkCount={sourceLinkCountByID.get(sourceFile.id) ?? 0}
                        tone="source"
                        onClick={() => setSelectedFile({ kind: 'source', id: sourceFile.id })}
                        onMouseEnter={() => setHoveredFile({ kind: 'source', id: sourceFile.id })}
                        onMouseLeave={() => setHoveredFile((current) => (current?.kind === 'source' && current.id === sourceFile.id ? null : current))}
                        refCallback={(node) => {
                          sourceFileRefs.current[sourceFile.id] = node
                        }}
                      />
                    ))}
                  </div>
                </div>

                <div className="space-y-4">
                  {flow.target_directories.map((directory) => {
                    const files = directory.id == null ? [] : (targetFilesByDirectoryID.get(directory.id) ?? [])
                    const artifactMeta = resolveTargetArtifactMeta({
                      artifactType: directory.artifact_type,
                      path: directory.path,
                    })
                    return (
                      <div
                        key={directory.id ?? directory.path}
                        className={cn('space-y-2 p-3 shadow-hard backdrop-blur', artifactMeta.panelClassName)}
                      >
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <div className="flex flex-wrap items-center gap-2">
                              <p className={cn('text-xs font-black', artifactMeta.headingClassName)}>{formatDirectoryTitle(directory, false)}</p>
                              <span className={cn('rounded-full border px-2 py-0.5 text-[10px] font-black', artifactMeta.badgeClassName)}>
                                {artifactMeta.label}
                              </span>
                            </div>
                            <p className="mt-1 break-all font-mono text-[11px] font-bold text-muted-foreground">{directory.path || '—'}</p>
                            <p className="mt-1 text-[10px] font-bold text-muted-foreground">{artifactMeta.description}</p>
                          </div>
                          <div className="shrink-0 space-y-1 text-right">
                            <p className={cn('border px-1.5 py-0.5 text-[10px] font-black', artifactMeta.countClassName)}>
                              {files.length} 项
                            </p>
                          </div>
                        </div>
                        <div className="space-y-1.5">
                          {files.map((targetFile) => (
                            <FileItem
                              key={targetFile.id}
                              file={targetFile}
                              isActive={highlightedTargetIDs.has(targetFile.id)}
                              isMuted={relatedLinkIDs.size > 0 && !highlightedTargetIDs.has(targetFile.id)}
                              linkCount={targetLinkCountByID.get(targetFile.id) ?? 0}
                              tone="target"
                              artifactMeta={resolveTargetArtifactMeta({
                                artifactType: targetFile.artifact_type,
                                nodeType: targetFile.node_type,
                                path: targetFile.path,
                              })}
                              onClick={() => setSelectedFile({ kind: 'target', id: targetFile.id })}
                              onMouseEnter={() => setHoveredFile({ kind: 'target', id: targetFile.id })}
                              onMouseLeave={() => setHoveredFile((current) => (current?.kind === 'target' && current.id === targetFile.id ? null : current))}
                              refCallback={(node) => {
                                targetFileRefs.current[targetFile.id] = node
                              }}
                            />
                          ))}
                        </div>
                      </div>
                    )
                  })}
                </div>
              </div>

              {relatedLinkIDs.size > 0 && (
                <div className="pointer-events-none absolute left-1/2 top-3 z-20 -translate-x-1/2">
                  <div className="inline-flex items-center gap-2 border border-blue-900/20 bg-white/95 px-3 py-1.5 text-[11px] font-black text-blue-900 shadow-hard backdrop-blur">
                    <Link2 className="h-3.5 w-3.5" />
                    当前高亮 {relatedLinkIDs.size} 条映射关系
                  </div>
                </div>
              )}
            </div>
          </div>

          <div className="grid grid-cols-1 gap-3 lg:grid-cols-[minmax(0,1fr)_300px]">
            <div className="border-2 border-foreground bg-card p-3 shadow-hard">
              <h3 className="mb-2 text-sm font-black">映射说明</h3>
              <div className="flex flex-wrap items-center gap-2 text-[11px] font-bold text-muted-foreground">
                <span className="inline-flex items-center gap-1.5">
                  <span className="h-2.5 w-8 rounded-full bg-blue-700" />
                  选中或悬停后，仅强化相关映射
                </span>
                <span className="inline-flex items-center gap-1.5">
                  <span className="h-2.5 w-8 rounded-full bg-muted-foreground/40" />
                  未聚焦时保留整体网络感
                </span>
                <span className="inline-flex items-center gap-1.5">
                  <span className="inline-flex h-5 items-center rounded-full border border-foreground/15 bg-muted/40 px-1.5 text-[10px] font-black">MP4</span>
                  文件名已压缩显示，完整路径看右侧详情
                </span>
                <span className="inline-flex items-center gap-1.5">
                  <span className="inline-flex h-5 items-center rounded-full border border-amber-900/20 bg-amber-100 px-1.5 text-[10px] font-black text-amber-900">归档</span>
                  中间归档产物，后续通常还会继续流转
                </span>
                <span className="inline-flex items-center gap-1.5">
                  <span className="inline-flex h-5 items-center rounded-full border border-emerald-900/20 bg-emerald-100 px-1.5 text-[10px] font-black text-emerald-900">最终</span>
                  最终输出产物，表示当前流程的主落位结果
                </span>
              </div>
            </div>

            <div className="border-2 border-foreground bg-card p-3 shadow-hard">
              <h3 className="mb-2 text-sm font-black">详情面板</h3>
              {selectedDetails == null ? (
                <p className="text-xs font-bold text-muted-foreground">点击任一文件条目查看完整路径和运行信息</p>
              ) : (
                <div className="space-y-1.5 text-xs font-bold">
                  <p className="inline-flex items-center gap-1 border-2 border-foreground bg-background px-2 py-1">
                    <Link2 className="h-3.5 w-3.5" />
                    {selectedDetails.title}
                  </p>
                  <p className="inline-flex w-fit items-center border border-foreground/40 bg-muted/30 px-2 py-0.5 text-[11px] font-black text-muted-foreground">
                    当前路径
                  </p>
                  <p>完整路径：<span className="break-all font-mono">{selectedDetails.path || '—'}</span></p>
                  <p>目录：<span className="break-all font-mono">{selectedDetails.directory || '—'}</span></p>
                  <p>节点类型：{selectedDetails.nodeType || '—'}</p>
                  {selectedDetails.artifactLabel && <p>产物阶段：{selectedDetails.artifactLabel}</p>}
                  <p>Workflow Run：{selectedDetails.workflowRunID || '—'}</p>
                  <p>Job：{selectedDetails.jobID || '—'}</p>
                </div>
              )}
            </div>
          </div>
        </section>
      ))}

      <section className="border-2 border-foreground bg-card p-4 shadow-hard">
        <h2 className="mb-4 text-sm font-black">时间线</h2>
        {data.timeline.length === 0 ? (
          <p className="text-xs font-bold text-muted-foreground">暂无时间线事件</p>
        ) : (
          <ol className="space-y-3">
            {data.timeline.map((event) => (
              <li key={event.id} className={cn('border-2 px-3 py-2 text-xs font-bold', TIMELINE_EVENT_COLOR[event.type])}>
                <div className="flex flex-wrap items-center gap-2">
                  <Clock3 className="h-3.5 w-3.5" />
                  <span>{event.title}</span>
                  <span className="ml-auto text-[11px]">{formatTime(event.occurred_at)}</span>
                </div>
                {(event.path_from || event.path_to) && (
                  <PathChangePreview
                    fromPath={event.path_from}
                    toPath={event.path_to}
                    fromLabel="变更前"
                    toLabel="变更后"
                    className="mt-2"
                  />
                )}
                {event.description && (
                  <p className="mt-1 inline-flex items-start gap-1.5">
                    <AlertCircle className="mt-0.5 h-3.5 w-3.5 shrink-0" />
                    <span>{event.description}</span>
                  </p>
                )}
              </li>
            ))}
          </ol>
        )}
      </section>
    </div>
  )
}
