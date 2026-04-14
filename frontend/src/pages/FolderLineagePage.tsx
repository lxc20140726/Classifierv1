import { useEffect, useMemo, useRef, useState } from 'react'
import { AlertCircle, ArrowLeft, Clock3, Link2, Sparkles } from 'lucide-react'
import { Link, useParams } from 'react-router-dom'

import { ApiRequestError } from '@/api/client'
import { getFolderLineage } from '@/api/folders'
import { PathChangePreview } from '@/components/PathChangePreview'
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

interface LinkGeometry {
  id: string
  d: string
}

const EMPTY_SOURCE_FILES: FolderLineageSourceFile[] = []
const EMPTY_TARGET_FILES: FolderLineageFile[] = []
const EMPTY_LINKS: FolderLineageFlow['links'] = []

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

function buildLinkGeometries(
  flow: FolderLineageFlow,
  container: HTMLDivElement,
  sourceRefs: Record<string, HTMLButtonElement | null>,
  targetRefs: Record<string, HTMLButtonElement | null>,
) {
  const bounds = container.getBoundingClientRect()
  const geometries: LinkGeometry[] = []

  for (const link of flow.links) {
    const sourceEl = sourceRefs[link.source_file_id]
    const targetEl = targetRefs[link.target_file_id]
    if (sourceEl == null || targetEl == null) {
      continue
    }
    const sourceRect = sourceEl.getBoundingClientRect()
    const targetRect = targetEl.getBoundingClientRect()
    const x1 = sourceRect.right - bounds.left
    const y1 = sourceRect.top + (sourceRect.height / 2) - bounds.top
    const x2 = targetRect.left - bounds.left
    const y2 = targetRect.top + (targetRect.height / 2) - bounds.top
    const deltaX = Math.max(42, (x2 - x1) * 0.35)
    const c1x = x1 + deltaX
    const c2x = x2 - deltaX

    geometries.push({
      id: link.id,
      d: `M ${x1} ${y1} C ${c1x} ${y1}, ${c2x} ${y2}, ${x2} ${y2}`,
    })
  }

  return geometries
}

function FileItem({
  file,
  isActive,
  isMuted,
  linkCount,
  tone,
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
  onClick: () => void
  onMouseEnter: () => void
  onMouseLeave: () => void
  refCallback: (node: HTMLButtonElement | null) => void
}) {
  const extension = getFileExtension(file.name)
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
            : 'border-emerald-900 bg-emerald-950 text-white shadow-hard -translate-y-0.5'
          : 'border-foreground/20 bg-background/90 text-foreground hover:border-foreground/50 hover:bg-muted/50',
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
              : 'border-emerald-900/20 bg-emerald-100 text-emerald-900',
        )}
      >
        {tone === 'source' ? '源' : '达'}
      </span>
      <span className="min-w-0 flex-1 truncate">{compactFileName(file.name)}</span>
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
          isActive ? 'bg-white/10 text-white/90' : 'bg-foreground text-background',
        )}
      >
        <Link2 className="h-3 w-3" />
        {linkCount}
      </span>
    </button>
  )
}

export default function FolderLineagePage() {
  const { id } = useParams<{ id: string }>()
  const [data, setData] = useState<FolderLineageResponse | null>(null)
  const [selectedFile, setSelectedFile] = useState<SelectedFile>(null)
  const [hoveredFile, setHoveredFile] = useState<SelectedFile>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [linkGeometries, setLinkGeometries] = useState<LinkGeometry[]>([])

  const flowCanvasRef = useRef<HTMLDivElement | null>(null)
  const sourceFileRefs = useRef<Record<string, HTMLButtonElement | null>>({})
  const targetFileRefs = useRef<Record<string, HTMLButtonElement | null>>({})

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

  const activeFile = hoveredFile ?? selectedFile

  const relatedLinkIDs = useMemo(() => {
    const ids = new Set<string>()
    if (activeFile == null) return ids
    for (const link of links) {
      if (activeFile.kind === 'source' && link.source_file_id === activeFile.id) {
        ids.add(link.id)
      }
      if (activeFile.kind === 'target' && link.target_file_id === activeFile.id) {
        ids.add(link.id)
      }
    }
    return ids
  }, [activeFile, links])

  const highlightedSourceIDs = useMemo(() => {
    const ids = new Set<string>()
    if (activeFile?.kind === 'source') {
      ids.add(activeFile.id)
    }
    for (const link of links) {
      if (relatedLinkIDs.has(link.id)) {
        ids.add(link.source_file_id)
      }
    }
    return ids
  }, [activeFile, links, relatedLinkIDs])

  const highlightedTargetIDs = useMemo(() => {
    const ids = new Set<string>()
    if (activeFile?.kind === 'target') {
      ids.add(activeFile.id)
    }
    for (const link of links) {
      if (relatedLinkIDs.has(link.id)) {
        ids.add(link.target_file_id)
      }
    }
    return ids
  }, [activeFile, links, relatedLinkIDs])

  const selectedDetails = useMemo(() => {
    if (selectedFile == null) return null
    if (selectedFile.kind === 'source') {
      const sourceFile = sourceFileByID.get(selectedFile.id)
      if (sourceFile == null) return null
      const relatedTargets = links
        .filter((link) => link.source_file_id === sourceFile.id)
        .map((link) => targetFileByID.get(link.target_file_id))
        .filter((item): item is FolderLineageFile => item != null)
      return {
        title: sourceFile.name,
        path: sourceFile.path,
        directory: flow?.source_directory.path ?? '',
        nodeType: relatedTargets[0]?.node_type ?? '',
        workflowRunID: relatedTargets[0]?.workflow_run_id ?? '',
        jobID: relatedTargets[0]?.job_id ?? '',
      }
    }

    const targetFile = targetFileByID.get(selectedFile.id)
    if (targetFile == null) return null
    const directoryPath = flow?.target_directories.find((item) => item.id === targetFile.directory_id)?.path ?? ''
    return {
      title: targetFile.name,
      path: targetFile.path,
      directory: directoryPath,
      nodeType: targetFile.node_type ?? '',
      workflowRunID: targetFile.workflow_run_id ?? '',
      jobID: targetFile.job_id ?? '',
    }
  }, [flow, links, selectedFile, sourceFileByID, targetFileByID])

  useEffect(() => {
    if (flow == null) {
      return
    }
    const container = flowCanvasRef.current
    if (container == null) {
      return
    }

    let frameID = 0
    const compute = () => {
      setLinkGeometries(buildLinkGeometries(flow, container, sourceFileRefs.current, targetFileRefs.current))
    }
    const schedule = () => {
      cancelAnimationFrame(frameID)
      frameID = window.requestAnimationFrame(compute)
    }

    schedule()
    const observer = new ResizeObserver(schedule)
    observer.observe(container)
    for (const sourceFile of sourceFiles) {
      const sourceEl = sourceFileRefs.current[sourceFile.id]
      if (sourceEl != null) observer.observe(sourceEl)
    }
    for (const targetFile of targetFiles) {
      const targetEl = targetFileRefs.current[targetFile.id]
      if (targetEl != null) observer.observe(targetEl)
    }
    window.addEventListener('resize', schedule)

    return () => {
      cancelAnimationFrame(frameID)
      observer.disconnect()
      window.removeEventListener('resize', schedule)
    }
  }, [flow, sourceFiles, targetFiles])

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

      {flow == null ? (
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
                    const artifactTypeLabel = resolveArtifactTypeLabel(directory.artifact_type)
                    return (
                      <div
                        key={directory.id ?? directory.path}
                        className="space-y-2 border border-emerald-900/15 bg-white/90 p-3 shadow-hard backdrop-blur"
                      >
                        <div className="flex items-start justify-between gap-3">
                          <div>
                            <p className="text-xs font-black text-emerald-900">{formatDirectoryTitle(directory, false)}</p>
                            <p className="mt-1 break-all font-mono text-[11px] font-bold text-muted-foreground">{directory.path || '—'}</p>
                          </div>
                          <div className="shrink-0 space-y-1 text-right">
                            <p className="border border-emerald-900/20 bg-emerald-100 px-1.5 py-0.5 text-[10px] font-black text-emerald-900">
                              {files.length} 项
                            </p>
                            {artifactTypeLabel && (
                              <p className="text-[10px] font-black text-muted-foreground">{artifactTypeLabel}</p>
                            )}
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
                  <p>Workflow Run：{selectedDetails.workflowRunID || '—'}</p>
                  <p>Job：{selectedDetails.jobID || '—'}</p>
                </div>
              )}
            </div>
          </div>
        </section>
      )}

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
