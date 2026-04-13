import { useEffect, useMemo, useRef, useState } from 'react'
import { AlertCircle, ArrowLeft, Clock3, Link2 } from 'lucide-react'
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
  onClick,
  refCallback,
}: {
  file: FolderLineageSourceFile | FolderLineageFile
  isActive: boolean
  onClick: () => void
  refCallback: (node: HTMLButtonElement | null) => void
}) {
  return (
    <button
      ref={refCallback}
      type="button"
      onClick={onClick}
      className={cn(
        'w-full truncate border-2 px-3 py-2 text-left text-xs font-black transition-all',
        isActive
          ? 'border-foreground bg-foreground text-background shadow-hard'
          : 'border-foreground bg-background text-foreground hover:bg-muted/60',
      )}
      title={file.path}
    >
      {file.name}
    </button>
  )
}

export default function FolderLineagePage() {
  const { id } = useParams<{ id: string }>()
  const [data, setData] = useState<FolderLineageResponse | null>(null)
  const [selectedFile, setSelectedFile] = useState<SelectedFile>(null)
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

  const relatedLinkIDs = useMemo(() => {
    const ids = new Set<string>()
    if (selectedFile == null) return ids
    for (const link of links) {
      if (selectedFile.kind === 'source' && link.source_file_id === selectedFile.id) {
        ids.add(link.id)
      }
      if (selectedFile.kind === 'target' && link.target_file_id === selectedFile.id) {
        ids.add(link.id)
      }
    }
    return ids
  }, [links, selectedFile])

  const highlightedSourceIDs = useMemo(() => {
    const ids = new Set<string>()
    if (selectedFile?.kind === 'source') {
      ids.add(selectedFile.id)
    }
    for (const link of links) {
      if (relatedLinkIDs.has(link.id)) {
        ids.add(link.source_file_id)
      }
    }
    return ids
  }, [links, relatedLinkIDs, selectedFile])

  const highlightedTargetIDs = useMemo(() => {
    const ids = new Set<string>()
    if (selectedFile?.kind === 'target') {
      ids.add(selectedFile.id)
    }
    for (const link of links) {
      if (relatedLinkIDs.has(link.id)) {
        ids.add(link.target_file_id)
      }
    }
    return ids
  }, [links, relatedLinkIDs, selectedFile])

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
          <div className="mb-1 flex items-center justify-between">
            <h2 className="text-sm font-black">目录分组文件流向图</h2>
            <button
              type="button"
              onClick={() => setSelectedFile(null)}
              className="text-xs font-bold text-muted-foreground underline-offset-4 hover:underline"
            >
              清除高亮
            </button>
          </div>

          <div className="max-h-[560px] overflow-auto border-2 border-foreground bg-background">
            <div ref={flowCanvasRef} className="relative min-w-[980px] p-4">
              <svg className="pointer-events-none absolute inset-0 h-full w-full" aria-hidden="true">
                <g>
                  {linkGeometries.map((geometry) => (
                    <path
                      key={geometry.id}
                      d={geometry.d}
                      fill="none"
                      className={cn(
                        'transition-all',
                        relatedLinkIDs.size === 0
                          ? 'stroke-muted-foreground/50 stroke-[1.5]'
                          : relatedLinkIDs.has(geometry.id)
                            ? 'stroke-blue-700 stroke-[2.5]'
                            : 'stroke-muted-foreground/20 stroke-[1.2]',
                      )}
                    />
                  ))}
                </g>
              </svg>

              <div className="relative z-10 grid grid-cols-[320px_minmax(620px,1fr)] gap-6">
                <div className="space-y-3">
                  <div className="border-2 border-foreground bg-card p-3 shadow-hard">
                    <p className="text-xs font-black text-muted-foreground">{formatDirectoryTitle(flow.source_directory, true)}</p>
                    <p className="mt-1 break-all font-mono text-[11px] font-bold text-muted-foreground">{flow.source_directory.path || '—'}</p>
                  </div>

                  <div className="space-y-2">
                    {sourceFiles.map((sourceFile) => (
                      <FileItem
                        key={sourceFile.id}
                        file={sourceFile}
                        isActive={highlightedSourceIDs.has(sourceFile.id)}
                        onClick={() => setSelectedFile({ kind: 'source', id: sourceFile.id })}
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
                      <div key={directory.id ?? directory.path} className="space-y-2 border-2 border-foreground bg-card p-3 shadow-hard">
                        <div>
                          <p className="text-xs font-black text-muted-foreground">{formatDirectoryTitle(directory, false)}</p>
                          <p className="mt-1 break-all font-mono text-[11px] font-bold text-muted-foreground">{directory.path || '—'}</p>
                          {artifactTypeLabel && (
                            <p className="mt-1 text-[11px] font-black text-muted-foreground">{artifactTypeLabel}</p>
                          )}
                        </div>
                        <div className="space-y-2">
                          {files.map((targetFile) => (
                            <FileItem
                              key={targetFile.id}
                              file={targetFile}
                              isActive={highlightedTargetIDs.has(targetFile.id)}
                              onClick={() => setSelectedFile({ kind: 'target', id: targetFile.id })}
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
