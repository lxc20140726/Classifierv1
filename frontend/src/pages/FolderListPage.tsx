import { useEffect, useState, useRef } from 'react'
import {
  Clock,
  FileText,
  FolderOpen,
  Grid2X2,
  History,
  Link2,
  List,
  Loader2,
  Search,
  X,
} from 'lucide-react'
import gsap from 'gsap'
import { createPortal } from 'react-dom'
import { useNavigate } from 'react-router-dom'

import { startWorkflowJob } from '@/api/workflowRuns'
import { SnapshotDrawer } from '@/components/SnapshotDrawer'
import { WorkflowRunStatusCard } from '@/components/WorkflowRunStatusCard'
import { useIsMobile } from '@/hooks/useIsMobile'
import {
  getWorkflowFolderLaunchability,
  launchWorkflowForFolder,
} from '@/lib/workflowFolderLaunch'
import { cn } from '@/lib/utils'
import { useActivityStore } from '@/store/activityStore'
import { useFolderStore } from '@/store/folderStore'
import { useJobStore } from '@/store/jobStore'
import { useWorkflowDefStore } from '@/store/workflowDefStore'
import { useWorkflowRunStore } from '@/store/workflowRunStore'
import type { Category, Folder, FolderStatus, Job, WorkflowGraph, WorkflowStageStatus } from '@/types'

type SortableFolderColumn = 'updated_at' | 'total_size'

const CATEGORY_LABEL: Record<Category | '', string> = {
  '': '鍏ㄩ儴鍒嗙被',
  photo: '鍐欑湡',
  video: '瑙嗛',
  mixed: '娣峰悎',
  manga: '婕敾',
  other: '鍏朵粬',
}

const CATEGORY_COLOR: Record<Category, string> = {
  photo: 'bg-pink-200 text-pink-900 border-2 border-foreground',
  video: 'bg-blue-200 text-blue-900 border-2 border-foreground',
  mixed: 'bg-purple-200 text-purple-900 border-2 border-foreground',
  manga: 'bg-orange-200 text-orange-900 border-2 border-foreground',
  other: 'bg-gray-200 text-gray-900 border-2 border-foreground',
}

const STATUS_LABEL: Record<FolderStatus | '', string> = {
  '': '全部状态',
  pending: '待处理',
  done: '已完成',
  skip: '跳过',
}

const STATUS_COLOR: Record<FolderStatus, string> = {
  pending: 'bg-yellow-300 text-yellow-900 border-2 border-foreground',
  done: 'bg-green-300 text-green-900 border-2 border-foreground',
  skip: 'bg-gray-300 text-gray-900 border-2 border-foreground',
}

const JOB_STATUS_LABEL: Record<string, string> = {
  pending: '等待中',
  running: '进行中',
  succeeded: '已完成',
  failed: '失败',
  partial: '部分成功',
  cancelled: '已取消',
}

const JOB_STATUS_COLOR: Record<string, string> = {
  pending: 'bg-gray-200 text-gray-900 border-2 border-foreground',
  running: 'bg-blue-300 text-blue-900 border-2 border-foreground',
  succeeded: 'bg-green-300 text-green-900 border-2 border-foreground',
  failed: 'bg-red-300 text-red-900 border-2 border-foreground',
  partial: 'bg-yellow-300 text-yellow-900 border-2 border-foreground',
  cancelled: 'bg-gray-300 text-gray-900 border-2 border-foreground',
}

const WORKFLOW_STATUS_COLOR: Record<WorkflowStageStatus, string> = {
  not_run: 'bg-gray-200 text-gray-900 border-2 border-foreground',
  running: 'bg-blue-300 text-blue-900 border-2 border-foreground',
  succeeded: 'bg-green-300 text-green-900 border-2 border-foreground',
  failed: 'bg-red-300 text-red-900 border-2 border-foreground',
  waiting_input: 'bg-yellow-300 text-yellow-900 border-2 border-foreground',
  partial: 'bg-yellow-300 text-yellow-900 border-2 border-foreground',
  rolled_back: 'bg-gray-300 text-gray-900 border-2 border-foreground',
}

const CLASSIFICATION_WORKFLOW_LABEL: Record<WorkflowStageStatus, string> = {
  not_run: '未分类流程',
  running: '分类中',
  succeeded: '分类完成',
  failed: '分类失败',
  waiting_input: '待确认',
  partial: '分类部分完成',
  rolled_back: '分类已回退',
}

const PROCESSING_WORKFLOW_LABEL: Record<WorkflowStageStatus, string> = {
  not_run: '未处理流程',
  running: '处理中',
  succeeded: '处理完成',
  failed: '处理失败',
  waiting_input: '待确认',
  partial: '处理部分完成',
  rolled_back: '已回退',
}

const ALL_CATEGORIES: Array<Category | ''> = ['', 'photo', 'video', 'mixed', 'manga', 'other']
const ALL_STATUSES: Array<FolderStatus | ''> = ['', 'pending', 'done', 'skip']

function formatBytes(value: number): string {
  if (value < 1024) return `${value} B`
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`
  if (value < 1024 * 1024 * 1024) return `${(value / (1024 * 1024)).toFixed(1)} MB`
  return `${(value / (1024 * 1024 * 1024)).toFixed(1)} GB`
}

function formatRelativeTime(iso: string): string {
  if (!iso) return ''
  const diff = Date.now() - new Date(iso).getTime()
  const mins = Math.floor(diff / 60000)
  if (mins < 1) return '刚刚'
  if (mins < 60) return `${mins} 分钟前`
  const hrs = Math.floor(mins / 60)
  if (hrs < 24) return `${hrs} 小时前`
  return `${Math.floor(hrs / 24)} 天前`
}

function formatDateTime(iso: string): string {
  if (!iso) return '--'
  return new Date(iso).toLocaleString('zh-CN')
}

function getSortLabel(active: boolean, descending: boolean): string {
  if (!active) return '↕'
  return descending ? '↓' : '↑'
}

interface FolderWorkflowLaunchDialogState {
  open: boolean
  folderIds: string[]
  mode: 'all' | 'classification' | 'processing'
}

function countEnabledNodes(graphJSON: string) {
  try {
    const parsed = JSON.parse(graphJSON) as Partial<WorkflowGraph>
    const nodes = Array.isArray(parsed.nodes) ? parsed.nodes : []
    return nodes.filter((node) => node && node.enabled !== false).length
  } catch {
    return 0
  }
}

const CLASSIFICATION_NODE_TYPES = new Set([
  'classification-writer',
  'classification-db-result-preview',
  'file-tree-classifier',
  'name-keyword-classifier',
  'ext-ratio-classifier',
  'signal-aggregator',
  'confidence-check',
  'classification-reader',
  'db-subtree-reader',
])

const PROCESSING_NODE_TYPES = new Set([
  'rename-node',
  'move-node',
  'compress-node',
  'thumbnail-node',
  'processing-result-preview',
])

function workflowMatchesLaunchMode(graphJSON: string, mode: FolderWorkflowLaunchDialogState['mode']) {
  if (mode === 'all') return true
  try {
    const parsed = JSON.parse(graphJSON) as Partial<WorkflowGraph>
    const nodes = Array.isArray(parsed.nodes) ? parsed.nodes : []
    const enabledTypes = new Set(
      nodes
        .filter((node) => node && node.enabled !== false)
        .map((node) => String(node.type ?? '').trim())
        .filter((nodeType) => nodeType !== ''),
    )
    if (mode === 'classification') {
      return [...enabledTypes].some((nodeType) => CLASSIFICATION_NODE_TYPES.has(nodeType))
    }
    return [...enabledTypes].some((nodeType) => PROCESSING_NODE_TYPES.has(nodeType))
  } catch {
    return false
  }
}

function buildJobHistoryLink(jobId: string, workflowRunId?: string) {
  const query = new URLSearchParams()
  query.set('job_id', jobId)
  if (workflowRunId && workflowRunId.trim() !== '') {
    query.set('workflow_run_id', workflowRunId)
  }
  return `/job-history?${query.toString()}`
}

function ScanProgressBanner() {
  const isScanning = useFolderStore((s) => s.isScanning)
  const scanProgress = useFolderStore((s) => s.scanProgress)

  if (!isScanning) return null

  const scanned = scanProgress?.scanned ?? 0
  const total = scanProgress?.total ?? 0
  const pct = total > 0 ? Math.round((scanned / total) * 100) : 0

  return (
    <div className="border-2 border-foreground bg-blue-100 px-4 py-3 shadow-hard mb-4">
      <div className="flex items-center gap-2 text-sm text-blue-900">
        <Loader2 className="h-5 w-5 shrink-0 animate-spin" />
        <span className="font-black">姝ｅ湪鎵弿</span>
        {scanProgress?.currentFolderName != null && (
          <span className="truncate font-mono font-bold">{scanProgress.currentFolderName}</span>
        )}
        <span className="ml-auto shrink-0 text-xs font-black tabular-nums">
          {scanned}&nbsp;/&nbsp;{total > 0 ? total : '?'}
        </span>
      </div>
      {total > 0 && (
        <div className="mt-3 h-2 w-full overflow-hidden border-2 border-foreground bg-blue-200">
          <div
            className="h-full bg-blue-600 transition-all duration-300"
            style={{ width: `${pct}%` }}
          />
        </div>
      )}
    </div>
  )
}

function JobItem({ job }: { job: Job }) {
  const pct = job.total > 0 ? Math.round((job.done / job.total) * 100) : 0
  const statusLabel = JOB_STATUS_LABEL[job.status] ?? job.status
  const statusColor = JOB_STATUS_COLOR[job.status] ?? 'bg-gray-200 text-gray-900 border-2 border-foreground'

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between gap-2">
        <span className="truncate text-xs font-bold">
          {job.type === 'move' ? '绉诲姩浠诲姟' : job.type}
        </span>
        <span className={cn('shrink-0 px-2 py-0.5 text-[10px] font-black', statusColor)}>
          {statusLabel}
        </span>
      </div>
      {(job.status === 'running' || job.status === 'partial') && (
        <div className="h-1.5 w-full overflow-hidden border-2 border-foreground bg-muted">
          <div
            className="h-full bg-foreground transition-all duration-300"
            style={{ width: `${pct}%` }}
          />
        </div>
      )}
      <p className="text-xs font-medium text-muted-foreground">
        <span className="tabular-nums font-bold text-foreground">{job.done}/{job.total} 项</span>
        {job.failed > 0 && <span className="text-red-600 font-bold"> 路 {job.failed} 澶辫触</span>}
        {job.created_at ? <span> 路 {formatRelativeTime(job.created_at)}</span> : null}
      </p>
    </div>
  )
}

function RecentJobsPanel() {
  const jobs = useJobStore((s) => s.jobs)
  const fetchJobs = useJobStore((s) => s.fetchJobs)

  useEffect(() => {
    void fetchJobs({ limit: 5 })
  }, [fetchJobs])

  return (
    <section className="border-2 border-foreground bg-card p-4 shadow-hard">
      <div className="mb-4 flex items-center gap-2 border-b-2 border-foreground pb-2">
        <Clock className="h-5 w-5 text-foreground" />
        <h3 className="text-base font-black tracking-tight">最近任务</h3>
      </div>
      {jobs.length === 0 ? (
        <p className="text-xs font-medium text-muted-foreground py-4 text-center">鏆傛棤浠诲姟璁板綍</p>
      ) : (
        <ul className="divide-y-2 divide-foreground">
          {jobs.slice(0, 5).map((job) => (
            <li key={job.id} className="py-3 first:pt-0 last:pb-0">
              <JobItem job={job} />
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}

function RecentLogsPanel() {
  const logs = useActivityStore((s) => s.logs)
  const fetchLogs = useActivityStore((s) => s.fetchLogs)

  useEffect(() => {
    void fetchLogs({ limit: 5 })
  }, [fetchLogs])

  return (
    <section className="border-2 border-foreground bg-card p-4 shadow-hard">
      <div className="mb-4 flex items-center gap-2 border-b-2 border-foreground pb-2">
        <FileText className="h-5 w-5 text-foreground" />
        <h3 className="text-base font-black tracking-tight">最近日志</h3>
      </div>
      {logs.length === 0 ? (
        <p className="text-xs font-medium text-muted-foreground py-4 text-center">鏆傛棤鎿嶄綔鏃ュ織</p>
      ) : (
        <ul className="divide-y-2 divide-foreground">
          {logs.slice(0, 5).map((log) => (
            <li key={log.id} className="space-y-1.5 py-3 first:pt-0 last:pb-0">
              <div className="flex items-center justify-between gap-2">
                <span className="truncate text-xs font-bold">{log.action}</span>
                <span
                  className={cn(
                    'shrink-0 border-2 border-foreground px-1.5 py-0.5 text-[10px] font-black',
                    log.result === 'success'
                      ? 'bg-green-300 text-green-900'
                      : log.result === 'failed'
                        ? 'bg-red-300 text-red-900'
                        : 'bg-gray-200 text-gray-900',
                  )}
                >
                  {log.result === 'success' ? '鎴愬姛' : log.result === 'failed' ? '澶辫触' : log.result}
                </span>
              </div>
              {log.folder_path ? (
                <p className="truncate font-mono text-[10px] text-muted-foreground">{log.folder_path}</p>
              ) : null}
              <p className="text-[10px] font-bold text-muted-foreground">{formatRelativeTime(log.created_at)}</p>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}

function WorkflowSummaryBadges({ folder }: { folder: Folder }) {
  const classificationStatus = folder.workflow_summary.classification.status
  const processingStatus = folder.workflow_summary.processing.status

  return (
    <>
      <span className={cn('px-2 py-0.5 text-xs font-bold', WORKFLOW_STATUS_COLOR[classificationStatus])}>
        分类流程：{CLASSIFICATION_WORKFLOW_LABEL[classificationStatus]}
      </span>
      <span className={cn('px-2 py-0.5 text-xs font-bold', WORKFLOW_STATUS_COLOR[processingStatus])}>
        处理流程：{PROCESSING_WORKFLOW_LABEL[processingStatus]}
      </span>
    </>
  )
}

interface FolderActionProps {
  folder: Folder
  selected: boolean
  onToggleSelect: () => void
  onLaunchWorkflow: () => void
  onOpenLiveClassification: () => void
  onOpenLineage: () => void
  onSnapshot: () => void
  onUpdateCategory: (c: Category) => void
  onUpdateStatus: (s: FolderStatus) => void
  onRemove: () => void
  onRestore: () => void
}

function FolderCard({
  folder,
  selected,
  onToggleSelect,
  onLaunchWorkflow,
  onOpenLiveClassification,
  onOpenLineage,
  onSnapshot,
  onUpdateCategory,
  onUpdateStatus,
  onRemove,
  onRestore,
}: FolderActionProps) {
  const isDeleted = folder.deleted_at !== null

  return (
    <div
      className={cn(
        'folder-card flex flex-col border-2 bg-card p-4 transition-all duration-200',
        selected ? 'border-foreground shadow-hard-hover -translate-y-1' : 'border-foreground shadow-hard hover:-translate-y-0.5 hover:shadow-hard-hover',
        isDeleted && 'opacity-60 bg-muted/50',
      )}
    >
      <div className="flex items-start justify-between gap-2">
        <label className="flex min-w-0 cursor-pointer items-center gap-3">
          <input
            type="checkbox"
            checked={selected}
            onChange={onToggleSelect}
            className="h-4 w-4 shrink-0 rounded-none border-2 border-foreground text-foreground focus:ring-foreground focus:ring-offset-0"
          />
          <FolderOpen className="h-5 w-5 shrink-0 text-foreground" />
          <span className="truncate text-base font-black tracking-tight" title={folder.name}>
            {folder.name}
          </span>
        </label>
        {isDeleted && (
          <span className="shrink-0 border-2 border-red-900 bg-red-200 px-1.5 py-0.5 text-[10px] font-black text-red-900">已隐藏</span>
        )}
      </div>

      <div className="mt-3 flex flex-wrap gap-2">
        <span className={cn('px-2 py-0.5 text-xs font-bold', CATEGORY_COLOR[folder.category])}>
          {CATEGORY_LABEL[folder.category]}
        </span>
        <span className={cn('px-2 py-0.5 text-xs font-bold', STATUS_COLOR[folder.status])}>
          {STATUS_LABEL[folder.status]}
        </span>
        <WorkflowSummaryBadges folder={folder} />
        {folder.has_other_files === true && (
          <span className="border-2 border-amber-900 bg-amber-200 px-2 py-0.5 text-xs font-bold text-amber-900">含其他文件</span>
        )}
        {folder.category_source === 'manual' && (
          <span className="border-2 border-indigo-900 bg-indigo-200 px-2 py-0.5 text-xs font-bold text-indigo-900">鎵嬪姩</span>
        )}
      </div>

      <p className="mt-3 break-all font-mono text-[11px] font-bold text-muted-foreground">{folder.path}</p>
      <div className="mt-4 grid grid-cols-3 gap-2 text-center">
        <div className="border-2 border-foreground bg-muted/30 p-1.5">
          <p className="text-[10px] font-bold text-muted-foreground">鍥剧墖</p>
          <p className="text-sm font-black tabular-nums">{folder.image_count}</p>
        </div>
        <div className="border-2 border-foreground bg-muted/30 p-1.5">
          <p className="text-[10px] font-bold text-muted-foreground">瑙嗛</p>
          <p className="text-sm font-black tabular-nums">{folder.video_count}</p>
        </div>
        <div className="border-2 border-foreground bg-muted/30 p-1.5">
          <p className="text-[10px] font-bold text-muted-foreground">澶у皬</p>
          <p className="text-sm font-black">{formatBytes(folder.total_size)}</p>
        </div>
      </div>

      <div className="mt-4 flex flex-wrap items-center gap-2 border-t-2 border-foreground pt-4">
        {isDeleted ? (
          <button
            type="button"
            onClick={onRestore}
            className="w-full border-2 border-foreground bg-background px-2 py-1.5 text-xs font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 sm:w-auto sm:min-w-28"
          >
            鎭㈠鎵弿
          </button>
        ) : (
          <>
            <button
              type="button"
              onClick={onLaunchWorkflow}
              className="w-full border-2 border-foreground bg-green-300 px-2 py-1.5 text-xs font-bold text-green-900 transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 sm:w-auto sm:min-w-28"
            >
              鍚姩宸ヤ綔娴?            </button>
            <button
              type="button"
              onClick={onOpenLiveClassification}
              className="w-full border-2 border-foreground bg-background px-2 py-1.5 text-xs font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 sm:w-auto"
            >
              瀹炴椂鍒嗙被
            </button>
            <button
              type="button"
              onClick={onOpenLineage}
              className="w-full border-2 border-foreground bg-background px-2 py-1.5 text-xs font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 sm:w-auto"
            >
              璺緞鍏崇郴
            </button>
            <select
              value={folder.category}
              onChange={(e) => onUpdateCategory(e.target.value as Category)}
              className="w-full min-w-0 border-2 border-foreground bg-background px-2 py-1.5 text-xs font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-1 sm:w-auto sm:flex-1 sm:min-w-[9rem]"
              aria-label="鏇存敼鍒嗙被"
            >
              {(['photo', 'video', 'mixed', 'manga', 'other'] as Category[]).map((c) => (
                <option key={c} value={c}>{CATEGORY_LABEL[c]}</option>
              ))}
            </select>
            <select
              value={folder.status}
              onChange={(e) => onUpdateStatus(e.target.value as FolderStatus)}
              className="w-full min-w-0 border-2 border-foreground bg-background px-2 py-1.5 text-xs font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-1 sm:w-auto sm:flex-1 sm:min-w-[9rem]"
              aria-label="更改状态"
            >
              {(['pending', 'done', 'skip'] as FolderStatus[]).map((s) => (
                <option key={s} value={s}>{STATUS_LABEL[s]}</option>
              ))}
            </select>
            <button
              type="button"
              onClick={onSnapshot}
              title="查看快照时间线"
              className="inline-flex h-8 w-8 items-center justify-center border-2 border-foreground bg-background p-1.5 transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
            >
              <History className="h-4 w-4" />
            </button>
            <button
              type="button"
              onClick={onRemove}
              title="浠庤蒋浠朵腑闅愯棌锛屼笉鏀瑰姩瀹為檯鏂囦欢"
              className="inline-flex h-8 w-8 items-center justify-center border-2 border-red-900 bg-red-100 p-1.5 text-red-900 transition-all hover:bg-red-900 hover:text-red-100 hover:shadow-hard hover:-translate-y-0.5"
            >
              <X className="h-4 w-4" />
            </button>
          </>
        )}
      </div>
    </div>
  )
}

function FolderRow({
  folder,
  selected,
  onToggleSelect,
  onLaunchWorkflow,
  onOpenLiveClassification,
  onOpenLineage,
  onSnapshot,
  onUpdateCategory,
  onUpdateStatus,
  onRemove,
  onRestore,
}: FolderActionProps) {
  const isDeleted = folder.deleted_at !== null
  const dotRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (dotRef.current) {
      // 绮掑瓙椋炲叆鎴愪负鍒楄〃鍦嗙偣鐨勫姩鏁?
      gsap.fromTo(dotRef.current, 
        { 
          scale: 0,
          x: () => (Math.random() - 0.5) * 80,
          y: () => (Math.random() - 0.5) * 80,
          opacity: 0
        }, 
        { 
          scale: 1, 
          x: 0,
          y: 0,
          opacity: 1,
          duration: 0.6, 
          ease: "expo.out", 
          delay: Math.random() * 0.2 
        }
      )
    }
  }, [])

  return (
    <tr
      className={cn(
        'folder-row border-b-2 border-foreground transition-colors hover:bg-muted/30',
        isDeleted && 'opacity-60 bg-muted/10',
      )}
    >
      <td className="w-12 px-4 py-4">
        <input
          type="checkbox"
          checked={selected}
          onChange={onToggleSelect}
          className="h-4 w-4 rounded-none border-2 border-foreground text-foreground focus:ring-foreground focus:ring-offset-0"
        />
      </td>
      <td className="px-4 py-4">
        <div className="flex items-center gap-3">
          <div ref={dotRef} className="h-2.5 w-2.5 rounded-full bg-foreground shrink-0 shadow-[2px_2px_0px_rgba(0,0,0,0.2)]" />
          <span className="max-w-[18rem] break-all text-sm font-black tracking-tight" title={folder.name}>
            {folder.name}
          </span>
        </div>
      </td>
      <td className="px-4 py-4">
        <div className="flex flex-wrap gap-2">
          <span className={cn('px-2 py-0.5 text-xs font-bold', CATEGORY_COLOR[folder.category])}>
            {CATEGORY_LABEL[folder.category]}
          </span>
          <span className={cn('px-2 py-0.5 text-xs font-bold', STATUS_COLOR[folder.status])}>
            {STATUS_LABEL[folder.status]}
          </span>
          <WorkflowSummaryBadges folder={folder} />
          {folder.has_other_files === true && (
            <span className="border-2 border-amber-900 bg-amber-200 px-2 py-0.5 text-xs font-bold text-amber-900">含其他文件</span>
          )}
        </div>
      </td>
      <td className="hidden px-4 py-4 text-xs font-bold text-muted-foreground sm:table-cell">
        <span className="tabular-nums text-foreground">{folder.image_count}</span> 鍥?
        <span className="mx-2">路</span>
        <span className="tabular-nums text-foreground">{folder.video_count}</span> 瑙?
      </td>
      <td className="hidden px-4 py-4 text-xs font-mono font-bold text-foreground md:table-cell">
        {formatBytes(folder.total_size)}
      </td>
      <td className="hidden px-4 py-4 text-xs font-bold text-muted-foreground lg:table-cell">
        <div className="text-foreground">{formatDateTime(folder.updated_at)}</div>
        <div className="mt-1">{formatRelativeTime(folder.updated_at)}</div>
      </td>
      <td className="px-4 py-4">
        <div className="flex flex-wrap items-center gap-2">
          {isDeleted ? (
            <button
              type="button"
              onClick={onRestore}
              className="border-2 border-foreground bg-background px-3 py-1.5 text-xs font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
            >
              鎭㈠鎵弿
            </button>
          ) : (
            <>
              <button
                type="button"
                onClick={onLaunchWorkflow}
                className="border-2 border-foreground bg-green-300 px-3 py-1.5 text-xs font-bold text-green-900 transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
              >
                鍚姩宸ヤ綔娴?              </button>
              <button
                type="button"
                onClick={onOpenLiveClassification}
                className="border-2 border-foreground bg-background px-3 py-1.5 text-xs font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
              >
                瀹炴椂鍒嗙被
              </button>
              <button
                type="button"
                onClick={onOpenLineage}
                className="inline-flex items-center gap-1 border-2 border-foreground bg-background px-2 py-1.5 text-xs font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
              >
                <Link2 className="h-3.5 w-3.5" />
                璺緞鍏崇郴
              </button>
              <select
                value={folder.category}
                onChange={(e) => onUpdateCategory(e.target.value as Category)}
                className="min-w-[8rem] border-2 border-foreground bg-background px-2 py-1.5 text-xs font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-1"
                aria-label="鏇存敼鍒嗙被"
              >
                {(['photo', 'video', 'mixed', 'manga', 'other'] as Category[]).map((c) => (
                  <option key={c} value={c}>{CATEGORY_LABEL[c]}</option>
                ))}
              </select>
              <select
                value={folder.status}
                onChange={(e) => onUpdateStatus(e.target.value as FolderStatus)}
                className="min-w-[7rem] border-2 border-foreground bg-background px-2 py-1.5 text-xs font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-1"
                aria-label="更改状态"
              >
                {(['pending', 'done', 'skip'] as FolderStatus[]).map((s) => (
                  <option key={s} value={s}>{STATUS_LABEL[s]}</option>
                ))}
              </select>
              <button
                type="button"
                onClick={onSnapshot}
                title="查看快照时间线"
                className="inline-flex h-8 w-8 items-center justify-center border-2 border-foreground bg-background p-1.5 transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
              >
                <History className="h-4 w-4" />
              </button>
              <button
                type="button"
                onClick={onRemove}
                title="浠庤蒋浠朵腑闅愯棌锛屼笉鏀瑰姩瀹為檯鏂囦欢"
                className="inline-flex h-8 w-8 items-center justify-center border-2 border-red-900 bg-red-100 p-1.5 text-red-900 transition-all hover:bg-red-900 hover:text-red-100 hover:shadow-hard hover:-translate-y-0.5"
              >
                <X className="h-4 w-4" />
              </button>
            </>
          )}
        </div>
      </td>
    </tr>
  )
}

export default function FolderListPage() {
  const navigate = useNavigate()
  const isMobile = useIsMobile(1024)
  const folders = useFolderStore((s) => s.folders)
  const total = useFolderStore((s) => s.total)
  const page = useFolderStore((s) => s.page)
  const limit = useFolderStore((s) => s.limit)
  const isLoading = useFolderStore((s) => s.isLoading)
  const error = useFolderStore((s) => s.error)
  const filters = useFolderStore((s) => s.filters)
  const isScanning = useFolderStore((s) => s.isScanning)
  const viewMode = useFolderStore((s) => s.viewMode)
  const fetchFolders = useFolderStore((s) => s.fetchFolders)
  const setFilters = useFolderStore((s) => s.setFilters)
  const setPage = useFolderStore((s) => s.setPage)
  const triggerScan = useFolderStore((s) => s.triggerScan)
  const setViewMode = useFolderStore((s) => s.setViewMode)
  const updateFolderCategory = useFolderStore((s) => s.updateFolderCategory)
  const updateFolderStatus = useFolderStore((s) => s.updateFolderStatus)
  const suppressFolder = useFolderStore((s) => s.suppressFolder)
  const unsuppressFolder = useFolderStore((s) => s.unsuppressFolder)
  const startJobPolling = useJobStore((s) => s.startPolling)
  const workflowDefs = useWorkflowDefStore((s) => s.defs)
  const workflowDefsLoading = useWorkflowDefStore((s) => s.isLoading)
  const workflowDefsError = useWorkflowDefStore((s) => s.error)
  const fetchWorkflowDefs = useWorkflowDefStore((s) => s.fetchDefs)
  const bindLatestLaunch = useWorkflowRunStore((s) => s.bindLatestLaunch)
  const bindLatestLaunchForFolders = useWorkflowRunStore((s) => s.bindLatestLaunchForFolders)
  const restoreLatestLaunch = useWorkflowRunStore((s) => s.restoreLatestLaunch)
  const buildRunCardView = useWorkflowRunStore((s) => s.buildRunCardView)
  const approveAllPendingReviews = useWorkflowRunStore((s) => s.approveAllPendingReviews)
  const rollbackAllPendingReviews = useWorkflowRunStore((s) => s.rollbackAllPendingReviews)
  const jobs = useJobStore((s) => s.jobs)

  const [selectedIds, setSelectedIds] = useState<Set<string>>(new Set())
  const [activeFolderId, setActiveFolderId] = useState<string | null>(null)
  const [launchDialog, setLaunchDialog] = useState<FolderWorkflowLaunchDialogState>({
    open: false,
    folderIds: [],
    mode: 'all',
  })
  const [selectedWorkflowDefId, setSelectedWorkflowDefId] = useState('')
  const [launchError, setLaunchError] = useState<string | null>(null)
  const [launchSuccessJobId, setLaunchSuccessJobId] = useState<string | null>(null)
  const [isLaunching, setIsLaunching] = useState(false)
  const [isHandlingReviewShortcut, setIsHandlingReviewShortcut] = useState(false)
  const previousListKeyRef = useRef<string>('')
  const effectiveViewMode = isMobile ? 'grid' : viewMode

  useEffect(() => {
    void fetchFolders()
  }, [fetchFolders, filters, page])

  // GSAP Stagger Animation for items
  useEffect(() => {
    const listKey = folders.map((folder) => folder.id).join('|')
    const listShapeChanged = previousListKeyRef.current !== listKey
    previousListKeyRef.current = listKey
    if (!isLoading && folders.length > 0 && listShapeChanged) {
      const selector = effectiveViewMode === 'grid' ? '.folder-card' : '.folder-row'
      gsap.fromTo(
        selector,
        { opacity: 0, x: -20 },
        { opacity: 1, x: 0, duration: 0.4, stagger: 0.05, ease: 'power2.out', clearProps: 'all' }
      )
    }
  }, [folders, effectiveViewMode, isLoading])

  const totalPages = Math.max(1, Math.ceil(total / limit))
  const currentSortBy = filters.sortBy ?? 'updated_at'
  const currentSortOrder = filters.sortOrder ?? 'desc'

  function setSort(sortBy: SortableFolderColumn) {
    const nextSortOrder =
      currentSortBy === sortBy ? (currentSortOrder === 'desc' ? 'asc' : 'desc') : 'desc'
    setPage(1)
    setFilters({ ...filters, sortBy, sortOrder: nextSortOrder })
  }

  function toggleSelect(id: string) {
    setSelectedIds((prev) => {
      const next = new Set(prev)
      if (next.has(id)) next.delete(id)
      else next.add(id)
      return next
    })
  }

  function toggleSelectAll() {
    if (selectedIds.size === folders.length) {
      setSelectedIds(new Set())
    } else {
      setSelectedIds(new Set(folders.map((f) => f.id)))
    }
  }

  function openLaunchDialog(folder: Folder) {
    setLaunchDialog({ open: true, folderIds: [folder.id], mode: 'all' })
    setLaunchError(null)
    setLaunchSuccessJobId(null)
    setIsLaunching(false)
    setSelectedWorkflowDefId('')
    void fetchWorkflowDefs()
  }

  function openBatchLaunchDialog(mode: FolderWorkflowLaunchDialogState['mode']) {
    const selectedFolderIDs = [...selectedIds]
    if (selectedFolderIDs.length === 0) return
    setLaunchDialog({ open: true, folderIds: selectedFolderIDs, mode })
    setLaunchError(null)
    setLaunchSuccessJobId(null)
    setIsLaunching(false)
    setSelectedWorkflowDefId('')
    void fetchWorkflowDefs()
  }

  function closeLaunchDialog() {
    setLaunchDialog({ open: false, folderIds: [], mode: 'all' })
    setSelectedWorkflowDefId('')
    setLaunchError(null)
    setLaunchSuccessJobId(null)
    setIsLaunching(false)
  }

  const workflowLaunchEntries = workflowDefs
    .filter((def) => workflowMatchesLaunchMode(def.graph_json, launchDialog.mode))
    .map((def) => ({
    def,
    launchability: getWorkflowFolderLaunchability(def.graph_json),
    }))
  const launchableWorkflowCount = workflowLaunchEntries.filter((entry) => entry.launchability.canLaunch).length
  const selectedWorkflowEntry = workflowLaunchEntries.find((entry) => entry.def.id === selectedWorkflowDefId) ?? null
  const selectedWorkflowDef = selectedWorkflowEntry?.def ?? null
  const selectedWorkflowLaunchability = selectedWorkflowEntry?.launchability ?? null

  useEffect(() => {
    if (!launchDialog.open) return
    if (selectedWorkflowDefId.trim() !== '') return
    const firstLaunchable = workflowLaunchEntries.find((entry) => entry.launchability.canLaunch)
    if (firstLaunchable) {
      setSelectedWorkflowDefId(firstLaunchable.def.id)
    }
  }, [launchDialog.open, selectedWorkflowDefId, workflowLaunchEntries])

  useEffect(() => {
    if (!launchDialog.open || selectedWorkflowDefId.trim() === '') return
    if (launchDialog.folderIds.length !== 1) return
    void restoreLatestLaunch(selectedWorkflowDefId, launchDialog.folderIds[0])
  }, [launchDialog.folderIds, launchDialog.open, restoreLatestLaunch, selectedWorkflowDefId])

  async function handleLaunchWorkflow() {
    if (launchDialog.folderIds.length === 0) return
    if (!selectedWorkflowDef) {
      setLaunchError('请选择一个工作流')
      return
    }

    setIsLaunching(true)
    setLaunchError(null)
    setLaunchSuccessJobId(null)
    try {
      const result = await launchWorkflowForFolder({
        workflowDef: selectedWorkflowDef,
        folderIds: launchDialog.folderIds,
        startWorkflow: async (workflowDefId, folderIds) => {
          const res = await startWorkflowJob({ workflow_def_id: workflowDefId, folder_ids: folderIds })
          return res.job_id
        },
        bindLatestLaunch,
      })
      if (result.folderIds.length > 1) {
        void bindLatestLaunchForFolders(selectedWorkflowDef.id, result.jobId, result.folderIds)
      }
      startJobPolling(result.jobId, {
        jobType: 'workflow',
        folderIds: result.folderIds,
      })
      setLaunchSuccessJobId(result.jobId)
    } catch (err) {
      setLaunchError(err instanceof Error ? err.message : '启动失败')
    } finally {
      setIsLaunching(false)
    }
  }

  const launchCardView = selectedWorkflowDef && launchDialog.folderIds.length === 1
    ? buildRunCardView(
      selectedWorkflowDef.id,
      countEnabledNodes(selectedWorkflowDef.graph_json),
      launchDialog.folderIds[0],
    )
    : null

  const launchBatchJob = launchSuccessJobId
    ? jobs.find((job) => job.id === launchSuccessJobId) ?? null
    : null

  async function handleApproveAllFromCard() {
    if (!launchCardView?.workflowRunId || launchCardView.pendingReviewCount <= 0) return
    setIsHandlingReviewShortcut(true)
    try {
      await approveAllPendingReviews(launchCardView.workflowRunId)
    } finally {
      setIsHandlingReviewShortcut(false)
    }
  }

  async function handleRollbackAllFromCard() {
    if (!launchCardView?.workflowRunId || launchCardView.pendingReviewCount <= 0) return
    setIsHandlingReviewShortcut(true)
    try {
      await rollbackAllPendingReviews(launchCardView.workflowRunId)
    } finally {
      setIsHandlingReviewShortcut(false)
    }
  }

  const launchSelectedFolders = folders.filter((folder) => launchDialog.folderIds.includes(folder.id))
  const launchModeLabel = launchDialog.mode === 'classification'
    ? '批量分类'
    : launchDialog.mode === 'processing'
      ? '批量处理'
      : '启动工作流'

  return (
    <>
      <div className="mb-6 flex flex-col gap-4 border-b-2 border-foreground pb-4 lg:flex-row lg:items-end lg:justify-between">
        <div>
          <h1 className="text-3xl font-black tracking-tight uppercase">媒体文件夹</h1>
          <p className="mt-1 text-sm font-bold text-muted-foreground">
            共 <span className="text-foreground">{total}</span> 个文件夹
            {selectedIds.size > 0 && <span className="ml-2 text-primary">· 已选 {selectedIds.size} 个</span>}
          </p>
        </div>
        <div className="flex w-full flex-wrap items-center gap-2 sm:gap-3 lg:w-auto lg:justify-end">
          <button
            type="button"
            onClick={() => void triggerScan()}
            disabled={isScanning}
            className="inline-flex min-w-0 items-center gap-2 border-2 border-foreground bg-background px-3 py-2 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-background disabled:hover:text-foreground disabled:hover:shadow-none disabled:hover:translate-y-0 sm:px-4"
          >
            {isScanning ? (
              <Loader2 className="h-4 w-4 animate-spin" />
            ) : (
              <Search className="h-4 w-4" />
            )}
            {isScanning ? '扫描中' : '扫描'}
          </button>
          {selectedIds.size > 0 && (
            <>
              <button
                type="button"
                onClick={() => openBatchLaunchDialog('classification')}
                className="inline-flex min-w-0 items-center gap-2 border-2 border-foreground bg-blue-200 px-3 py-2 text-sm font-bold text-blue-900 transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 sm:px-4"
              >
                批量分类
              </button>
              <button
                type="button"
                onClick={() => openBatchLaunchDialog('processing')}
                className="inline-flex min-w-0 items-center gap-2 border-2 border-foreground bg-green-200 px-3 py-2 text-sm font-bold text-green-900 transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 sm:px-4"
              >
                批量处理
              </button>
              <button
                type="button"
                onClick={() => setSelectedIds(new Set())}
                className="inline-flex min-w-0 items-center gap-2 border-2 border-foreground bg-background px-3 py-2 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 sm:px-4"
              >
                清空选择
              </button>
            </>
          )}
          <div className="hidden border-2 border-foreground bg-background shadow-hard lg:flex">
            <button
              type="button"
              onClick={() => setViewMode('grid')}
              className={cn(
                'px-3 py-2 text-sm transition-colors',
                viewMode === 'grid' ? 'bg-foreground text-background' : 'hover:bg-muted',
              )}
              title="缃戞牸瑙嗗浘"
            >
              <Grid2X2 className="h-4 w-4" />
            </button>
            <div className="w-0.5 bg-foreground" />
            <button
              type="button"
              onClick={() => setViewMode('list')}
              className={cn(
                'px-3 py-2 text-sm transition-colors',
                viewMode === 'list' ? 'bg-foreground text-background' : 'hover:bg-muted',
              )}
              title="鍒楄〃瑙嗗浘"
            >
              <List className="h-4 w-4" />
            </button>
          </div>
        </div>
      </div>

      <ScanProgressBanner />

      <div className="mb-6 flex flex-wrap gap-2 sm:gap-3">
        {ALL_CATEGORIES.map((c) => (
          <button
            key={c}
            type="button"
            onClick={() => { setPage(1); setFilters({ ...filters, category: c === '' ? undefined : c }) }}
            className={cn(
              'border-2 px-4 py-1.5 text-xs font-bold transition-all hover:-translate-y-0.5 hover:shadow-hard',
              filters.category === (c === '' ? undefined : c)
                ? 'border-foreground bg-foreground text-background shadow-hard -translate-y-0.5'
                : 'border-foreground bg-background text-foreground',
            )}
          >
            {CATEGORY_LABEL[c]}
          </button>
        ))}
        <div className="hidden h-6 w-0.5 self-center bg-foreground md:block" />
        {ALL_STATUSES.map((s) => (
          <button
            key={s}
            type="button"
            onClick={() => { setPage(1); setFilters({ ...filters, status: s === '' ? undefined : s, onlyDeleted: undefined }) }}
            className={cn(
              'border-2 px-4 py-1.5 text-xs font-bold transition-all hover:-translate-y-0.5 hover:shadow-hard',
              !filters.onlyDeleted && filters.status === (s === '' ? undefined : s)
                ? 'border-foreground bg-foreground text-background shadow-hard -translate-y-0.5'
                : 'border-foreground bg-background text-foreground',
            )}
          >
            {STATUS_LABEL[s]}
          </button>
        ))}
        <div className="hidden h-6 w-0.5 self-center bg-foreground md:block" />
        <button
          type="button"
          onClick={() => { setPage(1); setFilters({ onlyDeleted: filters.onlyDeleted ? undefined : true }) }}
          className={cn(
            'border-2 px-4 py-1.5 text-xs font-bold transition-all hover:-translate-y-0.5 hover:shadow-hard',
            filters.onlyDeleted
              ? 'border-red-900 bg-red-900 text-white shadow-hard -translate-y-0.5'
              : 'border-foreground bg-background text-foreground',
          )}
        >
          宸查殣钘?
        </button>
        <div className="hidden h-6 w-0.5 self-center bg-foreground md:block" />
        <button
          type="button"
          onClick={() => { setPage(1); setFilters({ ...filters, topLevelOnly: filters.topLevelOnly === false ? true : false }) }}
          className={cn(
            'border-2 px-4 py-1.5 text-xs font-bold transition-all hover:-translate-y-0.5 hover:shadow-hard',
            (filters.topLevelOnly ?? true)
              ? 'border-foreground bg-foreground text-background shadow-hard -translate-y-0.5'
              : 'border-foreground bg-background text-foreground',
          )}
        >
          {(filters.topLevelOnly ?? true) ? '仅一级目录' : '显示全部层级'}
        </button>
        <div className="hidden h-6 w-0.5 self-center bg-foreground md:block" />
        <button
          type="button"
          onClick={() => setSort('updated_at')}
          className={cn(
            'border-2 px-4 py-1.5 text-xs font-bold transition-all hover:-translate-y-0.5 hover:shadow-hard',
            currentSortBy === 'updated_at'
              ? 'border-foreground bg-foreground text-background shadow-hard -translate-y-0.5'
              : 'border-foreground bg-background text-foreground',
          )}
        >
          淇敼鏃堕棿 {getSortLabel(currentSortBy === 'updated_at', currentSortOrder === 'desc')}
        </button>
        <button
          type="button"
          onClick={() => setSort('total_size')}
          className={cn(
            'border-2 px-4 py-1.5 text-xs font-bold transition-all hover:-translate-y-0.5 hover:shadow-hard',
            currentSortBy === 'total_size'
              ? 'border-foreground bg-foreground text-background shadow-hard -translate-y-0.5'
              : 'border-foreground bg-background text-foreground',
          )}
        >
          澶у皬 {getSortLabel(currentSortBy === 'total_size', currentSortOrder === 'desc')}
        </button>
      </div>

      <div className="flex flex-col gap-6 xl:flex-row">
        <div className="min-w-0 flex-1">
          {error != null && (
            <div className="mb-6 border-2 border-foreground bg-red-100 px-4 py-3 text-sm font-bold text-red-900 shadow-hard">
              {error}
            </div>
          )}
          {isLoading && folders.length === 0 ? (
            <div className="flex flex-col items-center justify-center py-32 text-foreground">
              <Loader2 className="h-8 w-8 animate-spin" />
              <span className="mt-4 text-sm font-bold tracking-widest">LOADING DATA...</span>
            </div>
          ) : folders.length === 0 ? (
            <div className="flex flex-col items-center justify-center border-2 border-dashed border-foreground py-32 text-muted-foreground">
              <FolderOpen className="h-12 w-12 opacity-50" />
              <p className="mt-4 text-sm font-bold">鏆傛棤鏂囦欢澶癸紝璇峰厛鎵弿</p>
            </div>
          ) : effectiveViewMode === 'grid' ? (
            <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
              {folders.map((folder) => (
                <FolderCard
                  key={folder.id}
                  folder={folder}
                  selected={selectedIds.has(folder.id)}
                  onToggleSelect={() => toggleSelect(folder.id)}
                  onLaunchWorkflow={() => openLaunchDialog(folder)}
                  onOpenLiveClassification={() => navigate(`/folders/${folder.id}/live-classification`)}
                  onOpenLineage={() => navigate(`/folders/${folder.id}/lineage`)}
                  onSnapshot={() => setActiveFolderId(folder.id)}
                  onUpdateCategory={(c) => void updateFolderCategory(folder.id, c)}
                  onUpdateStatus={(s) => void updateFolderStatus(folder.id, s)}
                  onRemove={() => void suppressFolder(folder.id)}
                  onRestore={() => void unsuppressFolder(folder.id)}
                />
              ))}
            </div>
          ) : (
            <div className="overflow-hidden border-2 border-foreground bg-card shadow-hard">
              <table className="table-fixed w-full min-w-0 text-sm">
                <thead>
                  <tr className="border-b-2 border-foreground bg-muted/50">
                    <th className="w-12 px-4 py-4">
                      <input
                        type="checkbox"
                        checked={selectedIds.size === folders.length && folders.length > 0}
                        onChange={toggleSelectAll}
                        className="h-4 w-4 rounded-none border-2 border-foreground text-foreground focus:ring-foreground focus:ring-offset-0"
                        aria-label="全选"
                      />
                    </th>
                    <th className="px-4 py-4 text-left font-black tracking-widest">鍚嶇О</th>
                    <th className="px-4 py-4 text-left font-black tracking-widest">分类 / 状态</th>
                    <th className="hidden px-4 py-4 text-left font-black tracking-widest sm:table-cell">文件数</th>
                    <th className="hidden px-4 py-4 text-left font-black tracking-widest md:table-cell">
                      <button
                        type="button"
                        onClick={() => setSort('total_size')}
                        className="inline-flex items-center gap-1 transition-colors hover:text-foreground/70"
                      >
                        <span>澶у皬</span>
                        <span>{getSortLabel(currentSortBy === 'total_size', currentSortOrder === 'desc')}</span>
                      </button>
                    </th>
                    <th className="hidden px-4 py-4 text-left font-black tracking-widest lg:table-cell">
                      <button
                        type="button"
                        onClick={() => setSort('updated_at')}
                        className="inline-flex items-center gap-1 transition-colors hover:text-foreground/70"
                      >
                        <span>淇敼鏃堕棿</span>
                        <span>{getSortLabel(currentSortBy === 'updated_at', currentSortOrder === 'desc')}</span>
                      </button>
                    </th>
                    <th className="px-4 py-4 text-left font-black tracking-widest">鎿嶄綔</th>
                  </tr>
                </thead>
                <tbody>
                  {folders.map((folder) => (
                    <FolderRow
                      key={folder.id}
                      folder={folder}
                      selected={selectedIds.has(folder.id)}
                      onToggleSelect={() => toggleSelect(folder.id)}
                      onLaunchWorkflow={() => openLaunchDialog(folder)}
                      onOpenLiveClassification={() => navigate(`/folders/${folder.id}/live-classification`)}
                      onOpenLineage={() => navigate(`/folders/${folder.id}/lineage`)}
                      onSnapshot={() => setActiveFolderId(folder.id)}
                      onUpdateCategory={(c) => void updateFolderCategory(folder.id, c)}
                      onUpdateStatus={(s) => void updateFolderStatus(folder.id, s)}
                      onRemove={() => void suppressFolder(folder.id)}
                      onRestore={() => void unsuppressFolder(folder.id)}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {totalPages > 1 && (
            <div className="mt-8 flex flex-wrap items-center justify-center gap-3 sm:gap-4">
              <button
                type="button"
                disabled={page <= 1}
                onClick={() => setPage(page - 1)}
                className="border-2 border-foreground bg-background px-4 py-2 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-40 disabled:hover:bg-background disabled:hover:text-foreground disabled:hover:shadow-none disabled:hover:translate-y-0"
              >
                涓婁竴椤?
              </button>
              <span className="min-w-[4rem] text-center text-sm font-black font-mono">
                {page} / {totalPages}
              </span>
              <button
                type="button"
                disabled={page >= totalPages}
                onClick={() => setPage(page + 1)}
                className="border-2 border-foreground bg-background px-4 py-2 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-40 disabled:hover:bg-background disabled:hover:text-foreground disabled:hover:shadow-none disabled:hover:translate-y-0"
              >
                涓嬩竴椤?
              </button>
            </div>
          )}
        </div>
      {launchDialog.open && launchDialog.folderIds.length > 0 && typeof document !== 'undefined' && createPortal(
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
          <div className="max-h-[calc(100dvh-2rem)] w-full max-w-3xl overflow-y-auto border-2 border-foreground bg-background p-4 shadow-hard-lg sm:p-6">
            <h2 className="mb-2 text-xl font-black tracking-tight">{launchModeLabel}</h2>
            <p className="mb-1 text-sm font-bold text-muted-foreground">
              已选择 {launchDialog.folderIds.length} 个文件夹
            </p>
            <p className="mb-4 truncate font-mono text-xs font-bold text-muted-foreground" title={launchSelectedFolders.map((folder) => folder.name).join('，')}>
              {launchSelectedFolders.slice(0, 3).map((folder) => folder.name).join('，')}
              {launchSelectedFolders.length > 3 ? ` +${launchSelectedFolders.length - 3}` : ''}
            </p>

            <div className="space-y-3">
              <p className="text-xs font-black tracking-wider">选择工作流定义</p>
              <div className="max-h-72 space-y-2 overflow-auto border-2 border-foreground bg-muted/20 p-2">
                {workflowDefsLoading ? (
                  <p className="py-8 text-center text-xs font-bold text-muted-foreground">工作流加载中...</p>
                ) : workflowDefsError ? (
                  <p className="py-8 text-center text-xs font-bold text-red-700">{workflowDefsError}</p>
                ) : workflowLaunchEntries.length === 0 ? (
                  <p className="py-8 text-center text-xs font-bold text-muted-foreground">当前入口没有可用工作流</p>
                ) : (
                  workflowLaunchEntries.map(({ def, launchability }) => {
                    const disabled = !launchability.canLaunch
                    const selected = selectedWorkflowDefId === def.id
                    return (
                      <button
                        key={def.id}
                        type="button"
                        disabled={disabled}
                        onClick={() => {
                          setSelectedWorkflowDefId(def.id)
                          setLaunchError(null)
                          setLaunchSuccessJobId(null)
                        }}
                        className={cn(
                          'w-full border-2 px-3 py-3 text-left transition-all',
                          selected
                            ? 'border-foreground bg-foreground text-background shadow-hard'
                            : 'border-foreground bg-background hover:bg-muted',
                          disabled && 'cursor-not-allowed opacity-60 hover:bg-background',
                        )}
                      >
                        <div className="flex items-center justify-between gap-3">
                          <span className="truncate text-sm font-black">{def.name}</span>
                          <span className="shrink-0 font-mono text-xs font-bold">v{def.version}</span>
                        </div>
                        <p className={cn('mt-2 text-xs font-bold', selected ? 'text-background/90' : 'text-muted-foreground')}>
                          {launchability.canLaunch
                            ? `可快捷启动（${launchability.enabledPickerCount} 个 folder-picker）`
                            : (launchability.error ?? '该工作流暂不可快捷启动')}
                        </p>
                      </button>
                    )
                  })
                )}
              </div>
            </div>

            {launchError && (
              <p className="mt-4 border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900 shadow-hard">
                {launchError}
              </p>
            )}

            {launchSuccessJobId && (
              <div className="mt-4 border-2 border-green-900 bg-green-100 px-4 py-3 text-sm font-bold text-green-900 shadow-hard">
                启动成功，任务 ID：{launchSuccessJobId}
              </div>
            )}

            {launchBatchJob && launchDialog.folderIds.length > 1 && (
              <div className="mt-4 border-2 border-foreground bg-muted/10 px-4 py-3 text-sm font-bold">
                <p>批量进度：{launchBatchJob.done} / {launchBatchJob.total}，失败 {launchBatchJob.failed}</p>
                {launchBatchJob.status === 'waiting_input' && (
                  <p className="mt-1 text-xs text-amber-700">存在待确认项，请前往任务历史批量确认。</p>
                )}
              </div>
            )}

            {launchCardView && (
              <div className="mt-4">
                <WorkflowRunStatusCard
                  view={launchCardView}
                  title="最近一次运行状态"
                  onOpenJobs={() => navigate(buildJobHistoryLink(launchCardView.jobId, launchCardView.workflowRunId))}
                  onApproveAllPending={() => void handleApproveAllFromCard()}
                  onRollbackAllPending={() => void handleRollbackAllFromCard()}
                  actionLoading={isHandlingReviewShortcut}
                />
              </div>
            )}

            <div className="mt-8 flex flex-wrap items-center justify-between gap-3">
              <button
                type="button"
                onClick={closeLaunchDialog}
                disabled={isLaunching}
                className="border-2 border-foreground bg-background px-6 py-2.5 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50"
              >
                关闭
              </button>

              <div className="flex w-full flex-wrap items-center justify-end gap-3 sm:w-auto">
                {launchSuccessJobId && (
                  <button
                    type="button"
                    onClick={() => navigate(buildJobHistoryLink(launchSuccessJobId, launchCardView?.workflowRunId))}
                    className="border-2 border-foreground bg-primary px-4 py-2.5 text-sm font-bold text-primary-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
                  >
                    查看任务
                  </button>
                )}
                <button
                  type="button"
                  onClick={() => void handleLaunchWorkflow()}
                  disabled={
                    isLaunching
                    || launchDialog.folderIds.length === 0
                    || !selectedWorkflowDef
                    || selectedWorkflowLaunchability?.canLaunch !== true
                  }
                  className="inline-flex items-center gap-2 border-2 border-foreground bg-green-300 px-6 py-2.5 text-sm font-bold text-green-900 transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  {isLaunching && <Loader2 className="h-4 w-4 animate-spin" />}
                  {isLaunching ? '启动中...' : '确认启动'}
                </button>
              </div>
            </div>
          </div>
        </div>,
        document.body
      )}

      <SnapshotDrawer
        open={activeFolderId !== null}
        folderId={activeFolderId}
        onClose={() => setActiveFolderId(null)}
      />
    </>
  )
}
