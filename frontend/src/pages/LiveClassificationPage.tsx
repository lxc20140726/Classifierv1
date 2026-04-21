import { useEffect } from 'react'
import { Link, useNavigate, useParams } from 'react-router-dom'
import { ChevronRight, Clock3, FolderTree, Loader2 } from 'lucide-react'

import { cn } from '@/lib/utils'
import { useLiveClassificationStore } from '@/store/liveClassificationStore'
import type {
  Category,
  ClassificationFileKind,
  FolderClassificationTreeEntry,
  LiveClassificationStatus,
} from '@/types'

const CATEGORY_LABEL: Record<Category, string> = {
  photo: '写真',
  video: '视频',
  mixed: '混合',
  manga: '漫画',
  other: '其他',
}

const FILE_KIND_LABEL: Record<ClassificationFileKind, string> = {
  photo: '图片',
  video: '视频',
  manga: '漫画包',
  other: '其他',
}

const STATUS_LABEL: Record<LiveClassificationStatus, string> = {
  scanning: '扫描中',
  classifying: '分类中',
  waiting_input: '待确认',
  completed: '已完成',
  failed: '失败',
}

function formatTime(value: string): string {
  const timestamp = Date.parse(value)
  if (Number.isNaN(timestamp)) return '-'
  return new Date(timestamp).toLocaleString('zh-CN')
}

function FileKindBadge({ kind }: { kind: ClassificationFileKind }) {
  return (
    <span className="border border-foreground/40 bg-background px-1.5 py-0.5 text-[10px] font-bold">
      {FILE_KIND_LABEL[kind]}
    </span>
  )
}

const INDENT_CLASS = [
  'ml-0',
  'ml-2 sm:ml-3',
  'ml-3 sm:ml-6',
  'ml-4 sm:ml-9',
  'ml-5 sm:ml-12',
  'ml-6 sm:ml-16',
  'ml-7 sm:ml-20',
  'ml-8 sm:ml-24',
]

function indentClass(depth: number): string {
  if (depth <= 0) return INDENT_CLASS[0]
  if (depth >= INDENT_CLASS.length) return INDENT_CLASS[INDENT_CLASS.length - 1]
  return INDENT_CLASS[depth]
}

function TreeNode({ entry, depth }: { entry: FolderClassificationTreeEntry; depth: number }) {
  return (
    <div className="space-y-2">
      <div className={cn('border-2 border-foreground bg-card p-3', indentClass(depth))}>
        <div className="flex flex-wrap items-center gap-2">
          <span className="font-black">{entry.name}</span>
          <span className="border border-foreground/50 px-1.5 py-0.5 text-[10px] font-bold">
            {CATEGORY_LABEL[entry.category]}
          </span>
          <span className="text-[11px] font-bold text-muted-foreground">
            文件 {entry.total_files} 路 图 {entry.image_count} 路 视 {entry.video_count} 路 其 {entry.other_file_count}
          </span>
        </div>
        <p className="mt-1 break-all font-mono text-[11px] text-muted-foreground" title={entry.path}>
          {entry.path}
        </p>

        {entry.files.length > 0 && (
          <div className="mt-3 space-y-1 border-t border-foreground/30 pt-2">
            {entry.files.map((file) => (
              <div key={`${entry.folder_id}:${file.name}`} className="flex items-center justify-between gap-2 text-xs">
                <div className="min-w-0">
                  <span className="break-all font-mono">{file.name}</span>
                </div>
                <div className="shrink-0">
                  <FileKindBadge kind={file.kind} />
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {entry.subtree.length > 0 && (
        <div className="space-y-2">
          {entry.subtree.map((child) => (
            <TreeNode key={child.folder_id || child.path} entry={child} depth={depth + 1} />
          ))}
        </div>
      )}
    </div>
  )
}

export default function LiveClassificationPage() {
  const navigate = useNavigate()
  const params = useParams<{ id: string }>()
  const folderId = params.id?.trim() ?? ''

  const itemsById = useLiveClassificationStore((s) => s.itemsById)
  const currentScanJobId = useLiveClassificationStore((s) => s.currentScanJobId)
  const focusFolderId = useLiveClassificationStore((s) => s.focusFolderId)
  const treeByFolderId = useLiveClassificationStore((s) => s.treeByFolderId)
  const treeLoadingFolderId = useLiveClassificationStore((s) => s.treeLoadingFolderId)
  const treeError = useLiveClassificationStore((s) => s.treeError)
  const isLoading = useLiveClassificationStore((s) => s.isLoading)
  const error = useLiveClassificationStore((s) => s.error)
  const setFocusFolder = useLiveClassificationStore((s) => s.setFocusFolder)
  const clearFocusFolder = useLiveClassificationStore((s) => s.clearFocusFolder)
  const loadFolderLiveState = useLiveClassificationStore((s) => s.loadFolderLiveState)

  useEffect(() => {
    if (!folderId) return
    setFocusFolder(folderId)
    void loadFolderLiveState(folderId)
    return () => {
      clearFocusFolder()
    }
  }, [clearFocusFolder, folderId, loadFolderLiveState, setFocusFolder])

  const liveItem = itemsById[folderId]
  const tree = treeByFolderId[folderId]
  const isTreeLoading = treeLoadingFolderId === folderId
  const isFocused = focusFolderId === '' || focusFolderId === folderId

  return (
    <section className="space-y-4">
      <header className="flex flex-wrap items-end justify-between gap-3 border-b-2 border-foreground pb-3">
        <div>
          <h1 className="text-3xl font-black tracking-tight">实时分类</h1>
          <p className="mt-1 text-sm font-bold text-muted-foreground">仅展示当前文件夹的实时分类状态与分类树。</p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <button
            type="button"
            onClick={() => navigate('/')}
            className="border-2 border-foreground bg-background px-3 py-2 text-xs font-bold transition-all hover:-translate-y-0.5 hover:bg-foreground hover:text-background hover:shadow-hard"
          >
            返回列表
          </button>
          {currentScanJobId && (
            <div className="inline-flex items-center gap-2 border-2 border-foreground bg-blue-100 px-3 py-2 text-xs font-black text-blue-900">
              <Loader2 className="h-4 w-4 animate-spin" />
              扫描任务进行中：{currentScanJobId}
            </div>
          )}
        </div>
      </header>

      {!folderId && (
        <div className="border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900">
          目录 ID 无效，请从列表页重新进入。
        </div>
      )}

      {folderId && !isFocused && (
        <div className="border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900">
          当前页面焦点目录异常，请返回列表后重试。
        </div>
      )}

      {folderId && error && (
        <div className="border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900">
          {error}
        </div>
      )}

      <main className="space-y-3 border-2 border-foreground bg-card p-3">
        <div className="flex flex-wrap items-center justify-between gap-2 border-b-2 border-foreground pb-2">
          <div className="min-w-0">
            <h2 className="truncate text-sm font-black">
              {liveItem ? `目录内容分类 路 ${liveItem.folder_name}` : '目录内容分类'}
            </h2>
            {liveItem && (
              <p className="break-all font-mono text-[11px] text-muted-foreground" title={liveItem.folder_path}>
                {liveItem.folder_path}
              </p>
            )}
          </div>
          <div className="flex items-center gap-2">
            {liveItem && (
              <span className="border border-foreground/50 px-1.5 py-0.5 text-[11px] font-bold">
                {STATUS_LABEL[liveItem.classification_status]}
              </span>
            )}
            {liveItem?.job_id && (
              <Link
                to={`/job-history?job_id=${encodeURIComponent(liveItem.job_id)}${liveItem.workflow_run_id ? `&workflow_run_id=${encodeURIComponent(liveItem.workflow_run_id)}` : ''}`}
                className="inline-flex items-center gap-1 border-2 border-foreground px-2 py-1 text-[11px] font-bold transition-all hover:-translate-y-0.5 hover:bg-foreground hover:text-background hover:shadow-hard"
              >
                作业历史
                <ChevronRight className="h-3 w-3" />
              </Link>
            )}
          </div>
        </div>

        {isLoading && !liveItem ? (
          <div className="flex min-h-[320px] items-center justify-center gap-2 text-sm font-bold">
            <Loader2 className="h-5 w-5 animate-spin" />
            正在加载目录实时信息...
          </div>
        ) : !liveItem ? (
          <div className="flex min-h-[320px] flex-col items-center justify-center gap-3 text-sm font-bold text-muted-foreground">
            <p>未找到该目录或目录已不存在。</p>
            <button
              type="button"
              onClick={() => navigate('/')}
              className="border-2 border-foreground bg-background px-3 py-2 text-xs font-bold transition-all hover:-translate-y-0.5 hover:bg-foreground hover:text-background hover:shadow-hard"
            >
              返回文件夹列表
            </button>
          </div>
        ) : isTreeLoading && !tree ? (
          <div className="flex min-h-[320px] items-center justify-center gap-2 text-sm font-bold">
            <Loader2 className="h-5 w-5 animate-spin" />
            正在加载目录分类树...
          </div>
        ) : treeError && !tree ? (
          <div className="flex min-h-[320px] flex-col items-center justify-center gap-3">
            <div className="w-full border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900">
              {treeError}
            </div>
            <button
              type="button"
              onClick={() => navigate('/')}
              className="border-2 border-foreground bg-background px-3 py-2 text-xs font-bold transition-all hover:-translate-y-0.5 hover:bg-foreground hover:text-background hover:shadow-hard"
            >
              返回文件夹列表
            </button>
          </div>
        ) : tree ? (
          <div className="max-h-[72vh] space-y-2 overflow-auto pr-1">
            <div className="mb-2 inline-flex items-center gap-1 border border-foreground/30 bg-background px-2 py-1 text-[11px] font-bold text-muted-foreground">
              <FolderTree className="h-3.5 w-3.5" />
              最近刷新：{formatTime(liveItem.last_event_at)}
              {isTreeLoading && <Clock3 className="h-3.5 w-3.5 animate-pulse" />}
            </div>
            <TreeNode entry={tree} depth={0} />
          </div>
        ) : (
          <div className="flex min-h-[320px] items-center justify-center text-sm font-bold text-muted-foreground">
            当前目录暂无分类树数据。
          </div>
        )}
      </main>
    </section>
  )
}
