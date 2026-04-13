import { useEffect } from 'react'
import { Link } from 'react-router-dom'
import { ChevronRight, Clock3, FolderTree, Loader2 } from 'lucide-react'

import { cn } from '@/lib/utils'
import { useLiveClassificationStore } from '@/store/liveClassificationStore'
import type {
  Category,
  ClassificationFileKind,
  FolderClassificationTreeEntry,
  LiveClassificationItem,
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
  'ml-3',
  'ml-6',
  'ml-9',
  'ml-12',
  'ml-16',
  'ml-20',
  'ml-24',
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
            文件 {entry.total_files} · 图 {entry.image_count} · 视 {entry.video_count} · 其 {entry.other_file_count}
          </span>
        </div>
        <p className="mt-1 truncate font-mono text-[11px] text-muted-foreground" title={entry.path}>
          {entry.path}
        </p>

        {entry.files.length > 0 && (
          <div className="mt-3 space-y-1 border-t border-foreground/30 pt-2">
            {entry.files.map((file) => (
              <div key={`${entry.folder_id}:${file.name}`} className="flex items-center justify-between gap-2 text-xs">
                <div className="min-w-0">
                  <span className="truncate font-mono">{file.name}</span>
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

function FolderRow({
  item,
  active,
  onSelect,
}: {
  item: LiveClassificationItem
  active: boolean
  onSelect: (folderId: string) => void
}) {
  return (
    <button
      type="button"
      onClick={() => onSelect(item.folder_id)}
      className={cn(
        'w-full border-2 p-3 text-left transition-all hover:-translate-y-0.5 hover:shadow-hard',
        active ? 'border-foreground bg-primary/15' : 'border-foreground/50 bg-card',
      )}
    >
      <div className="flex items-center justify-between gap-2">
        <p className="truncate text-sm font-black" title={item.folder_name}>
          {item.folder_name}
        </p>
        <span className="text-[11px] font-bold text-muted-foreground">{STATUS_LABEL[item.classification_status]}</span>
      </div>
      <p className="mt-1 truncate font-mono text-[11px] text-muted-foreground" title={item.folder_path}>
        {item.folder_path}
      </p>
      <div className="mt-2 flex items-center justify-between gap-2 text-[11px] font-bold">
        <span>{CATEGORY_LABEL[item.category]}</span>
        <span>{formatTime(item.last_event_at)}</span>
      </div>
    </button>
  )
}

export default function LiveClassificationPage() {
  const orderedIds = useLiveClassificationStore((s) => s.orderedIds)
  const itemsById = useLiveClassificationStore((s) => s.itemsById)
  const currentScanJobId = useLiveClassificationStore((s) => s.currentScanJobId)
  const selectedFolderId = useLiveClassificationStore((s) => s.selectedFolderId)
  const treeByFolderId = useLiveClassificationStore((s) => s.treeByFolderId)
  const treeLoadingFolderId = useLiveClassificationStore((s) => s.treeLoadingFolderId)
  const treeError = useLiveClassificationStore((s) => s.treeError)
  const isLoading = useLiveClassificationStore((s) => s.isLoading)
  const error = useLiveClassificationStore((s) => s.error)
  const loadInitial = useLiveClassificationStore((s) => s.loadInitial)
  const selectFolder = useLiveClassificationStore((s) => s.selectFolder)

  useEffect(() => {
    if (orderedIds.length === 0) {
      void loadInitial()
    }
  }, [loadInitial, orderedIds.length])

  const selectedTree = selectedFolderId ? treeByFolderId[selectedFolderId] : undefined
  const selectedItem = selectedFolderId ? itemsById[selectedFolderId] : undefined

  return (
    <section className="space-y-4">
      <header className="flex flex-wrap items-end justify-between gap-3 border-b-2 border-foreground pb-3">
        <div>
          <h1 className="text-3xl font-black tracking-tight">实时分类</h1>
          <p className="mt-1 text-sm font-bold text-muted-foreground">
            查看每个目录下的子目录与文件分类结果（实时刷新）。
          </p>
        </div>
        {currentScanJobId && (
          <div className="inline-flex items-center gap-2 border-2 border-foreground bg-blue-100 px-3 py-2 text-xs font-black text-blue-900">
            <Loader2 className="h-4 w-4 animate-spin" />
            扫描任务进行中：{currentScanJobId}
          </div>
        )}
      </header>

      {error && (
        <div className="border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900">
          {error}
        </div>
      )}

      <div className="grid grid-cols-1 gap-4 xl:grid-cols-[380px_1fr]">
        <aside className="space-y-3 border-2 border-foreground bg-muted/20 p-3">
          <div className="flex items-center justify-between border-b-2 border-foreground pb-2">
            <h2 className="text-sm font-black">目录队列</h2>
            <span className="border-2 border-foreground bg-background px-1.5 py-0.5 text-[11px] font-black">
              {orderedIds.length}
            </span>
          </div>

          {isLoading && orderedIds.length === 0 ? (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="h-5 w-5 animate-spin" />
            </div>
          ) : orderedIds.length === 0 ? (
            <p className="py-8 text-center text-xs font-bold text-muted-foreground">暂无目录</p>
          ) : (
            <div className="max-h-[72vh] space-y-2 overflow-auto pr-1">
              {orderedIds.map((id) => {
                const item = itemsById[id]
                if (!item) return null
                return (
                  <FolderRow
                    key={id}
                    item={item}
                    active={id === selectedFolderId}
                    onSelect={(folderID) => {
                      void selectFolder(folderID)
                    }}
                  />
                )
              })}
            </div>
          )}
        </aside>

        <main className="space-y-3 border-2 border-foreground bg-card p-3">
          <div className="flex flex-wrap items-center justify-between gap-2 border-b-2 border-foreground pb-2">
            <div className="min-w-0">
              <h2 className="truncate text-sm font-black">
                {selectedItem ? `目录内容分类 · ${selectedItem.folder_name}` : '目录内容分类'}
              </h2>
              {selectedItem && (
                <p className="truncate font-mono text-[11px] text-muted-foreground" title={selectedItem.folder_path}>
                  {selectedItem.folder_path}
                </p>
              )}
            </div>
            {selectedItem?.job_id && (
              <Link
                to={`/job-history?job_id=${encodeURIComponent(selectedItem.job_id)}${selectedItem.workflow_run_id ? `&workflow_run_id=${encodeURIComponent(selectedItem.workflow_run_id)}` : ''}`}
                className="inline-flex items-center gap-1 border-2 border-foreground px-2 py-1 text-[11px] font-bold transition-all hover:-translate-y-0.5 hover:bg-foreground hover:text-background hover:shadow-hard"
              >
                作业历史
                <ChevronRight className="h-3 w-3" />
              </Link>
            )}
          </div>

          {!selectedFolderId ? (
            <div className="flex min-h-[320px] items-center justify-center text-sm font-bold text-muted-foreground">
              请先从左侧选择一个目录
            </div>
          ) : treeLoadingFolderId === selectedFolderId && !selectedTree ? (
            <div className="flex min-h-[320px] items-center justify-center gap-2 text-sm font-bold">
              <Loader2 className="h-5 w-5 animate-spin" />
              正在加载目录分类树
            </div>
          ) : treeError && !selectedTree ? (
            <div className="border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900">
              {treeError}
            </div>
          ) : selectedTree ? (
            <div className="max-h-[72vh] space-y-2 overflow-auto pr-1">
              <div className="mb-2 inline-flex items-center gap-1 border border-foreground/30 bg-background px-2 py-1 text-[11px] font-bold text-muted-foreground">
                <FolderTree className="h-3.5 w-3.5" />
                最近刷新：{selectedItem ? formatTime(selectedItem.last_event_at) : '-'}
                {treeLoadingFolderId === selectedFolderId && <Clock3 className="h-3.5 w-3.5 animate-pulse" />}
              </div>
              <TreeNode entry={selectedTree} depth={0} />
            </div>
          ) : (
            <div className="flex min-h-[320px] items-center justify-center text-sm font-bold text-muted-foreground">
              当前目录暂无分类树数据
            </div>
          )}
        </main>
      </div>
    </section>
  )
}
