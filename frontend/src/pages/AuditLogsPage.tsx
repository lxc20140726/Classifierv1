import { useEffect, useMemo, useState } from 'react'
import { RefreshCw, Search } from 'lucide-react'
import { useSearchParams } from 'react-router-dom'

import { listAuditLogs } from '@/api/auditLogs'
import { useIsMobile } from '@/hooks/useIsMobile'
import { PathChangePreview } from '@/components/PathChangePreview'
import { cn } from '@/lib/utils'
import type { AuditLog } from '@/types'

interface AuditFilterState {
  jobId: string
  workflowRunId: string
  nodeRunId: string
  nodeId: string
  nodeType: string
  action: string
  result: string
  folderPath: string
  from: string
  to: string
}

const PAGE_SIZE = 50

function readFiltersFromSearchParams(searchParams: URLSearchParams): AuditFilterState {
  return {
    jobId: searchParams.get('job_id') ?? '',
    workflowRunId: searchParams.get('workflow_run_id') ?? '',
    nodeRunId: searchParams.get('node_run_id') ?? '',
    nodeId: searchParams.get('node_id') ?? '',
    nodeType: searchParams.get('node_type') ?? '',
    action: searchParams.get('action') ?? '',
    result: searchParams.get('result') ?? '',
    folderPath: searchParams.get('folder_path') ?? '',
    from: searchParams.get('from') ?? '',
    to: searchParams.get('to') ?? '',
  }
}

function formatErrorSummary(log: AuditLog) {
  if (log.error_msg.trim() !== '') return log.error_msg
  const detailError = typeof log.detail?.error === 'string' ? log.detail.error : ''
  if (detailError.trim() !== '') return detailError
  return '—'
}

function asRecord(value: unknown): Record<string, unknown> | null {
  if (value == null || typeof value !== 'object' || Array.isArray(value)) {
    return null
  }
  return value as Record<string, unknown>
}

function asString(value: unknown) {
  return typeof value === 'string' ? value.trim() : ''
}

function joinPath(baseDir: string, name: string) {
  const trimmedBase = baseDir.trim().replaceAll('\\', '/').replace(/\/+$/, '')
  const trimmedName = name.trim().replaceAll('\\', '/').replace(/^\/+/, '')
  if (!trimmedBase) return trimmedName
  if (!trimmedName) return trimmedBase
  return `${trimmedBase}/${trimmedName}`
}

function resolveAuditPathChange(log: AuditLog) {
  const detail = asRecord(log.detail)
  const before = asRecord(detail?.before)
  const after = asRecord(detail?.after)

  const sourcePath = asString(detail?.source_path) || asString(before?.path)
  let targetPath = asString(detail?.target_path) || asString(after?.path)
  const targetDir = asString(detail?.target_dir)

  if (targetPath === '' && targetDir !== '' && sourcePath !== '') {
    const segments = sourcePath.replaceAll('\\', '/').split('/').filter((segment) => segment !== '')
    const baseName = segments.length === 0 ? '' : segments[segments.length - 1]
    if (baseName !== '') {
      targetPath = joinPath(targetDir, baseName)
    }
  }

  return {
    fromPath: sourcePath,
    toPath: targetPath,
    currentPath: log.folder_path,
  }
}

export default function AuditLogsPage() {
  const isMobile = useIsMobile(1024)
  const [searchParams, setSearchParams] = useSearchParams()
  const [filters, setFilters] = useState<AuditFilterState>(() => readFiltersFromSearchParams(searchParams))
  const [appliedFilters, setAppliedFilters] = useState<AuditFilterState>(() => readFiltersFromSearchParams(searchParams))
  const [logs, setLogs] = useState<AuditLog[]>([])
  const [page, setPage] = useState(() => {
    const rawPage = Number(searchParams.get('page') ?? '1')
    return Number.isFinite(rawPage) && rawPage > 0 ? rawPage : 1
  })
  const [total, setTotal] = useState(0)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    void fetchLogs(page, appliedFilters)
  }, [appliedFilters, page])

  function syncUrl(nextPage: number, nextFilters: AuditFilterState) {
    const next = new URLSearchParams()
    if (nextPage > 1) next.set('page', String(nextPage))
    if (nextFilters.jobId) next.set('job_id', nextFilters.jobId)
    if (nextFilters.workflowRunId) next.set('workflow_run_id', nextFilters.workflowRunId)
    if (nextFilters.nodeRunId) next.set('node_run_id', nextFilters.nodeRunId)
    if (nextFilters.nodeId) next.set('node_id', nextFilters.nodeId)
    if (nextFilters.nodeType) next.set('node_type', nextFilters.nodeType)
    if (nextFilters.action) next.set('action', nextFilters.action)
    if (nextFilters.result) next.set('result', nextFilters.result)
    if (nextFilters.folderPath) next.set('folder_path', nextFilters.folderPath)
    if (nextFilters.from) next.set('from', nextFilters.from)
    if (nextFilters.to) next.set('to', nextFilters.to)
    setSearchParams(next, { replace: true })
  }

  async function fetchLogs(nextPage: number, nextFilters: AuditFilterState) {
    setIsLoading(true)
    setError(null)
    try {
      const response = await listAuditLogs({
        jobId: nextFilters.jobId || undefined,
        workflowRunId: nextFilters.workflowRunId || undefined,
        nodeRunId: nextFilters.nodeRunId || undefined,
        nodeId: nextFilters.nodeId || undefined,
        nodeType: nextFilters.nodeType || undefined,
        action: nextFilters.action || undefined,
        result: nextFilters.result || undefined,
        folderPath: nextFilters.folderPath || undefined,
        from: nextFilters.from || undefined,
        to: nextFilters.to || undefined,
        page: nextPage,
        limit: PAGE_SIZE,
      })
      setLogs(response.data)
      setTotal(response.total)
    } catch (loadError) {
      setError(loadError instanceof Error ? loadError.message : '加载审计日志失败')
    } finally {
      setIsLoading(false)
    }
  }

  function handleSearch() {
    const nextPage = 1
    setAppliedFilters(filters)
    setPage(nextPage)
    syncUrl(nextPage, filters)
  }

  function handlePageChange(nextPage: number) {
    setPage(nextPage)
    syncUrl(nextPage, appliedFilters)
  }

  const totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE))
  const rowCount = useMemo(() => logs.length, [logs.length])

  function renderPathContent(log: AuditLog) {
    const pathChange = resolveAuditPathChange(log)
    if (pathChange.fromPath !== '' || pathChange.toPath !== '') {
      return (
        <PathChangePreview
          fromPath={pathChange.fromPath}
          toPath={pathChange.toPath}
          fromLabel="变更前"
          toLabel="变更后"
        />
      )
    }
    return <p className="break-all font-mono">{pathChange.currentPath || '—'}</p>
  }

  return (
    <section className="mx-auto flex max-w-[1500px] flex-col gap-6 px-6 py-8">
      <div className="flex flex-col gap-4 border-b-2 border-foreground pb-4 md:flex-row md:items-end md:justify-between">
        <div>
          <p className="text-[10px] font-bold uppercase tracking-widest text-muted-foreground">Audit Trail</p>
          <h1 className="mt-1 text-3xl font-black tracking-tight uppercase">审计日志</h1>
          <p className="mt-2 text-sm font-bold text-muted-foreground">支持按任务、工作流运行、节点运行等结构化维度筛选。</p>
        </div>
        <button
          type="button"
          onClick={() => void fetchLogs(page, appliedFilters)}
          disabled={isLoading}
          className="inline-flex items-center gap-2 border-2 border-foreground bg-background px-4 py-2 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-background disabled:hover:text-foreground disabled:hover:shadow-none disabled:hover:translate-y-0"
        >
          <RefreshCw className={cn('h-4 w-4', isLoading && 'animate-spin')} />
          刷新
        </button>
      </div>

      <div className="border-2 border-foreground bg-card p-5 shadow-hard">
        <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
          <input
            value={filters.jobId}
            onChange={(event) => setFilters((prev) => ({ ...prev, jobId: event.target.value }))}
            placeholder="任务 ID"
            className="border-2 border-foreground bg-background px-3 py-2.5 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
          />
          <input
            value={filters.workflowRunId}
            onChange={(event) => setFilters((prev) => ({ ...prev, workflowRunId: event.target.value }))}
            placeholder="工作流运行 ID"
            className="border-2 border-foreground bg-background px-3 py-2.5 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
          />
          <input
            value={filters.nodeRunId}
            onChange={(event) => setFilters((prev) => ({ ...prev, nodeRunId: event.target.value }))}
            placeholder="节点运行 ID"
            className="border-2 border-foreground bg-background px-3 py-2.5 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
          />
          <input
            value={filters.nodeId}
            onChange={(event) => setFilters((prev) => ({ ...prev, nodeId: event.target.value }))}
            placeholder="节点 ID"
            className="border-2 border-foreground bg-background px-3 py-2.5 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
          />
          <input
            value={filters.nodeType}
            onChange={(event) => setFilters((prev) => ({ ...prev, nodeType: event.target.value }))}
            placeholder="节点类型"
            className="border-2 border-foreground bg-background px-3 py-2.5 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
          />
          <input
            value={filters.folderPath}
            onChange={(event) => setFilters((prev) => ({ ...prev, folderPath: event.target.value }))}
            placeholder="路径关键词"
            className="border-2 border-foreground bg-background px-3 py-2.5 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
          />
          <input
            value={filters.action}
            onChange={(event) => setFilters((prev) => ({ ...prev, action: event.target.value }))}
            placeholder="动作"
            className="border-2 border-foreground bg-background px-3 py-2.5 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
          />
          <input
            value={filters.result}
            onChange={(event) => setFilters((prev) => ({ ...prev, result: event.target.value }))}
            placeholder="结果（如 failed）"
            className="border-2 border-foreground bg-background px-3 py-2.5 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
          />
          <input
            type="datetime-local"
            value={filters.from}
            onChange={(event) => setFilters((prev) => ({ ...prev, from: event.target.value }))}
            className="border-2 border-foreground bg-background px-3 py-2.5 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
          />
          <input
            type="datetime-local"
            value={filters.to}
            onChange={(event) => setFilters((prev) => ({ ...prev, to: event.target.value }))}
            className="border-2 border-foreground bg-background px-3 py-2.5 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
          />
        </div>
        <div className="mt-4 flex justify-end">
          <button
            type="button"
            onClick={handleSearch}
            className="inline-flex items-center gap-2 border-2 border-foreground bg-primary px-6 py-2.5 text-sm font-bold text-primary-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
          >
            <Search className="h-4 w-4" />
            搜索
          </button>
        </div>
      </div>

      {error && (
        <div className="border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900 shadow-hard">
          {error}
        </div>
      )}

      {isMobile ? (
        <div className="space-y-3">
          {isLoading ? (
            <div className="border-2 border-foreground bg-card px-5 py-12 text-center font-bold text-muted-foreground shadow-hard">
              正在加载审计日志...
            </div>
          ) : logs.length === 0 ? (
            <div className="border-2 border-dashed border-foreground bg-card px-5 py-12 text-center font-bold text-muted-foreground shadow-hard">
              没有匹配的审计记录。
            </div>
          ) : (
            logs.map((log) => (
              <article key={log.id} className="border-2 border-foreground bg-card p-4 shadow-hard">
                <div className="flex items-start justify-between gap-2">
                  <p className="break-all text-sm font-black">{log.action}</p>
                  <span
                    className={cn(
                      'inline-flex border-2 border-foreground px-2 py-0.5 text-[10px] font-black',
                      log.result === 'success' || log.result === 'moved'
                        ? 'bg-green-300 text-green-900'
                        : 'bg-yellow-300 text-yellow-900',
                    )}
                  >
                    {log.result || 'unknown'}
                  </span>
                </div>
                <p className="mt-2 break-all font-mono text-[11px] font-bold text-muted-foreground">
                  {new Date(log.created_at).toLocaleString('zh-CN')}
                </p>
                <p className="mt-2 break-all text-xs font-bold text-red-900">{formatErrorSummary(log)}</p>
                <div className="mt-3 space-y-1 border-2 border-foreground bg-muted/20 px-3 py-2 font-mono text-[11px] font-bold text-muted-foreground">
                  <p className="break-all">job={log.job_id || '—'}</p>
                  <p className="break-all">run={log.workflow_run_id || '—'}</p>
                  <p className="break-all">nodeRun={log.node_run_id || '—'}</p>
                  <p className="break-all">node={log.node_id || '—'}</p>
                  <p className="break-all">type={log.node_type || '—'}</p>
                </div>
                <div className="mt-3 text-xs font-bold text-muted-foreground">
                  {renderPathContent(log)}
                </div>
              </article>
            ))
          )}
        </div>
      ) : (
      <div className="overflow-hidden border-2 border-foreground bg-card shadow-hard">
        <table className="table-fixed w-full min-w-0 text-sm">
          <thead className="bg-muted/50 border-b-2 border-foreground">
            <tr>
              <th className="px-4 py-4 text-left font-black tracking-widest text-foreground">时间</th>
              <th className="px-4 py-4 text-left font-black tracking-widest text-foreground">动作</th>
              <th className="px-4 py-4 text-left font-black tracking-widest text-foreground">结果</th>
              <th className="px-4 py-4 text-left font-black tracking-widest text-foreground">失败原因</th>
              <th className="px-4 py-4 text-left font-black tracking-widest text-foreground">结构化上下文</th>
              <th className="px-4 py-4 text-left font-black tracking-widest text-foreground">路径</th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              <tr>
                <td colSpan={6} className="px-5 py-12 text-center font-bold text-muted-foreground">正在加载审计日志...</td>
              </tr>
            ) : logs.length === 0 ? (
              <tr>
                <td colSpan={6} className="px-5 py-12 text-center font-bold text-muted-foreground border-2 border-dashed border-foreground m-4">没有匹配的审计记录。</td>
              </tr>
            ) : (
              logs.map((log) => (
                <tr key={log.id} className="border-b-2 border-foreground last:border-0 align-top transition-colors hover:bg-muted/30">
                  <td className="px-4 py-4 font-mono text-xs font-bold text-muted-foreground break-all">{new Date(log.created_at).toLocaleString('zh-CN')}</td>
                  <td className="px-4 py-4 font-black break-all">{log.action}</td>
                  <td className="px-4 py-4">
                    <span className={cn(
                      'inline-flex border-2 border-foreground px-2.5 py-1 text-[10px] font-black',
                      log.result === 'success' || log.result === 'moved'
                        ? 'bg-green-300 text-green-900'
                        : 'bg-yellow-300 text-yellow-900',
                    )}>
                      {log.result || 'unknown'}
                    </span>
                  </td>
                  <td className="px-4 py-4 text-xs font-bold text-red-900 break-all">{formatErrorSummary(log)}</td>
                  <td className="px-4 py-4 text-[11px] font-mono text-muted-foreground break-all">
                    <div>job={log.job_id || '—'}</div>
                    <div>run={log.workflow_run_id || '—'}</div>
                    <div>nodeRun={log.node_run_id || '—'}</div>
                    <div>node={log.node_id || '—'}</div>
                    <div>type={log.node_type || '—'}</div>
                  </td>
                  <td className="px-4 py-4 text-xs font-bold text-muted-foreground">
                    {renderPathContent(log)}
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
      )}

      <div className="flex flex-wrap items-center justify-between gap-3 border-2 border-foreground bg-card px-4 py-4 text-sm shadow-hard sm:px-5">
        <p className="font-bold text-muted-foreground">第 <span className="text-foreground font-black">{page}</span> / {totalPages} 页，共 <span className="text-foreground font-black">{total}</span> 条（当前 {rowCount} 条）</p>
        <div className="flex gap-3">
          <button
            type="button"
            disabled={page <= 1 || isLoading}
            onClick={() => handlePageChange(Math.max(1, page - 1))}
            className="border-2 border-foreground bg-background px-4 py-2 font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-background disabled:hover:text-foreground disabled:hover:shadow-none disabled:hover:translate-y-0"
          >
            上一页
          </button>
          <button
            type="button"
            disabled={page >= totalPages || isLoading}
            onClick={() => handlePageChange(Math.min(totalPages, page + 1))}
            className="border-2 border-foreground bg-background px-4 py-2 font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-background disabled:hover:text-foreground disabled:hover:shadow-none disabled:hover:translate-y-0"
          >
            下一页
          </button>
        </div>
      </div>
    </section>
  )
}
