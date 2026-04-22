import { useEffect, useMemo, useRef, useState } from 'react'

import { FolderSearch, Play, Plus, RefreshCw, Trash2, X } from 'lucide-react'
import gsap from 'gsap'
import { useNavigate } from 'react-router-dom'

import {
  createScheduledWorkflow,
  deleteScheduledWorkflow,
  listScheduledWorkflows,
  runScheduledWorkflowNow,
  updateScheduledWorkflow,
  type ScheduledWorkflowBody,
} from '@/api/scheduledWorkflows'
import { listWorkflowDefs } from '@/api/workflowDefs'
import { CronExpressionField } from '@/components/CronExpressionField'
import { DirPicker } from '@/components/DirPicker'
import { useIsMobile } from '@/hooks/useIsMobile'
import { cn } from '@/lib/utils'
import { useConfigStore } from '@/store/configStore'
import type { ScheduledWorkflow, WorkflowDefinition } from '@/types'

function formatDate(dateStr: string | null) {
  if (!dateStr) return '—'
  return new Date(dateStr).toLocaleString('zh-CN')
}

interface ScheduledWorkflowFormState {
  jobType: 'workflow' | 'scan'
  name: string
  workflowDefId: string
  cronSpec: string
  enabled: boolean
  sourceDirs: string[]
}

const EMPTY_SCHEDULED_WORKFLOW_FORM: ScheduledWorkflowFormState = {
  jobType: 'workflow',
  name: '',
  workflowDefId: '',
  cronSpec: '0 * * * *',
  enabled: true,
  sourceDirs: [],
}

type ScheduledWorkflowModalMode =
  | { kind: 'create' }
  | { kind: 'edit'; workflow: ScheduledWorkflow }

function ScheduledWorkflowTable({
  workflows,
  workflowDefs,
  isLoading,
  isMobile,
  runningId,
  onCreate,
  onEdit,
  onDelete,
  onRunNow,
}: {
  workflows: ScheduledWorkflow[]
  workflowDefs: WorkflowDefinition[]
  isLoading: boolean
  isMobile: boolean
  runningId: string | null
  onCreate: () => void
  onEdit: (workflow: ScheduledWorkflow) => void
  onDelete: (workflow: ScheduledWorkflow) => Promise<void>
  onRunNow: (workflow: ScheduledWorkflow) => Promise<void>
}) {
  const workflowNameMap = useMemo(() => {
    return workflowDefs.reduce<Record<string, string>>((acc, item) => {
      acc[item.id] = item.name
      return acc
    }, {})
  }, [workflowDefs])

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
        <div>
          <h2 className="text-xl font-black tracking-tight">计划任务</h2>
          <p className="mt-1 text-sm font-medium text-muted-foreground">用 cron 管理工作流执行。</p>
        </div>
        <button
          type="button"
          onClick={onCreate}
          className="inline-flex w-full items-center justify-center gap-2 border-2 border-foreground bg-primary px-4 py-2 text-sm font-bold text-primary-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 sm:w-auto"
        >
          <Plus className="h-4 w-4" />
          新建计划任务
        </button>
      </div>

      {isMobile ? (
        <div className="space-y-3">
          {isLoading ? (
            <div className="border-2 border-foreground bg-card px-4 py-16 text-center font-bold text-muted-foreground shadow-hard">
              正在加载计划任务...
            </div>
          ) : workflows.length === 0 ? (
            <div className="border-2 border-dashed border-foreground bg-card px-4 py-16 text-center font-bold text-muted-foreground shadow-hard">
              暂无计划任务，可创建带 cron 的工作流作业。
            </div>
          ) : (
            workflows.map((workflow) => (
              <article key={workflow.id} className="border-2 border-foreground bg-card p-4 shadow-hard">
                <div className="flex flex-wrap items-start justify-between gap-2">
                  <p className="break-all text-sm font-black">{workflow.name}</p>
                  <span
                    className={cn(
                      'inline-flex border-2 border-foreground px-2 py-0.5 text-[10px] font-black',
                      workflow.enabled ? 'bg-green-300 text-green-900' : 'bg-gray-200 text-gray-900',
                    )}
                  >
                    {workflow.enabled ? '已启用' : '已停用'}
                  </span>
                </div>
                <div className="mt-3 space-y-1 text-xs font-bold text-muted-foreground">
                  <p>{workflow.job_type === 'scan' ? '扫描' : '工作流'}</p>
                  <p className="break-all">{workflow.job_type === 'scan' ? '扫描目录' : (workflowNameMap[workflow.workflow_def_id] ?? workflow.workflow_def_id)}</p>
                  <p className="break-all font-mono">{workflow.cron_spec}</p>
                  <p className="font-mono">{formatDate(workflow.last_run_at)}</p>
                  <p className="tabular-nums text-foreground">{workflow.job_type === 'scan' ? workflow.source_dirs.length : workflow.folder_ids.length}</p>
                </div>
                <div className="mt-4 grid grid-cols-1 gap-2 sm:grid-cols-3">
                  <button
                    type="button"
                    onClick={() => onEdit(workflow)}
                    className="border-2 border-foreground bg-background px-3 py-2 text-xs font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
                  >
                    编辑
                  </button>
                  <button
                    type="button"
                    onClick={() => void onRunNow(workflow)}
                    disabled={runningId === workflow.id}
                    className="inline-flex items-center justify-center gap-1 border-2 border-foreground bg-primary px-3 py-2 text-xs font-bold text-primary-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-primary disabled:hover:text-primary-foreground disabled:hover:shadow-none disabled:hover:translate-y-0"
                  >
                    <Play className="h-3 w-3" />
                    {runningId === workflow.id ? '启动中' : '立即执行'}
                  </button>
                  <button
                    type="button"
                    onClick={() => void onDelete(workflow)}
                    className="inline-flex items-center justify-center gap-1 border-2 border-red-900 bg-red-100 px-3 py-2 text-xs font-bold text-red-900 transition-all hover:bg-red-900 hover:text-red-100 hover:shadow-hard hover:-translate-y-0.5"
                  >
                    <Trash2 className="h-3 w-3" />
                    删除
                  </button>
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
              <th className="px-4 py-4 text-left font-black tracking-widest">名称</th>
              <th className="px-4 py-4 text-left font-black tracking-widest">类型</th>
              <th className="px-4 py-4 text-left font-black tracking-widest">工作流</th>
              <th className="px-4 py-4 text-left font-black tracking-widest">Cron</th>
              <th className="px-4 py-4 text-left font-black tracking-widest">目录数</th>
              <th className="px-4 py-4 text-left font-black tracking-widest">状态</th>
              <th className="px-4 py-4 text-left font-black tracking-widest">上次执行</th>
              <th className="px-4 py-4 text-left font-black tracking-widest">操作</th>
            </tr>
          </thead>
          <tbody>
            {isLoading ? (
              <tr>
                <td colSpan={8} className="px-4 py-16 text-center font-bold text-muted-foreground">正在加载计划任务...</td>
              </tr>
            ) : workflows.length === 0 ? (
              <tr>
                <td colSpan={8} className="px-4 py-16 text-center font-bold text-muted-foreground border-2 border-dashed border-foreground m-4">暂无计划任务，可创建带 cron 的工作流作业。</td>
              </tr>
            ) : (
              workflows.map((workflow) => (
                <tr key={workflow.id} className="scheduled-row border-b-2 border-foreground last:border-0 hover:bg-muted/30 transition-colors">
                  <td className="px-4 py-4 font-black break-all">{workflow.name}</td>
                  <td className="px-4 py-4 font-bold text-muted-foreground">{workflow.job_type === 'scan' ? '扫描' : '工作流'}</td>
                  <td className="px-4 py-4 font-bold text-muted-foreground">{workflow.job_type === 'scan' ? '扫描目录' : (workflowNameMap[workflow.workflow_def_id] ?? workflow.workflow_def_id)}</td>
                  <td className="px-4 py-4 font-mono text-xs font-bold bg-muted/50 px-2">{workflow.cron_spec}</td>
                  <td className="px-4 py-4 font-black tabular-nums">{workflow.job_type === 'scan' ? workflow.source_dirs.length : workflow.folder_ids.length}</td>
                  <td className="px-4 py-4">
                    <span className={cn(
                      'inline-flex border-2 border-foreground px-2 py-0.5 text-[10px] font-black',
                      workflow.enabled ? 'bg-green-300 text-green-900' : 'bg-gray-200 text-gray-900',
                    )}>
                      {workflow.enabled ? '已启用' : '已停用'}
                    </span>
                  </td>
                  <td className="px-4 py-4 font-mono text-xs font-bold text-muted-foreground">{formatDate(workflow.last_run_at)}</td>
                  <td className="px-4 py-4">
                    <div className="flex flex-wrap items-center gap-2">
                      <button
                        type="button"
                        onClick={() => onEdit(workflow)}
                        className="border-2 border-foreground bg-background px-3 py-1.5 text-xs font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
                      >
                        编辑
                      </button>
                      <button
                        type="button"
                        onClick={() => void onRunNow(workflow)}
                        disabled={runningId === workflow.id}
                        className="inline-flex items-center gap-1 border-2 border-foreground bg-primary px-3 py-1.5 text-xs font-bold text-primary-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-primary disabled:hover:text-primary-foreground disabled:hover:shadow-none disabled:hover:translate-y-0"
                      >
                        <Play className="h-3 w-3" />
                        {runningId === workflow.id ? '启动中' : '立即执行'}
                      </button>
                      <button
                        type="button"
                        onClick={() => void onDelete(workflow)}
                        className="inline-flex items-center gap-1 border-2 border-red-900 bg-red-100 px-3 py-1.5 text-xs font-bold text-red-900 transition-all hover:bg-red-900 hover:text-red-100 hover:shadow-hard hover:-translate-y-0.5"
                      >
                        <Trash2 className="h-3 w-3" />
                        删除
                      </button>
                    </div>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
      )}
    </div>
  )
}

function ScheduledWorkflowModal({
  modal,
  form,
  workflowDefs,
  formError,
  isSaving,
  onClose,
  onChange,
  onToggleSourceDir,
  onSave,
}: {
  modal: ScheduledWorkflowModalMode | null
  form: ScheduledWorkflowFormState
  workflowDefs: WorkflowDefinition[]
  formError: string | null
  isSaving: boolean
  onClose: () => void
  onChange: (patch: Partial<ScheduledWorkflowFormState>) => void
  onToggleSourceDir: (sourceDir: string) => void
  onSave: () => Promise<void>
}) {
  const [dirPickerOpen, setDirPickerOpen] = useState(false)
  const overlayRef = useRef<HTMLDivElement | null>(null)
  const modalRef = useRef<HTMLDivElement | null>(null)
  const { scanInputDirs, load: loadConfig } = useConfigStore()

  useEffect(() => {
    if (modal && overlayRef.current && modalRef.current) {
      gsap.fromTo(overlayRef.current, { opacity: 0 }, { opacity: 1, duration: 0.2 })
      gsap.fromTo(modalRef.current, { scale: 0.8, opacity: 0 }, { scale: 1, opacity: 1, duration: 0.4, ease: 'back.out(1.7)' })
    }
  }, [modal])

  useEffect(() => {
    void loadConfig()
  }, [loadConfig])

  if (!modal) return null

  return (
    <div ref={overlayRef} className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 px-4 py-8 overflow-y-auto">
      <div ref={modalRef} className="my-auto max-h-[calc(100dvh-2rem)] w-full max-w-4xl overflow-y-auto border-2 border-foreground bg-background shadow-hard-lg">
        <div className="flex items-center justify-between border-b-2 border-foreground bg-primary px-6 py-5 text-primary-foreground">
          <div>
            <h2 className="text-xl font-black tracking-tight">{modal.kind === 'create' ? '新建计划任务' : '编辑计划任务'}</h2>
            <p className="mt-1 text-sm font-medium">选择工作流、目标目录和 cron 规则，统一在作业页管理。</p>
          </div>
          <button type="button" onClick={onClose} className="border-2 border-transparent p-2 transition-all hover:border-primary-foreground hover:bg-foreground hover:text-background">
            <X className="h-5 w-5" />
          </button>
        </div>

        <div className="p-6 grid gap-8 lg:grid-cols-[1fr,1.2fr]">
          <div className="space-y-5 min-w-0">
            <div>
              <label className="mb-2 block text-sm font-black tracking-widest">任务类型</label>
              <select
                value={form.jobType}
                onChange={(event) => onChange({ jobType: event.target.value as 'workflow' | 'scan' })}
                className="w-full border-2 border-foreground bg-background px-4 py-3 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
              >
                <option value="workflow">工作流</option>
                <option value="scan">扫描</option>
              </select>
            </div>

            <div>
              <label className="mb-2 block text-sm font-black tracking-widest">任务名称</label>
              <input
                value={form.name}
                onChange={(event) => onChange({ name: event.target.value })}
                className="w-full border-2 border-foreground bg-background px-4 py-3 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
                placeholder="例如：每小时整理已扫描目录"
              />
            </div>

            {form.jobType === 'workflow' && (
              <div>
                <label className="mb-2 block text-sm font-black tracking-widest">工作流定义</label>
                <select
                  value={form.workflowDefId}
                  onChange={(event) => onChange({ workflowDefId: event.target.value })}
                  className="w-full border-2 border-foreground bg-background px-4 py-3 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
                >
                  <option value="">请选择工作流</option>
                  {workflowDefs.map((workflowDef) => (
                    <option key={workflowDef.id} value={workflowDef.id}>{workflowDef.name}</option>
                  ))}
                </select>
              </div>
            )}

            <div>
              <label className="mb-2 block text-sm font-black tracking-widest">CRON 表达式</label>
              <CronExpressionField
                value={form.cronSpec}
                onChange={(value) => onChange({ cronSpec: value })}
              />
            </div>

            <label className="flex items-center gap-3 border-2 border-foreground bg-muted/10 px-4 py-4 text-sm font-bold cursor-pointer hover:bg-muted/30 transition-colors">
              <input
                type="checkbox"
                checked={form.enabled}
                onChange={(event) => onChange({ enabled: event.target.checked })}
                className="h-5 w-5 rounded-none border-2 border-foreground text-foreground focus:ring-foreground focus:ring-offset-0"
              />
              <span>创建后立即启用调度</span>
            </label>

            {formError && <div className="border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900 shadow-hard">{formError}</div>}
          </div>

          <div className="space-y-4 min-w-0">
            <div>
              <label className="mb-2 block text-sm font-black tracking-widest">{form.jobType === 'scan' ? '扫描输入目录' : '目录输入说明'}</label>
              {form.jobType === 'workflow' ? (
                <div className="border-2 border-foreground bg-card px-4 py-4 text-sm font-bold text-muted-foreground shadow-hard">
                  v2 工作流计划任务不再绑定目录列表；目录输入请在工作流图内配置（如 `folder-picker` / `folder-tree-scanner`）。
                </div>
              ) : (
                <div className="flex flex-col gap-3 border-2 border-foreground bg-card px-4 py-4 shadow-hard">
                  <div>
                    <p className="text-sm font-black">已选择 {form.sourceDirs.length} 个扫描输入目录</p>
                    <p className="text-xs font-medium text-muted-foreground mt-1">每个扫描任务维护自己的目录列表。</p>
                  </div>
                  {scanInputDirs.length > 0 && (
                    <div className="space-y-2">
                      <p className="text-xs font-black tracking-widest">从系统扫描目录添加</p>
                      <div className="max-h-28 space-y-1 overflow-auto">
                        {scanInputDirs.map((path, index) => (
                          <button
                            key={`${path}-${index}`}
                            type="button"
                            onClick={() => onToggleSourceDir(path)}
                            className="w-full border-2 border-foreground bg-background px-3 py-2 text-left text-xs font-bold transition-all hover:bg-foreground hover:text-background"
                          >
                            <span className="block truncate">{`扫描目录 #${index + 1}`}</span>
                            <span className="block truncate font-mono text-[10px] opacity-70">{path}</span>
                          </button>
                        ))}
                      </div>
                    </div>
                  )}
                  <button
                    type="button"
                    onClick={() => setDirPickerOpen(true)}
                    className="inline-flex items-center justify-center gap-2 border-2 border-foreground bg-background px-4 py-2 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
                  >
                    <FolderSearch className="h-4 w-4" />
                    添加目录
                  </button>
                </div>
              )}
            </div>

            <div className="h-[320px] overflow-y-auto border-2 border-foreground bg-muted/10 p-3">
              <div className="space-y-2">
                {form.jobType === 'scan' ? form.sourceDirs.map((dir) => (
                  <div key={dir} className="flex items-center justify-between border-2 border-foreground bg-background px-3 py-3 text-sm transition-colors hover:bg-muted/30">
                    <p className="break-all font-mono text-xs font-bold text-foreground">{dir}</p>
                    <button
                      type="button"
                      onClick={() => onToggleSourceDir(dir)}
                      className="ml-3 shrink-0 border-2 border-red-900 bg-red-100 px-2 py-1 text-xs font-bold text-red-900 transition-all hover:bg-red-900 hover:text-red-100 hover:shadow-hard hover:-translate-y-0.5"
                    >
                      移除
                    </button>
                  </div>
                )) : (
                  <div className="border-2 border-dashed border-foreground bg-background px-4 py-12 text-center text-sm font-bold text-muted-foreground">
                    v2 工作流计划任务不再绑定目录列表。
                  </div>
                )}
                {(form.jobType === 'scan' && form.sourceDirs.length === 0) && (
                  <div className="border-2 border-dashed border-foreground px-4 py-12 text-center text-sm font-bold text-muted-foreground">
                    尚未为这个扫描任务添加输入目录。
                  </div>
                )}
              </div>
            </div>
          </div>
        </div>

        <div className="flex flex-wrap items-center justify-end gap-3 border-t-2 border-foreground bg-muted/30 px-6 py-5">
          <button type="button" onClick={onClose} disabled={isSaving} className="border-2 border-foreground bg-background px-6 py-2.5 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50">
            取消
          </button>
          <button type="button" onClick={() => void onSave()} disabled={isSaving} className="border-2 border-foreground bg-primary px-6 py-2.5 text-sm font-bold text-primary-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-primary disabled:hover:text-primary-foreground disabled:hover:shadow-none disabled:hover:translate-y-0">
            {isSaving ? '保存中...' : '保存'}
          </button>
        </div>

        <DirPicker
          open={dirPickerOpen}
          initialPath={scanInputDirs[0] ?? '/'}
          title="选择扫描输入目录"
          onCancel={() => setDirPickerOpen(false)}
          onConfirm={(path) => {
            setDirPickerOpen(false)
            if (!form.sourceDirs.includes(path)) {
              onChange({ sourceDirs: [...form.sourceDirs, path] })
            }
          }}
        />
      </div>
    </div>
  )
}

export default function JobsPage() {
  const navigate = useNavigate()
  const isMobile = useIsMobile(1024)
  const [scheduledWorkflows, setScheduledWorkflows] = useState<ScheduledWorkflow[]>([])
  const [workflowDefs, setWorkflowDefs] = useState<WorkflowDefinition[]>([])
  const [isScheduledLoading, setIsScheduledLoading] = useState(false)
  const [scheduledError, setScheduledError] = useState<string | null>(null)
  const [modal, setModal] = useState<ScheduledWorkflowModalMode | null>(null)
  const [form, setForm] = useState<ScheduledWorkflowFormState>(EMPTY_SCHEDULED_WORKFLOW_FORM)
  const [formError, setFormError] = useState<string | null>(null)
  const [isSaving, setIsSaving] = useState(false)
  const [runningId, setRunningId] = useState<string | null>(null)

  useEffect(() => {
    void fetchScheduledData()
  }, [])

  useEffect(() => {
    if (!isScheduledLoading && scheduledWorkflows.length > 0) {
      gsap.fromTo(
        '.scheduled-row',
        { opacity: 0, x: -20 },
        { opacity: 1, x: 0, duration: 0.4, stagger: 0.05, ease: 'power2.out', clearProps: 'all' },
      )
    }
  }, [scheduledWorkflows, isScheduledLoading])

  async function fetchScheduledData() {
    setIsScheduledLoading(true)
    setScheduledError(null)
    try {
      const [workflowRes, scheduledRes] = await Promise.all([
        listWorkflowDefs({ limit: 100 }),
        listScheduledWorkflows(),
      ])
      setWorkflowDefs(workflowRes.data)
      setScheduledWorkflows(scheduledRes.data)
    } catch (loadError) {
      setScheduledError(loadError instanceof Error ? loadError.message : '加载计划任务失败')
    } finally {
      setIsScheduledLoading(false)
    }
  }

  function openCreateModal() {
    setForm(EMPTY_SCHEDULED_WORKFLOW_FORM)
    setFormError(null)
    setModal({ kind: 'create' })
  }

  function openEditModal(workflow: ScheduledWorkflow) {
    setForm({
      jobType: workflow.job_type,
      name: workflow.name,
      workflowDefId: workflow.workflow_def_id,
      cronSpec: workflow.cron_spec,
      enabled: workflow.enabled,
      sourceDirs: workflow.source_dirs,
    })
    setFormError(null)
    setModal({ kind: 'edit', workflow })
  }

  function closeModal() {
    setModal(null)
    setFormError(null)
  }

  function toggleSourceDir(sourceDir: string) {
    setForm((prev) => ({
      ...prev,
      sourceDirs: prev.sourceDirs.includes(sourceDir)
        ? prev.sourceDirs.filter((item) => item !== sourceDir)
        : [...prev.sourceDirs, sourceDir],
    }))
  }

  async function handleSaveScheduledWorkflow() {
    if (!form.name.trim()) {
      setFormError('任务名称不能为空')
      return
    }
    if (form.jobType === 'workflow' && !form.workflowDefId) {
      setFormError('请选择工作流定义')
      return
    }
    if (!form.cronSpec.trim()) {
      setFormError('Cron 表达式不能为空')
      return
    }
    if (form.jobType === 'scan' && form.sourceDirs.length === 0) {
      setFormError('请至少选择一个扫描输入目录')
      return
    }

    const body: ScheduledWorkflowBody = {
      job_type: form.jobType,
      name: form.name.trim(),
      workflow_def_id: form.jobType === 'workflow' ? form.workflowDefId : '',
      cron_spec: form.cronSpec.trim(),
      enabled: form.enabled,
      folder_ids: [],
      source_dirs: form.jobType === 'scan' ? form.sourceDirs : [],
    }

    setIsSaving(true)
    setFormError(null)
    try {
      if (modal?.kind === 'create') {
        await createScheduledWorkflow(body)
      } else if (modal?.kind === 'edit') {
        await updateScheduledWorkflow(modal.workflow.id, body)
      }
      closeModal()
      await fetchScheduledData()
    } catch (saveError) {
      setFormError(saveError instanceof Error ? saveError.message : '保存计划任务失败')
    } finally {
      setIsSaving(false)
    }
  }

  async function handleDeleteScheduledWorkflow(workflow: ScheduledWorkflow) {
    if (!window.confirm(`确认删除计划任务「${workflow.name}」？`)) return
    await deleteScheduledWorkflow(workflow.id)
    await fetchScheduledData()
  }

  async function handleRunNow(workflow: ScheduledWorkflow) {
    setRunningId(workflow.id)
    try {
      await runScheduledWorkflowNow(workflow.id)
      await fetchScheduledData()
      navigate('/job-history')
    } finally {
      setRunningId(null)
    }
  }

  return (
    <div className="flex flex-col gap-8 p-6">
      <div className="flex items-end justify-between border-b-2 border-foreground pb-4">
        <div>
          <h1 className="text-3xl font-black tracking-tight uppercase">计划任务</h1>
          <p className="mt-1 text-sm font-bold text-muted-foreground">管理定时执行计划，执行结果请前往“执行历史”。</p>
        </div>
        <button
          type="button"
          onClick={() => void fetchScheduledData()}
          disabled={isScheduledLoading}
          className="flex items-center gap-2 border-2 border-foreground bg-background px-4 py-2 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-background disabled:hover:text-foreground disabled:hover:shadow-none disabled:hover:translate-y-0"
        >
          <RefreshCw className={cn('h-4 w-4', isScheduledLoading && 'animate-spin')} />
          刷新
        </button>
      </div>

      {scheduledError && (
        <div className="border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900 shadow-hard">{scheduledError}</div>
      )}

      <ScheduledWorkflowTable
        workflows={scheduledWorkflows}
        workflowDefs={workflowDefs}
        isLoading={isScheduledLoading}
        isMobile={isMobile}
        runningId={runningId}
        onCreate={openCreateModal}
        onEdit={openEditModal}
        onDelete={handleDeleteScheduledWorkflow}
        onRunNow={handleRunNow}
      />

      <ScheduledWorkflowModal
        modal={modal}
        form={form}
        workflowDefs={workflowDefs}
        formError={formError}
        isSaving={isSaving}
        onClose={closeModal}
        onChange={(patch) => setForm((prev) => ({ ...prev, ...patch }))}
        onToggleSourceDir={toggleSourceDir}
        onSave={handleSaveScheduledWorkflow}
      />
    </div>
  )
}
