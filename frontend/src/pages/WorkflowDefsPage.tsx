import { useEffect, useState } from 'react'
import { Check, Loader2, Pencil, Play, Plus, Trash2 } from 'lucide-react'
import { createPortal } from 'react-dom'
import { Link, useNavigate } from 'react-router-dom'

import { listFolders } from '@/api/folders'
import { updateWorkflowDef } from '@/api/workflowDefs'
import { startWorkflowJob } from '@/api/workflowRuns'
import { WorkflowRunStatusCard } from '@/components/WorkflowRunStatusCard'
import {
  getWorkflowFolderLaunchability,
  launchWorkflowForFolder,
} from '@/lib/workflowFolderLaunch'
import { cn } from '@/lib/utils'
import { useWorkflowRunStore } from '@/store/workflowRunStore'
import { useWorkflowDefStore } from '@/store/workflowDefStore'
import type { Folder, WorkflowDefinition, WorkflowGraph } from '@/types'

export type WorkflowDefsPageProps = Record<string, never>

// ─── Template Graphs ─────────────────────────────────────────────────────────

const TEMPLATE_CLASSIFICATION_ONLY = JSON.stringify({
  nodes: [
    { id: 'n-trigger', type: 'trigger', label: '触发器', config: {}, inputs: {}, enabled: true, ui_position: { x: 60, y: 280 } },
    { id: 'n-scanner', type: 'folder-tree-scanner', label: '目录树扫描器', config: {}, inputs: {}, enabled: true, ui_position: { x: 260, y: 280 } },
    { id: 'n-kw', type: 'name-keyword-classifier', label: '关键词分类器', config: {}, inputs: {}, enabled: true, ui_position: { x: 520, y: 100 } },
    { id: 'n-ft', type: 'file-tree-classifier', label: '文件树分类器', config: {}, inputs: {}, enabled: true, ui_position: { x: 520, y: 260 } },
    { id: 'n-ext', type: 'ext-ratio-classifier', label: '扩展名分类器', config: {}, inputs: {}, enabled: true, ui_position: { x: 520, y: 420 } },
    { id: 'n-agg', type: 'subtree-aggregator', label: '子树聚合器', config: {}, inputs: {}, enabled: true, ui_position: { x: 780, y: 260 } },
  ],
  edges: [
    { id: 'e1', source: 'n-scanner', source_port: 0, target: 'n-kw', target_port: 0 },
    { id: 'e2', source: 'n-scanner', source_port: 0, target: 'n-ft', target_port: 0 },
    { id: 'e3', source: 'n-scanner', source_port: 0, target: 'n-ext', target_port: 0 },
    { id: 'e4', source: 'n-kw', source_port: 0, target: 'n-agg', target_port: 1 },
    { id: 'e5', source: 'n-ft', source_port: 0, target: 'n-agg', target_port: 2 },
    { id: 'e6', source: 'n-ext', source_port: 0, target: 'n-agg', target_port: 3 },
  ],
})

const TEMPLATE_CLASSIFY_AND_MOVE = JSON.stringify({
  nodes: [
    { id: 'n-trigger', type: 'trigger', label: '触发器', config: {}, inputs: {}, enabled: true, ui_position: { x: 60, y: 320 } },
    { id: 'n-scanner', type: 'folder-tree-scanner', label: '目录树扫描器', config: {}, inputs: {}, enabled: true, ui_position: { x: 260, y: 320 } },
    { id: 'n-kw', type: 'name-keyword-classifier', label: '关键词分类器', config: {}, inputs: {}, enabled: true, ui_position: { x: 520, y: 120 } },
    { id: 'n-ft', type: 'file-tree-classifier', label: '文件树分类器', config: {}, inputs: {}, enabled: true, ui_position: { x: 520, y: 300 } },
    { id: 'n-ext', type: 'ext-ratio-classifier', label: '扩展名分类器', config: {}, inputs: {}, enabled: true, ui_position: { x: 520, y: 480 } },
    { id: 'n-agg', type: 'subtree-aggregator', label: '子树聚合器', config: {}, inputs: {}, enabled: true, ui_position: { x: 780, y: 300 } },
    { id: 'n-reader', type: 'classification-reader', label: '分类读取器', config: {}, inputs: {}, enabled: true, ui_position: { x: 1120, y: 300 } },
    { id: 'n-splitter', type: 'folder-splitter', label: '文件夹拆分器', config: {}, inputs: {}, enabled: true, ui_position: { x: 1480, y: 300 } },
    { id: 'n-router', type: 'category-router', label: '类别路由器', config: {}, inputs: {}, enabled: true, ui_position: { x: 1700, y: 300 } },
    { id: 'n-move-video', type: 'move-node', label: '移动节点（视频）', config: { path_ref_type: 'output', path_ref_key: 'video', path_suffix: '' }, inputs: {}, enabled: true, ui_position: { x: 1960, y: 100 } },
    { id: 'n-move-manga', type: 'move-node', label: '移动节点（漫画）', config: { path_ref_type: 'output', path_ref_key: 'manga', path_suffix: '' }, inputs: {}, enabled: true, ui_position: { x: 1960, y: 260 } },
    { id: 'n-move-photo', type: 'move-node', label: '移动节点（图片）', config: { path_ref_type: 'output', path_ref_key: 'photo', path_suffix: '' }, inputs: {}, enabled: true, ui_position: { x: 1960, y: 420 } },
    { id: 'n-move-other', type: 'move-node', label: '移动节点（其他）', config: { path_ref_type: 'output', path_ref_key: 'other', path_suffix: '' }, inputs: {}, enabled: true, ui_position: { x: 1960, y: 580 } },
  ],
  edges: [
    { id: 'e1', source: 'n-scanner', source_port: 0, target: 'n-kw', target_port: 0 },
    { id: 'e2', source: 'n-scanner', source_port: 0, target: 'n-ft', target_port: 0 },
    { id: 'e3', source: 'n-scanner', source_port: 0, target: 'n-ext', target_port: 0 },
    { id: 'e4', source: 'n-kw', source_port: 0, target: 'n-agg', target_port: 1 },
    { id: 'e5', source: 'n-ft', source_port: 0, target: 'n-agg', target_port: 2 },
    { id: 'e6', source: 'n-ext', source_port: 0, target: 'n-agg', target_port: 3 },
    { id: 'e7', source: 'n-agg', source_port: 0, target: 'n-reader', target_port: 0 },
    { id: 'e9', source: 'n-reader', source_port: 0, target: 'n-splitter', target_port: 0 },
    { id: 'e10', source: 'n-splitter', source_port: 0, target: 'n-router', target_port: 0 },
    { id: 'e11', source: 'n-router', source_port: 0, target: 'n-move-video', target_port: 0 },
    { id: 'e12', source: 'n-router', source_port: 1, target: 'n-move-manga', target_port: 0 },
    { id: 'e13', source: 'n-router', source_port: 2, target: 'n-move-photo', target_port: 0 },
    { id: 'e14', source: 'n-router', source_port: 3, target: 'n-move-other', target_port: 0 },
  ],
})

const TEMPLATE_GENERIC_PROCESSING = JSON.stringify({
  nodes: [
    { id: 'g-reader', type: 'classification-reader', label: '分类读取器', config: {}, inputs: {}, enabled: true, ui_position: { x: 80, y: 280 } },
    { id: 'g-split', type: 'folder-splitter', label: '文件夹拆分器', config: { split_mixed: true, split_depth: 1 }, inputs: { entry: { link_source: { source_node_id: 'g-reader', source_port: 'entry' } } }, enabled: true, ui_position: { x: 320, y: 280 } },
    { id: 'g-router', type: 'category-router', label: '类别路由器', config: {}, inputs: { items: { link_source: { source_node_id: 'g-split', source_port: 'items' } } }, enabled: true, ui_position: { x: 560, y: 280 } },
    { id: 'g-rename-video', type: 'rename-node', label: '重命名（视频）', config: { strategy: 'template', template: '{name}' }, inputs: { items: { link_source: { source_node_id: 'g-router', source_port: 'video' } } }, enabled: true, ui_position: { x: 860, y: 60 } },
    { id: 'g-rename-manga', type: 'rename-node', label: '重命名（漫画）', config: { strategy: 'template', template: '{name}' }, inputs: { items: { link_source: { source_node_id: 'g-router', source_port: 'manga' } } }, enabled: true, ui_position: { x: 860, y: 180 } },
    { id: 'g-rename-photo', type: 'rename-node', label: '重命名（图片）', config: { strategy: 'template', template: '{name}' }, inputs: { items: { link_source: { source_node_id: 'g-router', source_port: 'photo' } } }, enabled: true, ui_position: { x: 860, y: 300 } },
    { id: 'g-rename-other', type: 'rename-node', label: '重命名（其他）', config: { strategy: 'template', template: '{name}' }, inputs: { items: { link_source: { source_node_id: 'g-router', source_port: 'other' } } }, enabled: true, ui_position: { x: 860, y: 420 } },
    { id: 'g-mixed-router', type: 'mixed-leaf-router', label: '混合叶子分流器', config: {}, inputs: { items: { link_source: { source_node_id: 'g-router', source_port: 'mixed_leaf' } } }, enabled: true, ui_position: { x: 860, y: 540 } },
    { id: 'g-rename-mixed-video', type: 'rename-node', label: '重命名（混合-视频）', config: { strategy: 'template', template: '{name}' }, inputs: { items: { link_source: { source_node_id: 'g-mixed-router', source_port: 'video' } } }, enabled: true, ui_position: { x: 1160, y: 520 } },
    { id: 'g-rename-mixed-photo', type: 'rename-node', label: '重命名（混合-图片）', config: { strategy: 'template', template: '{name}' }, inputs: { items: { link_source: { source_node_id: 'g-mixed-router', source_port: 'photo' } } }, enabled: true, ui_position: { x: 1160, y: 640 } },
    { id: 'g-collect', type: 'collect-node', label: '收集节点', config: {}, inputs: { items_1: { link_source: { source_node_id: 'g-rename-video', source_port: 'items' } }, items_2: { link_source: { source_node_id: 'g-rename-manga', source_port: 'items' } }, items_3: { link_source: { source_node_id: 'g-rename-photo', source_port: 'items' } }, items_4: { link_source: { source_node_id: 'g-rename-other', source_port: 'items' } }, items_5: { link_source: { source_node_id: 'g-rename-mixed-video', source_port: 'items' } }, items_6: { link_source: { source_node_id: 'g-rename-mixed-photo', source_port: 'items' } } }, enabled: true, ui_position: { x: 1460, y: 320 } },
    { id: 'g-move', type: 'move-node', label: '移动节点', config: { path_ref_type: 'output', path_ref_key: 'mixed', path_suffix: '.processed', move_unit: 'folder', conflict_policy: 'auto_rename' }, inputs: { items: { link_source: { source_node_id: 'g-collect', source_port: 'items' } } }, enabled: true, ui_position: { x: 1720, y: 320 } },
  ],
  edges: [
    { id: 'g-e1', source: 'g-reader', source_port: 'entry', target: 'g-split', target_port: 'entry' },
    { id: 'g-e2', source: 'g-split', source_port: 'items', target: 'g-router', target_port: 'items' },
    { id: 'g-e3', source: 'g-router', source_port: 'video', target: 'g-rename-video', target_port: 'items' },
    { id: 'g-e4', source: 'g-router', source_port: 'manga', target: 'g-rename-manga', target_port: 'items' },
    { id: 'g-e5', source: 'g-router', source_port: 'photo', target: 'g-rename-photo', target_port: 'items' },
    { id: 'g-e6', source: 'g-router', source_port: 'other', target: 'g-rename-other', target_port: 'items' },
    { id: 'g-e7', source: 'g-router', source_port: 'mixed_leaf', target: 'g-mixed-router', target_port: 'items' },
    { id: 'g-e8', source: 'g-mixed-router', source_port: 'video', target: 'g-rename-mixed-video', target_port: 'items' },
    { id: 'g-e9', source: 'g-mixed-router', source_port: 'photo', target: 'g-rename-mixed-photo', target_port: 'items' },
    { id: 'g-e10', source: 'g-rename-video', source_port: 'items', target: 'g-collect', target_port: 'items_1' },
    { id: 'g-e11', source: 'g-rename-manga', source_port: 'items', target: 'g-collect', target_port: 'items_2' },
    { id: 'g-e12', source: 'g-rename-photo', source_port: 'items', target: 'g-collect', target_port: 'items_3' },
    { id: 'g-e13', source: 'g-rename-other', source_port: 'items', target: 'g-collect', target_port: 'items_4' },
    { id: 'g-e14', source: 'g-rename-mixed-video', source_port: 'items', target: 'g-collect', target_port: 'items_5' },
    { id: 'g-e15', source: 'g-rename-mixed-photo', source_port: 'items', target: 'g-collect', target_port: 'items_6' },
    { id: 'g-e16', source: 'g-collect', source_port: 'items', target: 'g-move', target_port: 'items' },
  ],
})

interface WorkflowTemplate {
  id: string
  name: string
  description: string
  graphJson: string
}

const WORKFLOW_TEMPLATES: WorkflowTemplate[] = [
  {
    id: 'blank',
    name: '空白工作流',
    description: '从零开始，自由搭建节点图',
    graphJson: '{"nodes":[],"edges":[]}',
  },
  {
    id: 'classify-only',
    name: '分类流',
    description: '扫描源目录 → 三路并行分类 → 聚合写入数据库。适合只想给文件夹打类别标签的场景',
    graphJson: TEMPLATE_CLASSIFICATION_ONLY,
  },
  {
    id: 'classify-and-move',
    name: '分类 + 路由移动',
    description: '扫描 → 分类 → 预览结果 → 按类别移动到不同目标目录。完整管道，开箱即用',
    graphJson: TEMPLATE_CLASSIFY_AND_MOVE,
  },
  {
    id: 'generic-processing',
    name: '通用处理流程',
    description: '分类结果读取 → 拆分 → 类别路由（含 mixed 二次分流）→ 重命名 → 合并收集 → 统一移动。适合处理阶段的通用落地流程',
    graphJson: TEMPLATE_GENERIC_PROCESSING,
  },
]

// ─── Types ───────────────────────────────────────────────────────────────────

interface FormState {
  name: string
  graphJson: string
}

const EMPTY_FORM: FormState = { name: '', graphJson: '{"nodes":[],"edges":[]}' }

type ModalMode = { kind: 'create' } | { kind: 'edit'; def: WorkflowDefinition }
type CreateStep = 'pick-template' | 'fill-details'
interface LaunchDialogState {
  open: boolean
  def: WorkflowDefinition | null
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

function buildJobHistoryLink(jobId: string, workflowRunId?: string) {
  const query = new URLSearchParams()
  query.set('job_id', jobId)
  if (workflowRunId && workflowRunId.trim() !== '') {
    query.set('workflow_run_id', workflowRunId)
  }
  return `/job-history?${query.toString()}`
}

export default function WorkflowDefsPage(_props: WorkflowDefsPageProps) {
  const navigate = useNavigate()
  const { defs, isLoading, error, fetchDefs, createDef, updateDef, deleteDef, setActive } =
    useWorkflowDefStore()
  const { bindLatestLaunch, restoreLatestLaunch, buildRunCardView } = useWorkflowRunStore()

  const [modal, setModal] = useState<ModalMode | null>(null)
  const [createStep, setCreateStep] = useState<CreateStep>('pick-template')
  const [form, setForm] = useState<FormState>(EMPTY_FORM)
  const [formError, setFormError] = useState<string | null>(null)
  const [isSaving, setIsSaving] = useState(false)
  const [launchDialog, setLaunchDialog] = useState<LaunchDialogState>({ open: false, def: null })
  const [launchFolderRecords, setLaunchFolderRecords] = useState<Folder[]>([])
  const [launchSearchQuery, setLaunchSearchQuery] = useState('')
  const [selectedFolderId, setSelectedFolderId] = useState('')
  const [launchRecordsLoading, setLaunchRecordsLoading] = useState(false)
  const [launchRecordsError, setLaunchRecordsError] = useState<string | null>(null)
  const [launchError, setLaunchError] = useState<string | null>(null)
  const [launchSuccessJobId, setLaunchSuccessJobId] = useState<string | null>(null)
  const [isLaunching, setIsLaunching] = useState(false)
  const [launchReloadKey, setLaunchReloadKey] = useState(0)

  useEffect(() => {
    void fetchDefs()
  }, [fetchDefs])

  const currentLaunchDef = launchDialog.def
  let enabledPickerCount = 0
  let launchCheckError: string | null = null
  let canDirectLaunch = false

  if (currentLaunchDef) {
    try {
      const checkResult = getWorkflowFolderLaunchability(currentLaunchDef.graph_json)
      enabledPickerCount = checkResult.enabledPickerCount
      canDirectLaunch = checkResult.canLaunch
      launchCheckError = checkResult.error
    } catch (err) {
      launchCheckError = err instanceof Error ? err.message : '工作流图解析失败'
    }
  }

  useEffect(() => {
    if (!launchDialog.open || !currentLaunchDef) return

    let cancelled = false
    setLaunchRecordsLoading(true)
    setLaunchRecordsError(null)

    void listFolders({
      q: launchSearchQuery.trim() || undefined,
      limit: 100,
      page: 1,
      top_level_only: true,
    }).then((res) => {
      if (cancelled) return
      setLaunchFolderRecords(res.data)
    }).catch((err: unknown) => {
      if (cancelled) return
      setLaunchFolderRecords([])
      setLaunchRecordsError(err instanceof Error ? err.message : '文件夹记录加载失败')
    }).finally(() => {
      if (cancelled) return
      setLaunchRecordsLoading(false)
    })

    return () => {
      cancelled = true
    }
  }, [currentLaunchDef, launchDialog.open, launchReloadKey, launchSearchQuery])

  function openCreate() {
    setForm(EMPTY_FORM)
    setFormError(null)
    setCreateStep('pick-template')
    setModal({ kind: 'create' })
  }

  function selectTemplate(tpl: WorkflowTemplate) {
    setForm({ name: tpl.id === 'blank' ? '' : tpl.name, graphJson: tpl.graphJson })
    setCreateStep('fill-details')
  }

  function openEdit(def: WorkflowDefinition) {
    setForm({ name: def.name, graphJson: def.graph_json })
    setFormError(null)
    setModal({ kind: 'edit', def })
  }

  function closeModal() {
    setModal(null)
    setFormError(null)
  }

  function openLaunchDialog(def: WorkflowDefinition) {
    setLaunchDialog({ open: true, def })
    setLaunchSearchQuery('')
    setLaunchError(null)
    setLaunchSuccessJobId(null)
    setLaunchReloadKey((prev) => prev + 1)

    const checkResult = getWorkflowFolderLaunchability(def.graph_json)
    setSelectedFolderId(checkResult.initialSelectedFolderId)
    if (checkResult.initialSelectedFolderId.trim() !== '') {
      void restoreLatestLaunch(def.id, checkResult.initialSelectedFolderId)
    }
  }

  function closeLaunchDialog() {
    setLaunchDialog({ open: false, def: null })
    setLaunchFolderRecords([])
    setLaunchSearchQuery('')
    setSelectedFolderId('')
    setLaunchRecordsLoading(false)
    setLaunchRecordsError(null)
    setLaunchError(null)
    setLaunchSuccessJobId(null)
    setIsLaunching(false)
  }

  async function handleSave() {
    if (!form.name.trim()) {
      setFormError('名称不能为空')
      return
    }
    try {
      JSON.parse(form.graphJson)
    } catch {
      setFormError('Graph JSON 格式不正确')
      return
    }

    setIsSaving(true)
    setFormError(null)
    try {
      if (modal?.kind === 'create') {
        await createDef(form.name.trim(), form.graphJson)
      } else if (modal?.kind === 'edit') {
        await updateDef(modal.def.id, { name: form.name.trim(), graph_json: form.graphJson })
      }
      closeModal()
    } catch (err) {
      setFormError(err instanceof Error ? err.message : '保存失败')
    } finally {
      setIsSaving(false)
    }
  }

  async function handleDelete(def: WorkflowDefinition) {
    if (!window.confirm('确认删除此工作流定义？')) return
    await deleteDef(def.id)
  }

  async function handleSetActive(def: WorkflowDefinition) {
    await setActive(def.id)
  }

  function toggleSelectedFolder(folderId: string) {
    setLaunchError(null)
    setLaunchSuccessJobId(null)
    if (selectedFolderId === folderId) {
      setSelectedFolderId('')
      return
    }
    setSelectedFolderId(folderId)
  }

  useEffect(() => {
    if (!launchDialog.open || !currentLaunchDef) return
    if (selectedFolderId.trim() === '') return
    void restoreLatestLaunch(currentLaunchDef.id, selectedFolderId)
  }, [currentLaunchDef, launchDialog.open, restoreLatestLaunch, selectedFolderId])

  async function handleLaunchWorkflow() {
    if (!currentLaunchDef) return
    if (!canDirectLaunch) {
      setLaunchError('该工作流缺少文件夹选择器节点，无法直接启动')
      return
    }
    if (selectedFolderId.trim() === '') {
      setLaunchError('请选择一条文件夹记录')
      return
    }

    setIsLaunching(true)
    setLaunchError(null)
    setLaunchSuccessJobId(null)
    try {
      const result = await launchWorkflowForFolder({
        workflowDef: currentLaunchDef,
        folderId: selectedFolderId,
        updateWorkflowGraph: async (workflowDefId, graphJson) => {
          await updateWorkflowDef(workflowDefId, { graph_json: graphJson })
        },
        startWorkflow: async (workflowDefId) => {
          const res = await startWorkflowJob({ workflow_def_id: workflowDefId })
          return res.job_id
        },
        bindLatestLaunch,
      })
      await fetchDefs()
      setLaunchSuccessJobId(result.jobId)
    } catch (err) {
      setLaunchError(err instanceof Error ? err.message : '启动失败')
    } finally {
      setIsLaunching(false)
    }
  }

  const launchCardView = currentLaunchDef
    ? buildRunCardView(
      currentLaunchDef.id,
      countEnabledNodes(currentLaunchDef.graph_json),
      selectedFolderId || undefined,
    )
    : null

  return (
    <section className="mx-auto max-w-5xl px-6 py-8">
      <div className="mb-8 flex items-end justify-between border-b-2 border-foreground pb-4">
        <h1 className="text-3xl font-black tracking-tight uppercase">工作流管理</h1>
        <button
          type="button"
          onClick={openCreate}
          className="flex items-center gap-2 border-2 border-foreground bg-primary px-5 py-2.5 text-sm font-bold text-primary-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
        >
          <Plus className="h-4 w-4" />
          新建
        </button>
      </div>

      {isLoading && <p className="text-sm font-bold text-muted-foreground">加载中…</p>}

      {error && <p className="border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900 shadow-hard">{error}</p>}

      {!isLoading && !error && defs.length === 0 && (
        <div className="border-2 border-dashed border-foreground py-20 text-center">
          <p className="text-sm font-bold text-muted-foreground">暂无工作流定义，点击「新建」创建第一个。</p>
        </div>
      )}

      {defs.length > 0 && (
        <div className="overflow-hidden border-2 border-foreground bg-card shadow-hard">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b-2 border-foreground bg-muted/50 text-left">
                <th className="px-5 py-4 font-black tracking-widest">名称</th>
                <th className="px-5 py-4 font-black tracking-widest">版本</th>
                <th className="px-5 py-4 font-black tracking-widest">状态</th>
                <th className="px-5 py-4 font-black tracking-widest">创建时间</th>
                <th className="px-5 py-4 font-black tracking-widest">操作</th>
              </tr>
            </thead>
            <tbody>
              {defs.map((def, idx) => (
                <tr
                  key={def.id}
                  className={cn(
                    'border-b-2 border-foreground last:border-0 transition-colors hover:bg-muted/30',
                    idx % 2 === 0 ? 'bg-background' : 'bg-muted/10',
                  )}
                >
                  <td className="px-5 py-4 font-black">{def.name}</td>
                  <td className="px-5 py-4 font-mono font-bold text-muted-foreground">v{def.version}</td>
                  <td className="px-5 py-4">
                    {def.is_active ? (
                      <span className="inline-flex items-center gap-1.5 border-2 border-foreground bg-green-300 px-3 py-1 text-xs font-black text-green-900">
                        <Check className="h-3 w-3" />
                        已激活
                      </span>
                    ) : (
                      <button
                        type="button"
                        onClick={() => void handleSetActive(def)}
                        className="border-2 border-foreground bg-background px-3 py-1 text-xs font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
                      >
                        设为激活
                      </button>
                    )}
                  </td>
                  <td className="px-5 py-4 font-mono text-xs font-bold text-muted-foreground">
                    {new Date(def.created_at).toLocaleString('zh-CN')}
                  </td>
                  <td className="px-5 py-4">
                    <div className="flex items-center gap-3">
                      <button
                        type="button"
                        onClick={() => openLaunchDialog(def)}
                        className="flex items-center gap-1.5 border-2 border-foreground bg-green-300 px-3 py-1.5 text-xs font-bold text-green-900 transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
                      >
                        <Play className="h-3 w-3" />
                        启动
                      </button>
                      <button
                        type="button"
                        onClick={() => openEdit(def)}
                        className="flex items-center gap-1.5 border-2 border-foreground bg-background px-3 py-1.5 text-xs font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
                      >
                        <Pencil className="h-3 w-3" />
                        编辑
                      </button>
                      <Link
                        to={`/workflow-defs/${def.id}/editor`}
                        className="flex items-center gap-1.5 border-2 border-foreground bg-primary px-3 py-1.5 text-xs font-bold text-primary-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
                      >
                        可视化编辑
                      </Link>
                      <button
                        type="button"
                        onClick={() => void handleDelete(def)}
                        className="flex items-center gap-1.5 border-2 border-red-900 bg-red-100 px-3 py-1.5 text-xs font-bold text-red-900 transition-all hover:bg-red-900 hover:text-red-100 hover:shadow-hard hover:-translate-y-0.5"
                      >
                        <Trash2 className="h-3 w-3" />
                        删除
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {launchDialog.open && currentLaunchDef && typeof document !== 'undefined' && createPortal(
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
          <div className="w-full max-w-2xl border-2 border-foreground bg-background p-6 shadow-hard-lg">
            <h2 className="mb-2 text-xl font-black tracking-tight">启动工作流</h2>
            <p className="mb-4 text-sm font-bold text-muted-foreground">
              当前工作流：{currentLaunchDef.name}
            </p>

            <div className="mb-4 border-2 border-foreground bg-muted/20 p-3">
              <p className="text-xs font-black tracking-wider">节点检测结果</p>
              {launchCheckError ? (
                <p className="mt-2 text-xs font-bold text-red-700">{launchCheckError}</p>
              ) : canDirectLaunch ? (
                <p className="mt-2 text-xs font-bold text-green-700">
                  检测到 {enabledPickerCount} 个启用中的 folder-picker 节点，将统一写入所选单条记录。
                </p>
              ) : (
                <p className="mt-2 text-xs font-bold text-amber-700">
                  该工作流缺少文件夹选择器节点，无法直接启动。
                </p>
              )}
            </div>

            {canDirectLaunch && !launchCheckError && (
              <div className="space-y-3">
                <div>
                  <label className="mb-2 block text-sm font-black tracking-widest">搜索文件夹记录</label>
                  <input
                    type="text"
                    value={launchSearchQuery}
                    onChange={(e) => {
                      setLaunchSearchQuery(e.target.value)
                      setLaunchError(null)
                    }}
                    placeholder="输入名称或路径关键词"
                    className="w-full border-2 border-foreground bg-background px-4 py-3 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
                  />
                </div>

                <div className="max-h-72 space-y-1 overflow-auto border-2 border-foreground bg-muted/20 p-2">
                  {launchRecordsLoading ? (
                    <p className="py-8 text-center text-xs font-bold text-muted-foreground">记录加载中...</p>
                  ) : launchRecordsError ? (
                    <div className="py-4 text-center">
                      <p className="text-xs font-bold text-red-700">{launchRecordsError}</p>
                      <button
                        type="button"
                        onClick={() => setLaunchReloadKey((prev) => prev + 1)}
                        className="mt-3 border-2 border-foreground bg-background px-3 py-1.5 text-xs font-bold transition-all hover:bg-foreground hover:text-background"
                      >
                        重试
                      </button>
                    </div>
                  ) : launchFolderRecords.length === 0 ? (
                    <p className="py-8 text-center text-xs font-bold text-muted-foreground">暂无可选记录</p>
                  ) : (
                    launchFolderRecords.map((folder) => (
                      <label
                        key={folder.id}
                        className="flex cursor-pointer items-start gap-2 border-2 border-foreground bg-background px-2 py-2"
                      >
                        <input
                          type="radio"
                          name="workflow-launch-folder-record"
                          checked={selectedFolderId === folder.id}
                          onChange={() => toggleSelectedFolder(folder.id)}
                          className="mt-0.5 h-4 w-4 rounded-none border-2 border-foreground text-foreground focus:ring-foreground focus:ring-offset-0"
                        />
                        <span className="min-w-0">
                          <span className="block truncate text-xs font-black">{folder.name}</span>
                          <span className="block truncate font-mono text-[10px] text-muted-foreground">{folder.path}</span>
                        </span>
                      </label>
                    ))
                  )}
                </div>

                <p className="text-xs font-bold text-muted-foreground">
                  已选记录：{selectedFolderId ? '1 条' : '0 条'}
                </p>
              </div>
            )}

            {launchError && (
              <p className="mt-4 border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900 shadow-hard">
                {launchError}
              </p>
            )}

            {launchSuccessJobId && (
              <div className="mt-4 space-y-3">
                <div className="border-2 border-green-900 bg-green-100 px-4 py-3 text-sm font-bold text-green-900 shadow-hard">
                  启动成功，作业 ID：{launchSuccessJobId}
                </div>
              </div>
            )}

            {launchCardView && (
              <div className="mt-4">
                <WorkflowRunStatusCard
                  view={launchCardView}
                  title="当前运行卡片"
                  onOpenJobs={() => navigate(buildJobHistoryLink(launchCardView.jobId, launchCardView.workflowRunId))}
                />
              </div>
            )}

            <div className="mt-8 flex items-center justify-between gap-3">
              <button
                type="button"
                onClick={closeLaunchDialog}
                disabled={isLaunching}
                className="border-2 border-foreground bg-background px-6 py-2.5 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-background disabled:hover:text-foreground disabled:hover:shadow-none disabled:hover:translate-y-0"
              >
                关闭
              </button>

              <div className="flex items-center gap-3">
                {launchSuccessJobId && (
                  <button
                    type="button"
                    onClick={() => navigate(buildJobHistoryLink(launchSuccessJobId, launchCardView?.workflowRunId))}
                    className="border-2 border-foreground bg-primary px-4 py-2.5 text-sm font-bold text-primary-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
                  >
                    前往作业页
                  </button>
                )}
                <button
                  type="button"
                  onClick={() => void handleLaunchWorkflow()}
                  disabled={!canDirectLaunch || !!launchCheckError || selectedFolderId.trim() === '' || isLaunching}
                  className="inline-flex items-center gap-2 border-2 border-foreground bg-green-300 px-6 py-2.5 text-sm font-bold text-green-900 transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:bg-green-300 disabled:hover:text-green-900 disabled:hover:shadow-none disabled:hover:translate-y-0"
                >
                  {isLaunching && <Loader2 className="h-4 w-4 animate-spin" />}
                  {isLaunching ? '启动中…' : '确认启动'}
                </button>
              </div>
            </div>
          </div>
        </div>,
        document.body,
      )}

      {modal !== null && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
          <div className={cn(
            'w-full border-2 border-foreground bg-background p-6 shadow-hard-lg',
            modal.kind === 'create' && createStep === 'pick-template' ? 'max-w-2xl' : 'max-w-lg',
          )}>
            <h2 className="mb-6 text-xl font-black tracking-tight">
              {modal.kind === 'edit' ? '编辑工作流定义' : createStep === 'pick-template' ? '选择模板' : '新建工作流定义'}
            </h2>

            {modal.kind === 'create' && createStep === 'pick-template' && (
              <div className="grid grid-cols-1 gap-3">
                {WORKFLOW_TEMPLATES.map((tpl) => (
                  <button
                    key={tpl.id}
                    type="button"
                    onClick={() => selectTemplate(tpl)}
                    className="border-2 border-foreground bg-background p-4 text-left transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5"
                  >
                    <p className="font-black tracking-wide">{tpl.name}</p>
                    <p className="mt-1 text-xs font-bold text-muted-foreground group-hover:text-background">{tpl.description}</p>
                  </button>
                ))}
              </div>
            )}

            {(modal.kind === 'edit' || createStep === 'fill-details') && (
              <div className="space-y-5">
                <div>
                  <label className="mb-2 block text-sm font-black tracking-widest">名称</label>
                  <input
                    type="text"
                    value={form.name}
                    onChange={(e) => setForm((prev) => ({ ...prev, name: e.target.value }))}
                    placeholder="工作流名称"
                    className="w-full border-2 border-foreground bg-background px-4 py-3 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
                  />
                </div>

                <div>
                  <label className="mb-2 block text-sm font-black tracking-widest">GRAPH JSON</label>
                  <textarea
                    value={form.graphJson}
                    onChange={(e) => setForm((prev) => ({ ...prev, graphJson: e.target.value }))}
                    rows={10}
                    spellCheck={false}
                    className="w-full border-2 border-foreground bg-muted/30 px-4 py-3 font-mono text-xs font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
                  />
                </div>

                {formError && <p className="border-2 border-red-900 bg-red-100 px-4 py-3 text-sm font-bold text-red-900 shadow-hard">{formError}</p>}
              </div>
            )}

            <div className="mt-8 flex justify-between gap-3">
              <div>
                {modal.kind === 'create' && createStep === 'fill-details' && (
                  <button
                    type="button"
                    onClick={() => setCreateStep('pick-template')}
                    disabled={isSaving}
                    className="border-2 border-foreground bg-background px-4 py-2.5 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50"
                  >
                    ← 重选模板
                  </button>
                )}
              </div>
              <div className="flex gap-3">
                <button
                  type="button"
                  onClick={closeModal}
                  disabled={isSaving}
                  className="border-2 border-foreground bg-background px-6 py-2.5 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-background disabled:hover:text-foreground disabled:hover:shadow-none disabled:hover:translate-y-0"
                >
                  取消
                </button>
                {(modal.kind === 'edit' || createStep === 'fill-details') && (
                  <button
                    type="button"
                    onClick={() => void handleSave()}
                    disabled={isSaving}
                    className="border-2 border-foreground bg-primary px-6 py-2.5 text-sm font-bold text-primary-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-primary disabled:hover:text-primary-foreground disabled:hover:shadow-none disabled:hover:translate-y-0"
                  >
                    {isSaving ? '保存中…' : '保存'}
                  </button>
                )}
              </div>
            </div>
          </div>
        </div>
      )}
    </section>
  )
}
