import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode, type ReactElement } from 'react'
import {
  addEdge,
  Background,
  BackgroundVariant,
  Controls,
  getBezierPath,
  MiniMap,
  PanOnScrollMode,
  ReactFlow,
  ReactFlowProvider,
  Handle,
  Position,
  SelectionMode,
  useEdgesState,
  useStore,
  useUpdateNodeInternals,
  useNodesState,
  type IsValidConnection,
  type Connection,
  type ConnectionLineComponentProps,
  type Edge,
  type InternalNode,
  type Node,
  type NodeProps,
  type OnConnect,
  type OnConnectEnd,
  type OnConnectStart,
  type OnReconnect,
} from '@xyflow/react'
import '@xyflow/react/dist/style.css'
import { ArrowLeft, CheckCircle2, ChevronDown, ChevronLeft, ChevronRight, ChevronUp, Loader2, MousePointer, Play, Plus, RotateCcw, Save, Trash2, TriangleAlert, Wand2 } from 'lucide-react'
import { useNavigate, useParams } from 'react-router-dom'
import gsap from 'gsap'

import { ApiRequestError } from '@/api/client'
import { ConfiguredPathField } from '@/components/ConfiguredPathField'
import { listFolders } from '@/api/folders'
import { Modal } from '@/components/Modal'
import { WorkflowRunStatusCard } from '@/components/WorkflowRunStatusCard'
import { startWorkflowJob } from '@/api/workflowRuns'
import type { Folder, NodeRun, NodeRunStatus } from '@/types'
import { listNodeTypes } from '@/api/nodeTypes'
import { getWorkflowDef, updateWorkflowDef } from '@/api/workflowDefs'
import { cn } from '@/lib/utils'
import { useThemeStore } from '@/store/themeStore'
import { useWorkflowRunStore } from '@/store/workflowRunStore'
import { ClassificationPreviewInline } from '@/components/workflow-preview/ClassificationPreviewInline'
import { ProcessingPreviewInline } from '@/components/workflow-preview/ProcessingPreviewInline'
import { isClassificationSummary, isProcessingSummary, parseNodePreviewSummary } from '@/components/workflow-preview/previewUtils'
import type {
  NodeInputSpec,
  NodeSchema,
  WorkflowDefinition,
  WorkflowGraph,
  WorkflowGraphEdge,
  WorkflowGraphNode,
} from '@/types'

// ─── Types ────────────────────────────────────────────────────────────────────

type EditorNodeData = {
  label: string
  type: string
  enabled: boolean
  schema: NodeSchema | undefined
}

type EditorNode = Node<EditorNodeData>

interface EditorContextValue {
  workflowNodes: Record<string, WorkflowGraphNode>
  schemas: NodeSchema[]
  updateNode: (nodeId: string, patch: Partial<WorkflowGraphNode> & { schemaType?: string }) => void
  updateNodeConfig: (nodeId: string, key: string, rawValue: string) => void
  removeNodeConfig: (nodeId: string, key: string) => void
  addNodeConfig: (nodeId: string, key: string, rawValue: string) => void
  deleteNode: (nodeId: string) => void
  nodeRunByNodeId: Record<string, NodeRun>
  rollbackingRunId: string | null
  onRollbackRun: (runId: string) => Promise<void>
  onViewNodeError: (nodeId: string) => void
}

const EditorContext = createContext<EditorContextValue | null>(null)

function useEditorContext(): EditorContextValue {
  const ctx = useContext(EditorContext)
  if (!ctx) throw new Error('useEditorContext must be used within EditorContext.Provider')
  return ctx
}

// ─── Constants ────────────────────────────────────────────────────────────────

const INITIAL_GRAPH: WorkflowGraph = { nodes: [], edges: [] }
const CONFIG_AUTO_SAVE_DEBOUNCE_MS = 700
const NODE_INSERT_FALLBACK_START = { x: 160, y: 140 }
const NODE_INSERT_FALLBACK_COLUMN_GAP = 280
const NODE_INSERT_FALLBACK_ROW_GAP = 180
const NODE_INSERT_ESTIMATED_SIZE = { width: 320, height: 180 }
const NODE_INSERT_STAGGER_OFFSETS = [
  { x: 0, y: 0 },
  { x: 28, y: 28 },
  { x: -28, y: 28 },
  { x: 28, y: -28 },
  { x: -28, y: -28 },
]

interface FlowPositionProjector {
  screenToFlowPosition: (position: { x: number; y: number }) => { x: number; y: number }
}

function buildJobHistoryLink(jobId: string, workflowRunId?: string) {
  const query = new URLSearchParams()
  query.set('job_id', jobId)
  if (workflowRunId && workflowRunId.trim() !== '') {
    query.set('workflow_run_id', workflowRunId)
  }
  return `/job-history?${query.toString()}`
}

/**
 * 节点分类定义：按业务语义划分颜色主题，保证同类节点颜色一致。
 *
 * 颜色语义：
 *   紫色 → 触发器（工作流入口）
 *   蓝色 → 扫描 & 读取（数据来源）
 *   青色 → 分类器（判断决策）
 *   琥珀 → 逻辑控制（流程分支）
 *   绿色 → 执行操作（文件变更）
 */
interface NodeCategory {
  label: string
  iconColor: string
  accentClass: string
  borderHoverClass: string
  types: ReadonlySet<string>
}

const NODE_CATEGORIES: NodeCategory[] = [
  {
    label: '触发器',
    iconColor: 'text-violet-600 dark:text-violet-400',
    accentClass: 'from-violet-500/20 to-purple-500/10 border-violet-200 dark:from-violet-500/25 dark:to-purple-500/15 dark:border-violet-700',
    borderHoverClass: 'hover:border-violet-300 dark:hover:border-violet-500',
    types: new Set(['trigger']),
  },
  {
    label: '扫描 & 读取',
    iconColor: 'text-blue-600 dark:text-blue-400',
    accentClass: 'from-blue-500/20 to-indigo-500/10 border-blue-200 dark:from-blue-500/25 dark:to-indigo-500/15 dark:border-blue-700',
    borderHoverClass: 'hover:border-blue-300 dark:hover:border-blue-500',
    types: new Set([
      'folder-tree-scanner',
      'folder-picker',
      'classification-reader',
      'db-subtree-reader',
      'classification-db-result-preview',
      'processing-result-preview',
    ]),
  },
  {
    label: '分类器',
    iconColor: 'text-cyan-600 dark:text-cyan-400',
    accentClass: 'from-cyan-500/20 to-teal-500/10 border-cyan-200 dark:from-cyan-500/25 dark:to-teal-500/15 dark:border-cyan-700',
    borderHoverClass: 'hover:border-cyan-300 dark:hover:border-cyan-500',
    types: new Set(['ext-ratio-classifier', 'name-keyword-classifier', 'file-tree-classifier']),
  },
  {
    label: '逻辑控制',
    iconColor: 'text-amber-600 dark:text-amber-400',
    accentClass: 'from-amber-500/20 to-orange-500/10 border-amber-200 dark:from-amber-500/25 dark:to-orange-500/15 dark:border-amber-700',
    borderHoverClass: 'hover:border-amber-300 dark:hover:border-amber-500',
    types: new Set(['confidence-check', 'folder-splitter', 'category-router', 'mixed-leaf-router', 'subtree-aggregator']),
  },
  {
    label: '执行操作',
    iconColor: 'text-emerald-600 dark:text-emerald-400',
    accentClass: 'from-emerald-500/20 to-green-500/10 border-emerald-200 dark:from-emerald-500/25 dark:to-green-500/15 dark:border-emerald-700',
    borderHoverClass: 'hover:border-emerald-300 dark:hover:border-emerald-500',
    types: new Set(['move-node', 'rename-node', 'compress-node', 'thumbnail-node']),
  },
]

const FALLBACK_CATEGORY: NodeCategory = {
  label: '其他',
  iconColor: 'text-gray-500 dark:text-gray-400',
  accentClass: 'from-gray-400/20 to-gray-300/10 border-gray-200 dark:from-gray-500/20 dark:to-gray-400/10 dark:border-gray-600',
  borderHoverClass: 'hover:border-gray-300 dark:hover:border-gray-500',
  types: new Set(),
}

function getNodeCategory(nodeType: string): NodeCategory {
  return NODE_CATEGORIES.find((category) => category.types.has(nodeType)) ?? FALLBACK_CATEGORY
}

// ─── Utility functions ────────────────────────────────────────────────────────

function safeParseGraph(graphJson: string): WorkflowGraph {
  try {
    const parsed = JSON.parse(graphJson) as Partial<WorkflowGraph>
    return {
      nodes: Array.isArray(parsed.nodes) ? parsed.nodes : [],
      edges: Array.isArray(parsed.edges) ? parsed.edges : [],
    }
  } catch {
    return INITIAL_GRAPH
  }
}

function buildNodeId(nodeType: string, existing: Node<EditorNodeData>[]) {
  const normalized = nodeType.replace(/[^a-z0-9]+/gi, '-').replace(/(^-|-$)/g, '').toLowerCase() || 'node'
  let index = existing.length + 1
  let candidate = `${normalized}-${index}`
  const ids = new Set(existing.map((node) => node.id))
  for (; ids.has(candidate); index += 1) {
    candidate = `${normalized}-${index}`
  }
  return candidate
}

function graphToNodes(graph: WorkflowGraph, schemas: Map<string, NodeSchema>): EditorNode[] {
  return graph.nodes.map((node, index) => ({
    id: node.id,
    type: 'workflowNode',
    position: node.ui_position ?? { x: 120 + ((index % 4) * 240), y: 120 + (Math.floor(index / 4) * 160) },
    data: {
      label: node.label || schemas.get(node.type)?.label || node.type,
      type: node.type,
      enabled: node.enabled,
      schema: schemas.get(node.type),
    },
  }))
}

function graphToEdges(graph: WorkflowGraph, schemaMap: Map<string, NodeSchema>): Edge[] {
  const seenIds = new Set<string>()
  const seenTargetPorts = new Set<string>()
  const result: Edge[] = []
  const nodeTypeById = new Map(graph.nodes.map((n) => [n.id, n.type]))

  for (let index = 0; index < graph.edges.length; index++) {
    const edge = graph.edges[index]
    const id = edge.id || `edge-${index}`
    const targetPortKey = `${edge.target}::${edge.target_port}`

    if (seenIds.has(id) || seenTargetPorts.has(targetPortKey)) continue

    seenIds.add(id)
    seenTargetPorts.add(targetPortKey)

    const sourceSchema = schemaMap.get(nodeTypeById.get(edge.source) ?? '')
    const targetSchema = schemaMap.get(nodeTypeById.get(edge.target) ?? '')
    if (!sourceSchema || !targetSchema) continue

    const sourcePortIndex = typeof edge.source_port === 'number'
      ? edge.source_port
      : sourceSchema.output_ports.findIndex((p) => p.name === edge.source_port)
    if (sourcePortIndex < 0 || sourcePortIndex >= sourceSchema.output_ports.length) continue

    const targetPortIndex = typeof edge.target_port === 'number'
      ? edge.target_port
      : targetSchema.input_ports.findIndex((p) => p.name === edge.target_port)
    if (targetPortIndex < 0 || targetPortIndex >= targetSchema.input_ports.length) continue

    const sourceHandle = `out-${sourcePortIndex}`
    const targetHandle = `in-${targetPortIndex}`

    result.push({
      id,
      source: edge.source,
      target: edge.target,
      sourceHandle,
      targetHandle,
      animated: false,
    })
  }

  return result
}

function buildInputMap(
  nodeId: string,
  edges: Edge[],
  schema?: NodeSchema,
  previous?: Record<string, NodeInputSpec>,
  getSourceSchema?: (sourceNodeId: string) => NodeSchema | undefined,
) {
  const nextInputs: Record<string, NodeInputSpec> = {}

  if (previous) {
    for (const [key, value] of Object.entries(previous)) {
      if (value.const_value !== undefined) {
        nextInputs[key] = { const_value: value.const_value }
      }
    }
  }

  for (const edge of edges) {
    if (edge.target !== nodeId) continue
    const targetPortIndex = parseHandleIndex(edge.targetHandle)
    const sourcePortIndex = parseHandleIndex(edge.sourceHandle)
    if (targetPortIndex == null || sourcePortIndex == null) continue
    const portName = schema?.input_ports?.[targetPortIndex]?.name ?? `input_${targetPortIndex}`
    const sourcePortName = getSourceSchema?.(edge.source)?.output_ports?.[sourcePortIndex]?.name
    nextInputs[portName] = {
      link_source: sourcePortName != null
        ? { source_node_id: edge.source, source_port: sourcePortName }
        : { source_node_id: edge.source, output_port_index: sourcePortIndex },
    }
  }

  return nextInputs
}

function parseHandleIndex(handle?: string | null) {
  if (!handle) return null
  const raw = handle.split('-')[1]
  const parsed = Number.parseInt(raw, 10)
  return Number.isNaN(parsed) ? null : parsed
}

function getHandleFromPointerEvent(event: MouseEvent | TouchEvent) {
  const point = 'changedTouches' in event
    ? event.changedTouches[0]
    : event
  if (!point) return null

  const element = document.elementFromPoint(point.clientX, point.clientY)
  const handleElement = element?.closest('.react-flow__handle')
  if (!(handleElement instanceof HTMLElement)) return null

  const nodeId = handleElement.getAttribute('data-nodeid')
  const handleType = handleElement.classList.contains('source')
    ? 'source'
    : handleElement.classList.contains('target')
      ? 'target'
      : null

  if (!nodeId || !handleType) return null

  return {
    nodeId,
    handleId: handleElement.getAttribute('data-handleid'),
    handleType,
  }
}

function getHandleAnchor(
  nodeLookup: Map<string, InternalNode<Node>>,
  nodeId: string,
  handleType: 'source' | 'target',
  handleId?: string | null,
) {
  const node = nodeLookup.get(nodeId)
  const handles = node?.internals.handleBounds?.[handleType]
  const handle = handles?.find((item) => item.id === handleId) ?? handles?.[0]
  if (!node || !handle) return null

  const absoluteX = handle.x + node.internals.positionAbsolute.x
  const absoluteY = handle.y + node.internals.positionAbsolute.y

  if (handle.position === Position.Right) {
    return { x: absoluteX + handle.width, y: absoluteY + (handle.height / 2), position: handle.position }
  }
  if (handle.position === Position.Left) {
    return { x: absoluteX, y: absoluteY + (handle.height / 2), position: handle.position }
  }
  if (handle.position === Position.Bottom) {
    return { x: absoluteX + (handle.width / 2), y: absoluteY + handle.height, position: handle.position }
  }
  return { x: absoluteX + (handle.width / 2), y: absoluteY, position: handle.position }
}

function WorkflowReconnectLine(
  props: ConnectionLineComponentProps<EditorNode> & { reconnectingEdge: Edge | null },
) {
  const nodeLookup = useStore((state) => state.nodeLookup)
  const { reconnectingEdge } = props

  let fromX = props.fromX
  let fromY = props.fromY
  let fromPosition = props.fromPosition

  if (reconnectingEdge) {
    const fixedAnchor = props.fromHandle.type === 'target'
      ? getHandleAnchor(nodeLookup, reconnectingEdge.source, 'source', reconnectingEdge.sourceHandle)
      : getHandleAnchor(nodeLookup, reconnectingEdge.target, 'target', reconnectingEdge.targetHandle)

    if (fixedAnchor) {
      fromX = fixedAnchor.x
      fromY = fixedAnchor.y
      fromPosition = fixedAnchor.position
    }
  }

  const [path] = getBezierPath({
    sourceX: fromX,
    sourceY: fromY,
    sourcePosition: fromPosition,
    targetX: props.toX,
    targetY: props.toY,
    targetPosition: props.toPosition,
  })

  return (
    <path
      d={path}
      fill="none"
      stroke={props.connectionLineStyle?.stroke ?? 'hsl(var(--primary))'}
      strokeWidth={props.connectionLineStyle?.strokeWidth ?? 3}
      strokeLinecap="round"
      className={cn(
        'transition-opacity',
        props.connectionStatus === 'invalid' ? 'opacity-60' : 'opacity-100',
      )}
    />
  )
}

function isValidEditorEdge(
  edge: Edge,
  getSchema: (nodeId: string) => NodeSchema | undefined,
) {
  if (!edge.source || !edge.target || edge.source === edge.target) return false
  const sourcePortIndex = parseHandleIndex(edge.sourceHandle)
  const targetPortIndex = parseHandleIndex(edge.targetHandle)
  if (sourcePortIndex == null || targetPortIndex == null) return false

  const sourceSchema = getSchema(edge.source)
  const targetSchema = getSchema(edge.target)
  if (!sourceSchema || !targetSchema) return false

  const sourcePort = sourceSchema.output_ports[sourcePortIndex]
  const targetPort = targetSchema.input_ports[targetPortIndex]
  if (!sourcePort || !targetPort) return false

  if (sourcePort.type && targetPort.type && sourcePort.type !== targetPort.type) return false

  return true
}

function isValidEditorConnection(
  connection: Connection | Edge,
  getSchema: (nodeId: string) => NodeSchema | undefined,
) {
  if (!connection.source || !connection.target) return false
  return isValidEditorEdge({
    id: '__connection__',
    source: connection.source,
    target: connection.target,
    sourceHandle: connection.sourceHandle ?? null,
    targetHandle: connection.targetHandle ?? null,
  }, getSchema)
}

function nodesToGraph(
  rfNodes: EditorNode[],
  rfEdges: Edge[],
  workflowNodes: Record<string, WorkflowGraphNode>,
  schemaMap: Map<string, NodeSchema>,
): WorkflowGraph {
  const nodeTypeMap = new Map(rfNodes.map((n) => [n.id, n.data.type]))
  const getSchema = (nodeId: string) => schemaMap.get(nodeTypeMap.get(nodeId) ?? '')
  const sanitizedEdges = rfEdges.filter((edge) => isValidEditorEdge(edge, getSchema))

  const nodes: WorkflowGraphNode[] = rfNodes.map((node) => {
    const previous = workflowNodes[node.id]
    const nextType = node.data.type
    const schema = schemaMap.get(nextType)
    return {
      id: node.id,
      type: nextType,
      label: node.data.label,
      config: previous?.config ?? {},
      inputs: buildInputMap(node.id, sanitizedEdges, schema, previous?.inputs, getSchema),
      ui_position: { x: Math.round(node.position.x), y: Math.round(node.position.y) },
      enabled: node.data.enabled,
    }
  })

  const edges: WorkflowGraphEdge[] = sanitizedEdges.map((edge) => {
    const sourcePortIndex = parseHandleIndex(edge.sourceHandle) ?? 0
    const targetPortIndex = parseHandleIndex(edge.targetHandle) ?? 0
    const sourcePortName = getSchema(edge.source)?.output_ports[sourcePortIndex]?.name
    const targetPortName = getSchema(edge.target)?.input_ports[targetPortIndex]?.name
    return {
      id: edge.id,
      source: edge.source,
      source_port: sourcePortName ?? '',
      target: edge.target,
      target_port: targetPortName ?? '',
    }
  })

  return { nodes, edges }
}

function parseConfigValue(input: string): unknown {
  const trimmed = input.trim()
  if (trimmed === 'true') return true
  if (trimmed === 'false') return false
  if (trimmed !== '' && /^-?\d+(\.\d+)?$/.test(trimmed)) return Number(trimmed)
  if ((trimmed.startsWith('{') && trimmed.endsWith('}')) || (trimmed.startsWith('[') && trimmed.endsWith(']'))) {
    try {
      return JSON.parse(trimmed) as unknown
    } catch {
      return input
    }
  }
  return input
}

function sendAgentDebugLog(payload: {
  runId: string
  hypothesisId: string
  location: string
  message: string
  data: Record<string, unknown>
}) {
  fetch('http://127.0.0.1:7712/ingest/5390b56f-af7c-4d76-be18-5b2daa414d69', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'X-Debug-Session-Id': '2da064' },
    body: JSON.stringify({
      sessionId: '2da064',
      runId: payload.runId,
      hypothesisId: payload.hypothesisId,
      location: payload.location,
      message: payload.message,
      data: payload.data,
      timestamp: Date.now(),
    }),
  }).catch(() => {})
}

// ─── Rename preview logic ─────────────────────────────────────────────────────

type RenameConditionalRule = { condition: string; template: string }
type RenamePreviewResult = { strategy: string; targetName: string; warning: string | null }

const RENAME_PREVIEW_SAMPLE = { name: 'Dune[2021]', category: 'video', parent: '电影合集', index: 1 }

function renamePreviewString(value: unknown) {
  return typeof value === 'string' ? value.trim() : ''
}

function renamePreviewBool(value: unknown, fallback = false) {
  return typeof value === 'boolean' ? value : fallback
}

function renamePreviewExtractYear(name: string) {
  const matched = name.match(/(19|20)\d{2}/)
  return matched?.[0]?.trim() ?? ''
}

function renamePreviewExtractTitle(name: string, year: string) {
  let title = name.trim()
  if (year !== '') {
    title = title.replaceAll(`(${year})`, '').replaceAll(`[${year}]`, '').replaceAll(year, '')
  }
  title = renamePreviewTrimDecorators(title.trim())
  return title === '' ? name.trim() : title
}

function renamePreviewTrimDecorators(input: string) {
  const trimChars = '-_()[] '
  let start = 0
  let end = input.length
  for (; start < end; start += 1) {
    if (!trimChars.includes(input[start])) break
  }
  for (; end > start; end -= 1) {
    if (!trimChars.includes(input[end - 1])) break
  }
  return input.slice(start, end)
}

function renamePreviewRenderTemplate(template: string, variables: Record<string, string>) {
  let result = template
  for (const [key, value] of Object.entries(variables)) {
    result = result.replaceAll(`{${key}}`, value)
  }
  return result.trim()
}

function renamePreviewParseRules(config: Record<string, unknown>) {
  const raw = config.rules
  if (!Array.isArray(raw)) return [] as RenameConditionalRule[]
  const rules: RenameConditionalRule[] = []
  for (const item of raw) {
    if (!item || typeof item !== 'object') continue
    const itemMap = item as Record<string, unknown>
    const condition = renamePreviewString(itemMap.condition) || renamePreviewString(itemMap.if)
    const template = renamePreviewString(itemMap.template)
    if (template === '') continue
    rules.push({ condition, template })
  }
  return rules
}

function renamePreviewEvaluateCondition(condition: string, name: string, category: string) {
  const trimmed = condition.trim()
  if (trimmed === '') return false
  const containsMatch = trimmed.match(/^name\s+CONTAINS\s+"([^"]+)"$/i)
  if (containsMatch) return name.toLowerCase().includes(containsMatch[1].toLowerCase())
  const matchesMatch = trimmed.match(/^name\s+MATCHES\s+"([^"]+)"$/i)
  if (matchesMatch) {
    try { return new RegExp(matchesMatch[1]).test(name) } catch { return false }
  }
  const categoryMatch = trimmed.match(/^category\s*==\s*"([^"]+)"$/i)
  if (categoryMatch) return category.trim().toLowerCase() === categoryMatch[1].trim().toLowerCase()
  return false
}

function buildRenamePreview(config: Record<string, unknown>): RenamePreviewResult {
  const strategy = renamePreviewString(config.strategy).toLowerCase() || 'template'
  const template = renamePreviewString(config.template)
  const regexPattern = renamePreviewString(config.regex) || renamePreviewString(config.pattern)
  const skipIfSame = renamePreviewBool(config.skip_if_same, false)
  const currentName = RENAME_PREVIEW_SAMPLE.name
  const year = renamePreviewExtractYear(currentName)
  const variables: Record<string, string> = {
    name: currentName,
    title: renamePreviewExtractTitle(currentName, year),
    category: RENAME_PREVIEW_SAMPLE.category,
    year,
    index: String(RENAME_PREVIEW_SAMPLE.index),
    parent: RENAME_PREVIEW_SAMPLE.parent,
  }

  let candidate = currentName
  let warning: string | null = null

  if (strategy === 'template') {
    if (template !== '') candidate = renamePreviewRenderTemplate(template, variables)
  } else if (strategy === 'regex_extract') {
    if (regexPattern !== '') {
      try {
        const regex = new RegExp(regexPattern)
        const matches = regex.exec(currentName)
        if (matches && matches.length > 0) {
          const groups = matches.groups ? Object.entries(matches.groups) : []
          for (const [key, value] of groups) {
            if (typeof value === 'string') variables[key] = value
          }
          if (template !== '') {
            candidate = renamePreviewRenderTemplate(template, variables)
          } else if (typeof matches.groups?.title === 'string' && matches.groups.title.trim() !== '') {
            candidate = matches.groups.title.trim()
          } else {
            const firstNamed = groups.find(([, value]) => typeof value === 'string' && value.trim() !== '')
            if (firstNamed && typeof firstNamed[1] === 'string') {
              candidate = firstNamed[1].trim()
            } else if (matches.length > 1 && typeof matches[1] === 'string' && matches[1].trim() !== '') {
              candidate = matches[1].trim()
            }
          }
        }
      } catch {
        warning = '正则表达式无效，预览已回退到原名称。'
      }
    }
  } else if (strategy === 'conditional') {
    const rules = renamePreviewParseRules(config)
    let defaultTemplate = ''
    for (const rule of rules) {
      if (rule.condition.trim().toUpperCase() === 'DEFAULT') {
        defaultTemplate = rule.template
        continue
      }
      if (renamePreviewEvaluateCondition(rule.condition, currentName, RENAME_PREVIEW_SAMPLE.category)) {
        candidate = renamePreviewRenderTemplate(rule.template, variables)
        break
      }
    }
    if (candidate === currentName && defaultTemplate !== '') {
      candidate = renamePreviewRenderTemplate(defaultTemplate, variables)
    }
  }

  if (candidate.trim() === '') candidate = currentName
  if (skipIfSame && candidate === currentName) {
    warning = warning ?? '当前命名结果与原名称一致，实际执行时会保持原名。'
  }

  return { strategy, targetName: candidate, warning }
}

// ─── Node config panels ─────────────────────────────────────────────────────

interface NodeConfigPanelProps {
  nodeId: string
  nodeType: string
  config: Record<string, unknown>
  updateNodeConfig: (nodeId: string, key: string, rawValue: string) => void
}

function cfgStr(config: Record<string, unknown>, key: string): string {
  const v = config[key]
  return typeof v === 'string' ? v : ''
}

function cfgNum(config: Record<string, unknown>, key: string, fallback: number): number {
  const v = config[key]
  return typeof v === 'number' ? v : fallback
}

function cfgJson(config: Record<string, unknown>, key: string): string {
  const v = config[key]
  if (v === undefined || v === null) return ''
  if (typeof v === 'string') return v
  return JSON.stringify(v, null, 2)
}

function cfgPathRefType(config: Record<string, unknown>, defaultType: 'output' | 'custom'): 'output' | 'custom' {
  const value = config['path_ref_type']
  if (value === 'output' || value === 'custom') return value
  return defaultType
}

function cfgPathRefKey(config: Record<string, unknown>, fallback: string): string {
  const value = config['path_ref_key']
  if (typeof value === 'string' && value.trim() !== '') return value
  return fallback
}

function cfgPathSuffix(config: Record<string, unknown>): string {
  const value = config['path_suffix']
  return typeof value === 'string' ? value : ''
}

const FIELD_CLS =
  'w-full border-2 border-foreground bg-background px-3 py-2 text-sm font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-1'
const TEXTAREA_FIELD_CLS =
  'w-full border-2 border-foreground bg-background px-3 py-2 font-mono text-xs font-bold outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-1'

interface ConfigFieldProps {
  label: string
  hint?: string
  children: ReactNode
}

function ConfigField({ label, hint, children }: ConfigFieldProps) {
  return (
    <div>
      <label className="mb-1 block text-[10px] font-black uppercase tracking-widest text-foreground">
        {label}
      </label>
      {hint && <p className="mb-2 text-[10px] font-bold text-muted-foreground">{hint}</p>}
      {children}
    </div>
  )
}

function NodeUsageHint({ children }: { children: ReactNode }) {
  return (
    <div className="border-2 border-dashed border-foreground bg-muted/30 p-3">
      <p className="text-xs font-bold leading-relaxed text-muted-foreground">{children}</p>
    </div>
  )
}

function NodeConfigPanel({ nodeId, nodeType, config, updateNodeConfig }: NodeConfigPanelProps) {
  const set = (key: string, val: string) => updateNodeConfig(nodeId, key, val)
  const setPathRef = (next: { pathRefType: 'output' | 'custom'; pathRefKey: string; pathSuffix: string }) => {
    set('path_ref_type', next.pathRefType)
    set('path_ref_key', next.pathRefKey)
    set('path_suffix', next.pathSuffix)
    set('target_dir', '')
    set('targetDir', '')
    set('output_dir', '')
    set('target_dir_source', '')
    set('output_dir_source', '')
    set('target_dir_option_id', '')
    set('output_dir_option_id', '')
  }
  const strategy = cfgStr(config, 'strategy') || 'simple'

  switch (nodeType) {
    case 'trigger':
      return (
        <NodeUsageHint>
          工作流入口节点，自动触发并将当前文件夹传递给下游。无需配置。
        </NodeUsageHint>
      )

    case 'ext-ratio-classifier':
      return (
        <NodeUsageHint>
          通过文件扩展名比例自动判断类别（photo / video / manga / other）。无需配置，输出分类信号到 Confidence Check 或 Subtree Aggregator。
        </NodeUsageHint>
      )

    case 'file-tree-classifier':
      return (
        <NodeUsageHint>
          通过文件树结构（子目录层级与文件分布）判断文件夹类别。无需配置。
        </NodeUsageHint>
      )

    case 'classification-reader':
      return (
        <NodeUsageHint>
          分类管道与处理管道之间的桥接节点。将 subtree-aggregator 输出的分类结果转为处理链可用的格式。完整流程中必须放在 subtree-aggregator 之后、folder-splitter 之前。无需配置。
        </NodeUsageHint>
      )

    case 'db-subtree-reader':
      return (
        <NodeUsageHint>
          从数据库读取指定目录及其子目录的分类结果，重建完整子树供处理流使用。通常接在 folder-picker（记录模式）之后，再接 folder-splitter。
        </NodeUsageHint>
      )

    case 'classification-db-result-preview':
      return <></>

    case 'processing-result-preview':
      return <></>

    case 'folder-splitter':
      return (
        <NodeUsageHint>
          将分类条目拆分为处理项列表。会初始化统一契约字段：`source_path` 表示原始来源，`current_path` 表示当前有效位置；开启 split_with_subdirs 后会递归拆分到叶子目录（不限 mixed）。
        </NodeUsageHint>
      )

    case 'category-router':
      return (
        <NodeUsageHint>
          按分类类别将处理项路由至对应输出端口（video / manga / photo / other / mixed_leaf）。将各端口连线到对应的处理节点即可，无需配置。
        </NodeUsageHint>
      )

    case 'mixed-leaf-router':
      return (
        <NodeUsageHint>
          专用于消费 `mixed_leaf` 最小叶子目录。节点会在原目录内原地物理拆分为 `__video` / `__photo` / `__unsupported` 并输出 video / photo / unsupported 三路目录处理项。无需配置参数。
        </NodeUsageHint>
      )

    case 'subtree-aggregator':
      return (
        <NodeUsageHint>
          聚合所有分类器的信号，取最高置信度结果，将最终分类写入数据库。需将各分类器的信号端口（signal_kw / signal_ft / signal_ext）分别连入对应输入端口。无需配置。
        </NodeUsageHint>
      )

    case 'folder-tree-scanner':
      return (
        <div className="space-y-3">
          <NodeUsageHint>
            必须将上游节点的输出连入本节点的 source_dir 端口以提供扫描根目录；不支持节点内配置，不读取系统环境变量。
          </NodeUsageHint>
          <ConfigField label="最大扫描深度" hint="向下递归的最大层级数（默认 5）">
            <input
              type="number"
              min={1}
              max={20}
              value={cfgNum(config, 'max_depth', 5)}
              onChange={(e) => set('max_depth', e.target.value)}
              className={FIELD_CLS}
            />
          </ConfigField>
          <ConfigField label="最少文件数" hint="文件数低于此值的文件夹将被跳过（默认 0，不过滤）">
            <input
              type="number"
              min={0}
              value={cfgNum(config, 'min_file_count', 0)}
              onChange={(e) => set('min_file_count', e.target.value)}
              className={FIELD_CLS}
            />
          </ConfigField>
        </div>
      )

    case 'confidence-check':
      return (
        <div className="space-y-3">
          <ConfigField
            label="置信度阈值"
            hint="信号置信度 ≥ 阈值走 high 端口，否则走 low 端口（默认 0.75，范围 0–1）"
          >
            <input
              type="number"
              min={0}
              max={1}
              step={0.05}
              value={cfgNum(config, 'threshold', 0.75)}
              onChange={(e) => set('threshold', e.target.value)}
              className={FIELD_CLS}
            />
          </ConfigField>
        </div>
      )

    case 'name-keyword-classifier':
      return (
        <div className="space-y-3">
          <ConfigField
            label="关键词规则（JSON）"
            hint={'格式：[{"keywords":["关键词"],"category":"video"}]，按顺序匹配，第一条命中规则生效'}
          >
            <textarea
              rows={5}
              value={cfgJson(config, 'rules')}
              onChange={(e) => set('rules', e.target.value)}
              placeholder={'[\n  {"keywords": ["电影"], "category": "video"}\n]'}
              className={TEXTAREA_FIELD_CLS}
            />
          </ConfigField>
        </div>
      )

    case 'rename-node':
      return (
        <div className="space-y-3">
          <ConfigField label="重命名策略">
            <select value={strategy} onChange={(e) => set('strategy', e.target.value)} className={FIELD_CLS}>
              <option value="simple">simple — 保持原名</option>
              <option value="template">template — 模板替换</option>
              <option value="regex_extract">regex_extract — 正则提取</option>
              <option value="conditional">conditional — 条件规则</option>
            </select>
          </ConfigField>
          {strategy === 'template' && (
            <ConfigField label="模板" hint="可用变量：{name} {title} {year} {category} {parent} {index}">
              <input
                type="text"
                value={cfgStr(config, 'template')}
                onChange={(e) => set('template', e.target.value)}
                placeholder="{title} ({year})"
                className={FIELD_CLS}
              />
            </ConfigField>
          )}
          {strategy === 'regex_extract' && (
            <ConfigField
              label="正则表达式"
              hint="使用命名捕获组 (?P<title>...) 提取标题，命中后输出捕获内容"
            >
              <input
                type="text"
                value={cfgStr(config, 'regex') || cfgStr(config, 'pattern')}
                onChange={(e) => set('regex', e.target.value)}
                placeholder="(?P<title>.+?)[\\s_]*[\\(\\[](?P<year>\\d{4})[\\)\\]]"
                className={FIELD_CLS}
              />
            </ConfigField>
          )}
          {strategy === 'conditional' && (
            <ConfigField
              label="条件规则（JSON）"
              hint={'格式：[{"condition":"category==video","template":"{title}"}]，condition 为 DEFAULT 时作为默认规则'}
            >
              <textarea
                rows={5}
                value={cfgJson(config, 'rules')}
                onChange={(e) => set('rules', e.target.value)}
                placeholder={'[\n  {"condition": "category==video", "template": "{title} ({year})"}\n]'}
                className={TEXTAREA_FIELD_CLS}
              />
            </ConfigField>
          )}
        </div>
      )

    case 'move-node':
      return (
        <div className="space-y-3">
          <NodeUsageHint>
            按处理项的 `current_path` 执行移动，成功后仅更新 `current_path`，保留 `source_path` 作为原始来源路径，便于后续缩略图、压缩和回滚稳定追踪。
          </NodeUsageHint>
          <ConfigField label="目标路径引用" hint="统一路径语义：输出目录 / 自定义路径">
            <ConfiguredPathField
              value={{
                pathRefType: cfgPathRefType(config, 'output'),
                pathRefKey: cfgPathRefKey(config, cfgStr(config, 'target_dir') || 'mixed:0'),
                pathSuffix: cfgPathSuffix(config),
              }}
              onChange={setPathRef}
              placeholder="/data/target"
              pickerTitle="选择目标目录"
              defaultOutputKey="mixed"
            />
          </ConfigField>
          <ConfigField label="冲突策略" hint="目标路径已存在时的处理方式（默认自动重命名）">
            <select
              value={cfgStr(config, 'conflict_policy') || 'auto_rename'}
              onChange={(e) => set('conflict_policy', e.target.value)}
              className={FIELD_CLS}
            >
              <option value="auto_rename">auto_rename — 自动重命名</option>
              <option value="skip">skip — 跳过</option>
              <option value="overwrite">overwrite — 覆盖</option>
            </select>
          </ConfigField>
        </div>
      )

    case 'compress-node':
      return (
        <div className="space-y-3">
          <NodeUsageHint>
            `items` 会透传原处理项；`archive_items` 会产出新的压缩包处理项，其中 `source_path` 仍保留原始来源，`current_path` 指向压缩产物，可直接接到 move-node / collect-node。
          </NodeUsageHint>
          <ConfigField label="打包模式" hint="默认 CBZ 存储式打包（zip.Store），仅封装不做 Deflate 压缩，优先降低 CPU 占用">
            <select
              value={cfgStr(config, 'format') || 'cbz'}
              onChange={(e) => set('format', e.target.value)}
              className={FIELD_CLS}
            >
              <option value="cbz">cbz — 存储式打包（默认）</option>
              <option value="zip">zip — 存储式打包</option>
            </select>
          </ConfigField>
          <ConfigField label="输出路径引用" hint="默认使用输出目录体系，仍支持自定义路径">
            <ConfiguredPathField
              value={{
                pathRefType: cfgPathRefType(config, 'output'),
                pathRefKey: cfgPathRefKey(config, cfgStr(config, 'target_dir') || 'mixed:0'),
                pathSuffix: cfgPathSuffix(config),
              }}
              onChange={setPathRef}
              placeholder="/data/archive"
              pickerTitle="选择输出目录"
              defaultOutputKey="mixed"
            />
          </ConfigField>
        </div>
      )

    case 'thumbnail-node':
      return (
        <div className="space-y-3">
          <NodeUsageHint>
            优先读取处理项的 `current_path` 作为当前有效媒体位置。未配置输出目录时，会跟随当前目录就近生成缩略图，移动后的目录也能稳定命中。
          </NodeUsageHint>
          <ConfigField label="输出路径引用" hint="留空时默认跟随当前有效目录，仍支持显式指定输出目录">
            <ConfiguredPathField
              value={{
                pathRefType: cfgPathRefType(config, 'output'),
                pathRefKey: cfgPathRefKey(config, cfgStr(config, 'output_dir') || 'video:0'),
                pathSuffix: cfgPathSuffix(config),
              }}
              onChange={setPathRef}
              placeholder="/data/thumbnails"
              pickerTitle="选择缩略图输出目录"
              defaultOutputKey="video"
            />
          </ConfigField>
          <ConfigField label="截图偏移（秒）" hint="从视频第几秒截取缩略图（默认 8）">
            <input
              type="number"
              min={0}
              value={cfgNum(config, 'offset_seconds', 8)}
              onChange={(e) => set('offset_seconds', e.target.value)}
              className={FIELD_CLS}
            />
          </ConfigField>
          <ConfigField label="缩略图宽度（像素）" hint="输出图片宽度，高度等比缩放（默认 640）">
            <input
              type="number"
              min={64}
              max={1920}
              value={cfgNum(config, 'width', 640)}
              onChange={(e) => set('width', e.target.value)}
              className={FIELD_CLS}
            />
          </ConfigField>
        </div>
      )

    case 'folder-picker':
      return (
        <FolderPickerConfigPanel nodeId={nodeId} config={config} updateNodeConfig={updateNodeConfig} />
      )

    default:
      return (
        <NodeUsageHint>
          该节点暂无可配置项。
        </NodeUsageHint>
      )
  }
}

// ─── FolderPickerConfigPanel ──────────────────────────────────────────────────

interface FolderPickerConfigPanelProps {
  nodeId: string
  config: Record<string, unknown>
  updateNodeConfig: (nodeId: string, key: string, rawValue: string) => void
}

function FolderPickerConfigPanel({ nodeId, config, updateNodeConfig }: FolderPickerConfigPanelProps) {
  const [query, setQuery] = useState('')
  const [recordsLoading, setRecordsLoading] = useState(true)
  const [recordsError, setRecordsError] = useState<string | null>(null)
  const [records, setRecords] = useState<Folder[]>([])

  const sourceMode = typeof config['source_mode'] === 'string' && config['source_mode'] === 'folders'
    ? 'folders'
    : 'path'

  const path = typeof config['path'] === 'string'
    ? config['path']
    : Array.isArray(config['paths'])
      ? (config['paths'].find((item): item is string => typeof item === 'string') ?? '')
      : ''
  const pathRefType = config['path_ref_type'] === 'output' || config['path_ref_type'] === 'custom'
    ? config['path_ref_type']
    : 'output'
  const pathRefKey = typeof config['path_ref_key'] === 'string' && config['path_ref_key'].trim() !== ''
    ? config['path_ref_key']
    : path
  const pathSuffix = typeof config['path_suffix'] === 'string' ? config['path_suffix'] : ''
  const rawFolderIDs = config['folder_ids']
  const folderIDsCompat: string[] = Array.isArray(rawFolderIDs)
    ? rawFolderIDs.filter((item): item is string => typeof item === 'string')
    : []
  const rawSavedFolderIDs = config['saved_folder_ids']
  const savedFolderID = typeof config['saved_folder_id'] === 'string'
    ? config['saved_folder_id']
    : Array.isArray(rawSavedFolderIDs)
      ? (rawSavedFolderIDs.find((item): item is string => typeof item === 'string') ?? (folderIDsCompat[0] ?? ''))
      : (folderIDsCompat[0] ?? '')
  const savedFolderIDsCompat: string[] = Array.isArray(rawSavedFolderIDs)
    ? rawSavedFolderIDs.filter((item): item is string => typeof item === 'string')
    : folderIDsCompat

  useEffect(() => {
    const requestTopLevelOnly = true
    let active = true
    void listFolders({
      q: query.trim() || undefined,
      limit: 100,
      page: 1,
      top_level_only: requestTopLevelOnly,
    }).then((res) => {
      if (!active) return
      setRecords(res.data)
    }).catch((err: unknown) => {
      if (!active) return
      setRecordsError(err instanceof Error ? err.message : '读取文件夹记录失败')
    }).finally(() => {
      if (active) setRecordsLoading(false)
    })

    return () => { active = false }
  }, [query])

  function setSourceMode(mode: 'path' | 'folders') {
    updateNodeConfig(nodeId, 'source_mode', mode)
    if (mode === 'path') {
      updateNodeConfig(nodeId, 'saved_folder_id', '')
      updateNodeConfig(nodeId, 'folder_ids', '[]')
      updateNodeConfig(nodeId, 'saved_folder_ids', '[]')
      return
    }
    updateNodeConfig(nodeId, 'path', '')
    updateNodeConfig(nodeId, 'path_ref_type', 'custom')
    updateNodeConfig(nodeId, 'path_ref_key', '')
    updateNodeConfig(nodeId, 'path_suffix', '')
    updateNodeConfig(nodeId, 'paths', '[]')
    updateNodeConfig(nodeId, 'paths_sources', '[]')
    updateNodeConfig(nodeId, 'paths_option_ids', '[]')
  }

  function setPath(nextValue: { pathRefType: 'output' | 'custom'; pathRefKey: string; pathSuffix: string }) {
    updateNodeConfig(nodeId, 'path', nextValue.pathRefType === 'custom' ? nextValue.pathRefKey : '')
    updateNodeConfig(nodeId, 'path_ref_type', nextValue.pathRefType)
    updateNodeConfig(nodeId, 'path_ref_key', nextValue.pathRefKey)
    updateNodeConfig(nodeId, 'path_suffix', nextValue.pathSuffix)
    updateNodeConfig(nodeId, 'path_source', 'custom')
    updateNodeConfig(nodeId, 'path_option_id', '')
    // 清空旧数组字段，收口为单值配置。
    updateNodeConfig(nodeId, 'paths', '[]')
    updateNodeConfig(nodeId, 'paths_sources', '[]')
    updateNodeConfig(nodeId, 'paths_option_ids', '[]')
  }

  function setSavedFolderID(nextID: string) {
    updateNodeConfig(nodeId, 'saved_folder_id', nextID)
    // 清空旧数组字段，收口为单值配置。
    updateNodeConfig(nodeId, 'saved_folder_ids', '[]')
    updateNodeConfig(nodeId, 'folder_ids', '[]')
  }

  function selectFolderRecord(id: string) {
    if (savedFolderID === id) {
      setSavedFolderID('')
      return
    }
    setSavedFolderID(id)
  }

  return (
    <div className="space-y-3">
      <NodeUsageHint>
        支持两种互斥模式：路径模式（单路径）与记录模式（单记录）。folders 端口输出目录树，path 端口输出该目录路径。
      </NodeUsageHint>
      <div className="grid grid-cols-2 gap-2">
        <button
          type="button"
          onClick={() => setSourceMode('path')}
          className={cn(
            'border-2 px-3 py-2 text-xs font-bold transition-all',
            sourceMode === 'path'
              ? 'border-foreground bg-foreground text-background'
              : 'border-foreground bg-background text-foreground',
          )}
        >
          路径模式
        </button>
        <button
          type="button"
          onClick={() => setSourceMode('folders')}
          className={cn(
            'border-2 px-3 py-2 text-xs font-bold transition-all',
            sourceMode === 'folders'
              ? 'border-foreground bg-foreground text-background'
              : 'border-foreground bg-background text-foreground',
          )}
        >
          记录模式
        </button>
      </div>

      {sourceMode === 'path' ? (
        <ConfigField label="文件夹路径" hint="运行时将该目录作为目录树输出（仅支持单路径）">
          <ConfiguredPathField
            value={{
              pathRefType,
              pathRefKey,
              pathSuffix,
            }}
            onChange={setPath}
            placeholder="/data/folder"
            pickerTitle="选择文件夹"
            defaultOutputKey="mixed"
          />
        </ConfigField>
      ) : (
        <ConfigField label="数据库已保存文件夹" hint="从数据库中的文件夹记录里选择 1 条目录记录">
          <div className="space-y-2">
            <input
              value={query}
              onChange={(e) => {
                setRecordsLoading(true)
                setRecordsError(null)
                setQuery(e.target.value)
              }}
              placeholder="搜索路径或目录名"
              className={FIELD_CLS}
            />
            <div className="max-h-64 space-y-1 overflow-auto border-2 border-foreground bg-muted/20 p-2">
              {recordsLoading ? (
                <p className="py-6 text-center text-xs font-bold text-muted-foreground">加载中...</p>
              ) : recordsError ? (
                <p className="py-6 text-center text-xs font-bold text-red-600">{recordsError}</p>
              ) : records.length === 0 ? (
                <p className="py-6 text-center text-xs font-bold text-muted-foreground">暂无可选记录</p>
              ) : (
                records.map((record) => (
                  <label key={record.id} className="flex cursor-pointer items-start gap-2 border-2 border-foreground bg-background px-2 py-2">
                    <input
                      type="radio"
                      name={`folder-picker-record-${nodeId}`}
                      checked={savedFolderID === record.id}
                      onChange={() => selectFolderRecord(record.id)}
                      className="mt-0.5 h-4 w-4 rounded-none border-2 border-foreground text-foreground focus:ring-foreground focus:ring-offset-0"
                    />
                    <span className="min-w-0">
                      <span className="block truncate text-xs font-black">{record.name}</span>
                      <span className="block truncate font-mono text-[10px] text-muted-foreground">{record.path}</span>
                    </span>
                  </label>
                ))
              )}
            </div>
            {(savedFolderID === '' && savedFolderIDsCompat.length > 1) && (
              <p className="text-[10px] font-bold text-amber-700">
                检测到旧版多选配置，当前仅会使用单条记录，请重新选择后保存。
              </p>
            )}
          </div>
        </ConfigField>
      )}
    </div>
  )
}

// ─── WorkflowNodeCard ─────────────────────────────────────────────────────────

const NODE_STATUS_CFG: Record<NodeRunStatus, { label: string; cls: string; icon: ReactElement | null }> = {
  running: { label: '执行中', cls: 'text-amber-900 bg-amber-200 border-2 border-foreground', icon: <Loader2 className="h-3 w-3 animate-spin" /> },
  succeeded: { label: '完成', cls: 'text-green-900 bg-green-200 border-2 border-foreground', icon: <CheckCircle2 className="h-3 w-3" /> },
  failed: { label: '失败', cls: 'text-red-900 bg-red-200 border-2 border-foreground', icon: <TriangleAlert className="h-3 w-3" /> },
  pending: { label: '等待中', cls: 'text-muted-foreground bg-muted border-2 border-foreground', icon: null },
  skipped: { label: '已跳过', cls: 'text-slate-900 bg-slate-200 border-2 border-foreground', icon: null },
  waiting_input: { label: '等待输入', cls: 'text-blue-900 bg-blue-200 border-2 border-foreground', icon: <Loader2 className="h-3 w-3 animate-pulse" /> },
}

const PORT_TYPE_COLORS: Record<string, string> = {
  PROCESSING_ITEM_LIST: '!bg-orange-500',
  FOLDER_TREE_LIST: '!bg-green-500',
  CLASSIFICATION_SIGNAL_LIST: '!bg-blue-500',
  CLASSIFIED_ENTRY_LIST: '!bg-purple-500',
  MOVE_RESULT_LIST: '!bg-red-400',
  STRING_LIST: '!bg-gray-400',
  STRING: '!bg-gray-400',
  PATH: '!bg-yellow-500',
  JSON: '!bg-teal-500',
  BOOLEAN: '!bg-pink-500',
}

function WorkflowNodeCard({ id, data, selected }: NodeProps<EditorNode>) {
  const { workflowNodes, updateNode, updateNodeConfig, deleteNode, nodeRunByNodeId, rollbackingRunId, onRollbackRun, onViewNodeError } =
    useEditorContext()
  const workflowNode = workflowNodes[id] ?? null
  const nodeRun = nodeRunByNodeId[id] ?? null

  const [expanded, setExpanded] = useState(true)

  const inputPorts = data.schema?.input_ports ?? []
  const outputPorts = data.schema?.output_ports ?? []
  const category = getNodeCategory(data.type)

  const renamePreview = useMemo(() => {
    if (!workflowNode || data.type !== 'rename-node') return null
    return buildRenamePreview(workflowNode.config)
  }, [workflowNode, data.type])

  const nodeRef = useRef<HTMLDivElement | null>(null)
  const previewRef = useRef<HTMLDivElement | null>(null)
  const updateNodeInternals = useUpdateNodeInternals()
  const previewSummary = useMemo(() => parseNodePreviewSummary(nodeRun), [nodeRun])
  const classificationSummary = useMemo(
    () => (isClassificationSummary(previewSummary) ? previewSummary : null),
    [previewSummary],
  )
  const processingSummary = useMemo(
    () => (isProcessingSummary(previewSummary) ? previewSummary : null),
    [previewSummary],
  )
  const isPreviewNode = data.type === 'classification-db-result-preview' || data.type === 'processing-result-preview'

  useEffect(() => {
    if (nodeRef.current) {
      gsap.fromTo(nodeRef.current, { scale: 0.8, opacity: 0, y: 20 }, { scale: 1, opacity: 1, y: 0, duration: 0.5, ease: "back.out(1.5)" })
    }
  }, [])

  useEffect(() => {
    if (!isPreviewNode || !expanded) return
    const element = previewRef.current
    if (!element) return
    let rafId = 0
    const observer = new ResizeObserver(() => {
      cancelAnimationFrame(rafId)
      rafId = requestAnimationFrame(() => {
        updateNodeInternals(id)
      })
    })
    observer.observe(element)
    return () => {
      cancelAnimationFrame(rafId)
      observer.disconnect()
    }
  }, [id, expanded, isPreviewNode, updateNodeInternals, classificationSummary, processingSummary, nodeRun?.status, nodeRun?.error])

  // 差异化节点样式逻辑
  const isTrigger = category.types.has('trigger')
  const isRouter = category.types.has('category-router') || category.types.has('folder-splitter') || category.types.has('confidence-check') || category.types.has('mixed-leaf-router')
  
  let nodeStyleClass = 'bg-card border-2 border-foreground rounded-none'
  let headerStyleClass = 'bg-foreground text-background px-3 py-1.5 flex items-center justify-between'
  
  if (isTrigger) {
    nodeStyleClass = 'bg-primary border-2 border-foreground rounded-l-full pl-2'
    headerStyleClass = 'bg-transparent text-foreground px-3 py-1.5 flex items-center justify-between'
  } else if (isRouter) {
    nodeStyleClass = 'bg-muted border-2 border-foreground rounded-none'
    headerStyleClass = 'bg-transparent border-b-2 border-foreground text-foreground px-3 py-1.5 flex items-center justify-between'
  }

  return (
    <div
      ref={nodeRef}
      className={cn(
        'relative shadow-hard transition-all duration-200 cursor-grab active:cursor-grabbing',
        nodeStyleClass,
        data.enabled ? 'opacity-100' : 'opacity-60 grayscale-[0.5]',
        expanded ? 'min-w-[300px] max-w-[680px] w-fit' : 'min-w-[220px] max-w-[260px]',
        selected ? 'shadow-hard-hover -translate-y-1 ring-2 ring-foreground ring-offset-2 ring-offset-background' : 'hover:-translate-y-0.5 hover:shadow-hard-hover',
      )}
    >

      {/* Header */}
      <div className={headerStyleClass}>
        <div className="flex items-center gap-2 min-w-0">
          {isTrigger && (
            <div className="w-8 h-8 rounded-full bg-foreground flex items-center justify-center text-background shrink-0">
              <Play className="w-4 h-4 ml-0.5" />
            </div>
          )}
          {!isTrigger && !isRouter && (
            <div className={cn('w-2 h-2 rounded-full shrink-0', category.iconColor.replace('text-', 'bg-'))} />
          )}
          <div className="min-w-0">
            <p className="truncate text-sm font-black tracking-widest">{data.label}</p>
            {isTrigger && <p className="truncate font-mono text-[10px] font-bold opacity-80">{data.type}</p>}
          </div>
        </div>
        <div className="flex items-center gap-1 shrink-0">
          {!isTrigger && (
            <span
              className={cn(
                'border-2 px-1.5 py-0.5 text-[10px] font-bold',
                data.enabled ? 'border-transparent' : 'border-foreground bg-background text-foreground',
              )}
            >
              {data.enabled ? '' : '停用'}
            </span>
          )}
          <button
            type="button"
            className="nodrag flex items-center justify-center w-5 h-5 opacity-60 hover:opacity-100 transition-opacity"
            onClick={(e) => { e.stopPropagation(); setExpanded((v) => !v) }}
            title={expanded ? '折叠' : '展开'}
          >
            {expanded ? <ChevronUp className="w-3.5 h-3.5" /> : <ChevronDown className="w-3.5 h-3.5" />}
          </button>
        </div>
      </div>

      {/* Port rows */}
      {(inputPorts.length > 0 || outputPorts.length > 0) && (
        <div className="pt-1 pb-0.5">
          {inputPorts.map((port, index) => (
            <div
              key={`in-${port.name}`}
              className="relative flex items-center gap-1.5 py-0.5 pl-4 pr-3"
              title={port.description || undefined}
            >
              <Handle
                id={`in-${index}`}
                type="target"
                position={Position.Left}
                style={{ left: '-7px', top: '50%', transform: 'translateY(-50%)' }}
                className={cn(
                  '!w-2.5 !h-2.5 !border-2 !border-background transition-transform hover:!scale-125 [&.connecting]:!scale-125 [&.valid]:!ring-2 [&.valid]:!ring-emerald-500',
                  PORT_TYPE_COLORS[port.type] ?? '!bg-foreground',
                )}
              />
              <span className="truncate font-mono text-[10px] text-foreground/80">{port.name}</span>
              {port.required && <span className="shrink-0 text-[8px] font-bold text-destructive">*</span>}
            </div>
          ))}
          {outputPorts.map((port, index) => (
            <div
              key={`out-${port.name}`}
              className="relative flex items-center justify-end gap-1.5 py-0.5 pl-3 pr-4"
              title={port.description || undefined}
            >
              <span className="truncate font-mono text-[10px] text-foreground/80">{port.name}</span>
              <Handle
                id={`out-${index}`}
                type="source"
                position={Position.Right}
                style={{ right: '-7px', top: '50%', transform: 'translateY(-50%)' }}
                className={cn(
                  '!w-2.5 !h-2.5 !border-2 !border-background transition-transform hover:!scale-125 [&.connecting]:!scale-125 [&.valid]:!ring-2 [&.valid]:!ring-emerald-500',
                  PORT_TYPE_COLORS[port.type] ?? '!bg-foreground',
                )}
              />
            </div>
          ))}
        </div>
      )}

      {/* Body */}
      <div ref={previewRef} className="px-3 py-3">
        {!isTrigger && data.type !== 'classification-db-result-preview' && data.type !== 'processing-result-preview' && (
          <p className="mb-2 truncate font-mono text-[10px] font-bold text-muted-foreground">{data.type}</p>
        )}

        {nodeRun && (() => {
          const cfg = NODE_STATUS_CFG[nodeRun.status]
          const hasProgress = typeof nodeRun.progress_percent === 'number'
          const progressLabel = hasProgress ? `${nodeRun.progress_percent}%` : '未开始'
          const progressStage = nodeRun.progress_stage || nodeRun.progress_message || ''
          const progressPath = nodeRun.progress_source_path || nodeRun.progress_target_path || ''
          return (
            <div className="mt-2 space-y-2">
              <div className="flex items-center gap-2">
                <div className={cn('inline-flex items-center gap-1.5 px-2 py-1 text-[10px] font-bold', cfg.cls)}>
                  {cfg.icon}
                  {cfg.label}
                </div>
                <span className="text-[10px] font-black text-foreground">{progressLabel}</span>
                {progressStage && <span className="truncate text-[10px] font-bold text-muted-foreground">{progressStage}</span>}
              </div>
              {progressPath && (
                <p className="truncate font-mono text-[10px] font-bold text-muted-foreground">{progressPath}</p>
              )}
              {nodeRun.error && (
                <button
                  type="button"
                  onClick={() => onViewNodeError(id)}
                  className="nodrag border-2 border-red-900 bg-red-100 px-2 py-1 text-[10px] font-bold text-red-900 transition-all hover:bg-red-900 hover:text-red-100"
                >
                  查看错误
                </button>
              )}
            </div>
          )
        })()}
        {data.type === 'classification-db-result-preview' && (
          <div className="mt-2 max-h-[42vh] overflow-y-auto">
            <ClassificationPreviewInline summary={classificationSummary} />
          </div>
        )}

        {data.type === 'processing-result-preview' && (
          <div className="mt-2 max-h-[42vh] overflow-y-auto">
            <ProcessingPreviewInline summary={processingSummary} />
          </div>
        )}
      </div>

      {/* Expanded config form */}
      {expanded && (
        <div className="nodrag nowheel nopan max-h-[55vh] overflow-y-auto border-t-2 border-foreground bg-background px-4 pb-4 pt-4">
          <div className="space-y-4">
            {/* Enable toggle */}
            <label className="flex cursor-pointer items-center justify-between border-2 border-foreground bg-muted/30 px-3 py-2 transition-colors hover:bg-muted/50">
              <span className="text-sm font-bold">启用该节点</span>
              <input
                type="checkbox"
                checked={data.enabled}
                onChange={(e) => updateNode(id, { enabled: e.target.checked })}
                className="h-4 w-4 rounded-none border-2 border-foreground text-foreground focus:ring-foreground focus:ring-offset-0"
              />
            </label>

            {/* Rename preview */}
            {renamePreview && (
              <div className="border-2 border-foreground bg-card p-3 shadow-hard">
                <div className="flex items-center justify-between border-b-2 border-foreground pb-2 mb-2">
                  <p className="text-xs font-black tracking-widest">RENAME PREVIEW</p>
                  <span className="text-[10px] font-bold bg-foreground text-background px-1.5 py-0.5">STRATEGY: {renamePreview.strategy}</span>
                </div>
                <p className="text-[10px] font-bold text-muted-foreground mb-1">
                  SAMPLE: <span className="font-mono text-foreground">{RENAME_PREVIEW_SAMPLE.name}</span>
                </p>
                <div className="border-2 border-foreground bg-muted/30 px-2 py-2">
                  <p className="text-[10px] font-bold text-muted-foreground mb-0.5">TARGET</p>
                  <p className="font-mono text-sm font-bold text-foreground break-all">{renamePreview.targetName}</p>
                </div>
                {renamePreview.warning && (
                  <p className="mt-2 text-[10px] font-bold text-red-600 bg-red-100 border-2 border-red-900 px-2 py-1">{renamePreview.warning}</p>
                )}
              </div>
            )}

            {workflowNode && (
              <NodeConfigPanel
                nodeId={id}
                nodeType={data.type}
                config={workflowNode.config}
                updateNodeConfig={updateNodeConfig}
              />
            )}

            {nodeRun && nodeRun.status === 'succeeded' && (
              <button
                type="button"
                disabled={rollbackingRunId === nodeRun.workflow_run_id}
                onClick={() => void onRollbackRun(nodeRun.workflow_run_id)}
                className="inline-flex w-full items-center justify-center gap-2 border-2 border-amber-900 bg-amber-200 px-3 py-2 text-sm font-bold text-amber-900 transition-all hover:bg-amber-900 hover:text-amber-100 hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-amber-200 disabled:hover:text-amber-900 disabled:hover:shadow-none disabled:hover:translate-y-0"
              >
                <RotateCcw className="h-4 w-4" />
                {rollbackingRunId === nodeRun.workflow_run_id ? '回退中...' : '回退此节点的工作流运行'}
              </button>
            )}
            <button
              type="button"
              onClick={() => deleteNode(id)}
              className="inline-flex w-full items-center justify-center gap-2 border-2 border-red-900 bg-red-100 px-3 py-2 text-sm font-bold text-red-900 transition-all hover:bg-red-900 hover:text-red-100 hover:shadow-hard hover:-translate-y-0.5"
            >
              <Trash2 className="h-4 w-4" />
              删除该节点
            </button>
          </div>
        </div>
      )}
    </div>
  )
}

const NODE_TYPES = { workflowNode: WorkflowNodeCard }

// ─── WorkflowEditorScreen ─────────────────────────────────────────────────────

function WorkflowEditorScreen() {
  const navigate = useNavigate()
  const params = useParams<{ id: string }>()
  const theme = useThemeStore((s) => s.theme)
  const workflowDefId = params.id ?? ''

  const [workflowDef, setWorkflowDef] = useState<WorkflowDefinition | null>(null)
  const [workflowNodes, setWorkflowNodes] = useState<Record<string, WorkflowGraphNode>>({})
  const [schemas, setSchemas] = useState<NodeSchema[]>([])
  const [nodes, setNodes, onNodesChange] = useNodesState<EditorNode>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([])
  const [selectedEdgeId, setSelectedEdgeId] = useState<string | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [isSaving, setIsSaving] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [isNodePanelOpen, setIsNodePanelOpen] = useState(false)
  const [isRunning, setIsRunning] = useState(false)
  const [configAutoSaveTick, setConfigAutoSaveTick] = useState(0)
  const [selectionModeOn, setSelectionModeOn] = useState(false)
  const [rollbackingRunId, setRollbackingRunId] = useState<string | null>(null)
  const [nodeErrorModal, setNodeErrorModal] = useState<{ nodeId: string; nodeLabel: string; error: string } | null>(null)
  const canvasViewportRef = useRef<HTMLDivElement | null>(null)
  const reactFlowRef = useRef<FlowPositionProjector | null>(null)
  const reconnectingEdgeRef = useRef<Edge | null>(null)
  const reconnectHandledRef = useRef(false)

  const recentLaunchRecord = useWorkflowRunStore((s) => {
    if (!workflowDefId) return undefined
    return s.recentLaunchByScope[`global:${workflowDefId.trim()}`]
  })
  const activeJobId = recentLaunchRecord?.jobId ?? null
  const nodesByRunId = useWorkflowRunStore((s) => s.nodesByRunId)
  const runsByJobId = useWorkflowRunStore((s) => s.runsByJobId)
  const bindLatestLaunch = useWorkflowRunStore((s) => s.bindLatestLaunch)
  const restoreLatestLaunch = useWorkflowRunStore((s) => s.restoreLatestLaunch)
  const buildRunCardView = useWorkflowRunStore((s) => s.buildRunCardView)
  const fetchRunsForJob = useWorkflowRunStore((s) => s.fetchRunsForJob)
  const fetchRunDetail = useWorkflowRunStore((s) => s.fetchRunDetail)
  const rollbackRun = useWorkflowRunStore((s) => s.rollbackRun)

  const nodeRunByNodeId = useMemo<Record<string, NodeRun>>(() => {
    if (!activeJobId) return {}
    const runs = runsByJobId[activeJobId] ?? []
    const result: Record<string, NodeRun> = {}
    for (const run of runs) {
      const nodeRuns = nodesByRunId[run.id] ?? []
      for (const nr of nodeRuns) {
        const prev = result[nr.node_id]
        if (!prev || nr.sequence > prev.sequence) result[nr.node_id] = nr
      }
    }
    return result
  }, [activeJobId, runsByJobId, nodesByRunId])

  const shownNodeErrorKeysRef = useRef<Set<string>>(new Set())

  const openNodeErrorModal = useCallback((nodeId: string) => {
    const nodeRun = nodeRunByNodeId[nodeId]
    if (!nodeRun?.error) return
    const fallbackLabel = workflowNodes[nodeId]?.label || nodeId
    const currentLabel = nodes.find((node) => node.id === nodeId)?.data.label || fallbackLabel
    setNodeErrorModal({ nodeId, nodeLabel: currentLabel, error: nodeRun.error })
  }, [nodeRunByNodeId, workflowNodes, nodes])

  useEffect(() => {
    const errorRuns = Object.values(nodeRunByNodeId)
      .filter((run) => typeof run.error === 'string' && run.error.trim() !== '')
      .sort((a, b) => b.sequence - a.sequence)

    const nextErrorRun = errorRuns.find((run) => {
      const errorKey = `${run.id}:${run.node_id}:${run.sequence}:${run.error ?? ''}`
      return !shownNodeErrorKeysRef.current.has(errorKey)
    })

    if (!nextErrorRun) return

    const errorKey = `${nextErrorRun.id}:${nextErrorRun.node_id}:${nextErrorRun.sequence}:${nextErrorRun.error ?? ''}`
    shownNodeErrorKeysRef.current.add(errorKey)
    openNodeErrorModal(nextErrorRun.node_id)
  }, [nodeRunByNodeId, openNodeErrorModal])

  useEffect(() => {
    if (!activeJobId) return
    void fetchRunsForJob(activeJobId)
  }, [activeJobId, fetchRunsForJob])

  useEffect(() => {
    if (!workflowDefId) return
    void restoreLatestLaunch(workflowDefId)
  }, [workflowDefId, restoreLatestLaunch])

  const activeRunIds = useMemo(() => {
    if (!activeJobId) return []
    return (runsByJobId[activeJobId] ?? [])
      .map((run) => run.id)
      .sort((a, b) => a.localeCompare(b))
  }, [activeJobId, runsByJobId])

  const activeRunIdsKey = activeRunIds.join('|')
  const enabledNodeCount = useMemo(
    () => Object.values(workflowNodes).filter((node) => node.enabled !== false).length,
    [workflowNodes],
  )
  const editorRunCardView = workflowDefId ? buildRunCardView(workflowDefId, enabledNodeCount) : null

  useEffect(() => {
    if (!activeRunIdsKey) return
    activeRunIdsKey.split('|').forEach((runId) => {
      void fetchRunDetail(runId)
    })
  }, [activeRunIdsKey, fetchRunDetail])

  const handleRollbackRun = useCallback(async (runId: string) => {
    if (rollbackingRunId) return
    setRollbackingRunId(runId)
    setError(null)
    try {
      await rollbackRun(runId)
      if (activeJobId) await fetchRunsForJob(activeJobId)
      setNotice('工作流回退完成')
    } catch (rollbackError) {
      setError(rollbackError instanceof Error ? rollbackError.message : '回退失败')
    } finally {
      setRollbackingRunId(null)
    }
  }, [rollbackRun, activeJobId, fetchRunsForJob, rollbackingRunId])

  const schemaMap = useMemo(() => new Map(schemas.map((schema) => [schema.type, schema])), [schemas])
  const schemaMapRef = useRef(schemaMap)
  schemaMapRef.current = schemaMap
  const workflowDefRef = useRef(workflowDef)
  workflowDefRef.current = workflowDef
  const nodesRef = useRef(nodes)
  nodesRef.current = nodes
  const edgesRef = useRef(edges)
  edgesRef.current = edges
  const workflowNodesRef = useRef(workflowNodes)
  workflowNodesRef.current = workflowNodes

  useEffect(() => {
    let active = true

    async function load() {
      if (!workflowDefId) {
        setError('缺少工作流 ID')
        setIsLoading(false)
        return
      }

      setIsLoading(true)
      setError(null)
      try {
        const [workflowResponse, nodeTypeResponse] = await Promise.all([
          getWorkflowDef(workflowDefId),
          listNodeTypes(),
        ])
        if (!active) return

        const nextSchemas = nodeTypeResponse.data ?? []
        const nextSchemaMap = new Map(nextSchemas.map((schema) => [schema.type, schema]))
        const nextWorkflow = workflowResponse.data
        const graph = safeParseGraph(nextWorkflow.graph_json)
        const nextWorkflowNodes = Object.fromEntries(graph.nodes.map((node) => [node.id, node]))

        setSchemas(nextSchemas)
        setWorkflowDef(nextWorkflow)
        setWorkflowNodes(nextWorkflowNodes)
        setNodes(graphToNodes(graph, nextSchemaMap))
        setEdges(graphToEdges(graph, nextSchemaMap))
        setSelectedEdgeId(null)
      } catch (loadError) {
        if (!active) return
        setError(loadError instanceof Error ? loadError.message : '加载工作流编辑器失败')
      } finally {
        if (active) setIsLoading(false)
      }
    }

    void load()
    return () => { active = false }
  }, [workflowDefId, setEdges, setNodes])

  const onConnectStart = useMemo<OnConnectStart>(
    () => (_event, params) => {
      reconnectHandledRef.current = false
      const handleType = params.handleType
      const handleId = params.handleId ?? null
      const nodeId = params.nodeId
      if (!handleType || !handleId || !nodeId) {
        reconnectingEdgeRef.current = null
        return
      }

      const matched = edgesRef.current.find((edge) => {
        if (handleType === 'source') {
          return edge.source === nodeId && edge.sourceHandle === handleId
        }
        return edge.target === nodeId && edge.targetHandle === handleId
      })
      reconnectingEdgeRef.current = matched ?? null
      if (matched) {
        setEdges((currentEdges) => currentEdges.filter((edge) => edge.id !== matched.id))
        setSelectedEdgeId((currentSelectedEdgeId) => (currentSelectedEdgeId === matched.id ? null : currentSelectedEdgeId))
      }
    },
    [setEdges],
  )

  const restoreReconnectingEdge = useCallback(() => {
    const reconnectingEdge = reconnectingEdgeRef.current
    if (!reconnectingEdge) return

    reconnectingEdgeRef.current = null
    setEdges((currentEdges) => {
      if (currentEdges.some((edge) => edge.id === reconnectingEdge.id)) return currentEdges
      return [...currentEdges, reconnectingEdge]
    })
  }, [setEdges])

  const applyEditorConnection = useCallback((connection: Connection) => {
    const getSchema = (nodeId: string) => {
      const node = nodesRef.current.find((item) => item.id === nodeId)
      if (!node) return undefined
      return schemaMapRef.current.get(node.data.type)
    }
    if (!isValidEditorConnection(connection, getSchema)) {
      setNotice('该连线无效，已阻止连接')
      restoreReconnectingEdge()
      reconnectHandledRef.current = false
      return
    }

    setEdges((currentEdges) => {
      if (!connection.source || !connection.target || !connection.sourceHandle || !connection.targetHandle) {
        return currentEdges
      }

      const reconnectingEdge = reconnectingEdgeRef.current
      if (reconnectingEdge) {
        reconnectHandledRef.current = true
        reconnectingEdgeRef.current = null
        const filtered = currentEdges.filter((edge) => !(
          edge.id === reconnectingEdge.id
          || (edge.target === connection.target && edge.targetHandle === connection.targetHandle)
          || (edge.source === connection.source && edge.sourceHandle === connection.sourceHandle)
        ))
        return [
          ...filtered,
          {
            ...reconnectingEdge,
            id: reconnectingEdge.id,
            source: connection.source,
            target: connection.target,
            sourceHandle: connection.sourceHandle,
            targetHandle: connection.targetHandle,
            animated: false,
          },
        ]
      }

      const withoutExisting = currentEdges.filter(
        (edge) => !(
          (edge.target === connection.target && edge.targetHandle === connection.targetHandle)
          || (edge.source === connection.source && edge.sourceHandle === connection.sourceHandle)
        ),
      )
      return addEdge({ ...connection, animated: false }, withoutExisting)
    })
    setNotice(null)
  }, [restoreReconnectingEdge, setEdges])

  const onConnect = useMemo<OnConnect>(
    () => (connection: Connection) => {
      // #region agent log
      sendAgentDebugLog({
        runId: 'pre-fix',
        hypothesisId: 'H2',
        location: 'WorkflowEditorPage.tsx:onConnect',
        message: 'edge connected in editor',
        data: {
          source: connection.source ?? '',
          target: connection.target ?? '',
          sourceHandle: connection.sourceHandle ?? '',
          targetHandle: connection.targetHandle ?? '',
        },
      })
      // #endregion
      applyEditorConnection(connection)
    },
    [applyEditorConnection],
  )

  const onReconnect = useMemo<OnReconnect>(
    () => (oldEdge, newConnection) => {
      const getSchema = (nodeId: string) => {
        const node = nodesRef.current.find((item) => item.id === nodeId)
        if (!node) return undefined
        return schemaMapRef.current.get(node.data.type)
      }
      if (!isValidEditorConnection(newConnection, getSchema)) {
        setNotice('该连线无效，已阻止重连')
        return
      }
      if (!newConnection.source || !newConnection.target || !newConnection.sourceHandle || !newConnection.targetHandle) {
        return
      }

      setEdges((currentEdges) => {
        const filtered = currentEdges.filter((edge) => {
          if (edge.id === oldEdge.id) return false
          return !(
            (edge.target === newConnection.target && edge.targetHandle === newConnection.targetHandle)
            || (edge.source === newConnection.source && edge.sourceHandle === newConnection.sourceHandle)
          )
        })
        return [
          ...filtered,
          {
            ...oldEdge,
            source: newConnection.source,
            target: newConnection.target,
            sourceHandle: newConnection.sourceHandle,
            targetHandle: newConnection.targetHandle,
            animated: false,
          },
        ]
      })
      setNotice(null)
    },
    [setEdges],
  )

  const onConnectEnd = useMemo<OnConnectEnd>(
    () => (event, connectionState) => {
      if (connectionState.isValid && reconnectingEdgeRef.current && !reconnectHandledRef.current && connectionState.fromHandle && connectionState.toHandle) {
        const connection: Connection = connectionState.fromHandle.type === 'target'
          ? {
            source: connectionState.toHandle.nodeId,
            sourceHandle: connectionState.toHandle.id ?? null,
            target: connectionState.fromHandle.nodeId,
            targetHandle: connectionState.fromHandle.id ?? null,
          }
          : {
            source: connectionState.fromHandle.nodeId,
            sourceHandle: connectionState.fromHandle.id ?? null,
            target: connectionState.toHandle.nodeId,
            targetHandle: connectionState.toHandle.id ?? null,
          }
        applyEditorConnection(connection)
      } else if (reconnectingEdgeRef.current && !reconnectHandledRef.current) {
        const reconnectingEdge = reconnectingEdgeRef.current
        const fallbackHandle = getHandleFromPointerEvent(event)
        const droppedHandle = connectionState.toHandle
          ? {
            nodeId: connectionState.toHandle.nodeId,
            handleId: connectionState.toHandle.id,
            handleType: connectionState.toHandle.type,
          }
          : fallbackHandle

        if (droppedHandle && connectionState.fromHandle) {
          const connection: Connection | null = connectionState.fromHandle.type === 'target' && droppedHandle.handleType === 'target'
            ? {
              source: reconnectingEdge.source,
              sourceHandle: reconnectingEdge.sourceHandle ?? null,
              target: droppedHandle.nodeId,
              targetHandle: droppedHandle.handleId ?? null,
            }
            : connectionState.fromHandle.type === 'source' && droppedHandle.handleType === 'source'
              ? {
                source: droppedHandle.nodeId,
                sourceHandle: droppedHandle.handleId ?? null,
                target: reconnectingEdge.target,
                targetHandle: reconnectingEdge.targetHandle ?? null,
              }
              : null

          if (connection) {
            applyEditorConnection(connection)
            reconnectHandledRef.current = false
            return
          }
        }

        restoreReconnectingEdge()
      } else if (!connectionState.isValid) {
        restoreReconnectingEdge()
      }
      reconnectHandledRef.current = false
    },
    [applyEditorConnection, restoreReconnectingEdge],
  )

  const onReconnectEnd = useMemo(
    () => () => {
      reconnectingEdgeRef.current = null
    },
    [],
  )

  const connectionLineComponent = useCallback(
    (props: ConnectionLineComponentProps<EditorNode>) => (
      <WorkflowReconnectLine
        {...props}
        reconnectingEdge={reconnectingEdgeRef.current}
      />
    ),
    [],
  )

  const isValidConnection = useMemo<IsValidConnection<Edge>>(
    () => (connection) => {
      const getSchema = (nodeId: string) => {
        const node = nodesRef.current.find((item) => item.id === nodeId)
        if (!node) return undefined
        return schemaMapRef.current.get(node.data.type)
      }
      return isValidEditorConnection(connection, getSchema)
    },
    [],
  )

  const persistGraph = useCallback(async (
    showNotice: boolean,
    options?: { throwOnError?: boolean },
  ): Promise<{ changed: boolean }> => {
    const currentWorkflowDef = workflowDefRef.current
    if (!currentWorkflowDef) return { changed: false }

    setIsSaving(true)
    if (showNotice) {
      setError(null)
      setNotice(null)
    }

    try {
      const graph = nodesToGraph(nodesRef.current, edgesRef.current, workflowNodesRef.current, schemaMapRef.current)
      const graphJson = JSON.stringify(graph, null, 2)
      const savedGraph = safeParseGraph(currentWorkflowDef.graph_json)
      // #region agent log
      sendAgentDebugLog({
        runId: 'pre-fix',
        hypothesisId: 'H3',
        location: 'WorkflowEditorPage.tsx:persistGraph:beforeUpdate',
        message: 'persist graph start',
        data: {
          showNotice,
          liveNodes: graph.nodes.length,
          liveEdges: graph.edges.length,
          savedNodesBefore: savedGraph.nodes.length,
          savedEdgesBefore: savedGraph.edges.length,
        },
      })
      // #endregion
      const response = await updateWorkflowDef(currentWorkflowDef.id, { graph_json: graphJson })
      const persistedGraphJson = response.data.graph_json
      const persistedGraph = safeParseGraph(persistedGraphJson)
      setWorkflowDef((prev) => (prev ? { ...prev, graph_json: persistedGraphJson } : prev))
      setWorkflowNodes(Object.fromEntries(persistedGraph.nodes.map((node) => [node.id, node])))
      // #region agent log
      sendAgentDebugLog({
        runId: 'pre-fix',
        hypothesisId: 'H3',
        location: 'WorkflowEditorPage.tsx:persistGraph:afterUpdate',
        message: 'persist graph success',
        data: {
          workflowDefId: currentWorkflowDef.id,
          savedNodesAfter: persistedGraph.nodes.length,
          savedEdgesAfter: persistedGraph.edges.length,
        },
      })
      // #endregion
      if (showNotice) setNotice('工作流已保存')
      return { changed: persistedGraphJson !== currentWorkflowDef.graph_json }
    } catch (saveError) {
      if (saveError instanceof ApiRequestError) {
        setError(saveError.message)
      } else {
        setError(saveError instanceof Error ? saveError.message : '保存失败')
      }
      if (options?.throwOnError) {
        throw saveError
      }
      return { changed: false }
    } finally {
      setIsSaving(false)
    }
  }, [setWorkflowDef, setWorkflowNodes])

  useEffect(() => {
    if (configAutoSaveTick === 0) return
    const timer = window.setTimeout(() => {
      void persistGraph(false)
    }, CONFIG_AUTO_SAVE_DEBOUNCE_MS)
    return () => window.clearTimeout(timer)
  }, [configAutoSaveTick, persistGraph])

  // ─── Context callbacks (stable via refs) ────────────────────────────────────

  const updateNode = useCallback(
    (nodeId: string, patch: Partial<WorkflowGraphNode> & { schemaType?: string }) => {
      setNodes((currentNodes) =>
        currentNodes.map((node) => {
          if (node.id !== nodeId) return node
          const nextType = patch.schemaType ?? node.data.type
          const nextSchema = schemaMapRef.current.get(nextType)
          return {
            ...node,
            data: {
              ...node.data,
              type: nextType,
              schema: nextSchema,
              label: patch.label ?? node.data.label,
              enabled: patch.enabled ?? node.data.enabled,
            },
          }
        }),
      )
      setWorkflowNodes((currentNodes) => {
        const previous = currentNodes[nodeId]
        if (!previous) return currentNodes
        return {
          ...currentNodes,
          [nodeId]: {
            ...previous,
            ...patch,
            type: patch.schemaType ?? previous.type,
          },
        }
      })
    },
    [setNodes, setWorkflowNodes],
  )

  const updateNodeConfig = useCallback(
    (nodeId: string, key: string, rawValue: string) => {
      setWorkflowNodes((currentNodes) => {
        const previous = currentNodes[nodeId]
        if (!previous) return currentNodes
        return {
          ...currentNodes,
          [nodeId]: {
            ...previous,
            config: { ...previous.config, [key]: parseConfigValue(rawValue) },
          },
        }
      })
      setConfigAutoSaveTick((value) => value + 1)
    },
    [setWorkflowNodes],
  )

  const removeNodeConfig = useCallback(
    (nodeId: string, key: string) => {
      setWorkflowNodes((currentNodes) => {
        const previous = currentNodes[nodeId]
        if (!previous) return currentNodes
        const nextConfig = { ...previous.config }
        delete nextConfig[key]
        return { ...currentNodes, [nodeId]: { ...previous, config: nextConfig } }
      })
      setConfigAutoSaveTick((value) => value + 1)
    },
    [setWorkflowNodes],
  )

  const addNodeConfig = useCallback(
    (nodeId: string, key: string, rawValue: string) => {
      setWorkflowNodes((currentNodes) => {
        const previous = currentNodes[nodeId]
        if (!previous) return currentNodes
        return {
          ...currentNodes,
          [nodeId]: {
            ...previous,
            config: { ...previous.config, [key]: parseConfigValue(rawValue) },
          },
        }
      })
      setConfigAutoSaveTick((value) => value + 1)
    },
    [setWorkflowNodes],
  )

  const deleteNode = useCallback(
    (nodeId: string) => {
      setNodes((currentNodes) => currentNodes.filter((n) => n.id !== nodeId))
      setEdges((currentEdges) =>
        currentEdges.filter((e) => e.source !== nodeId && e.target !== nodeId),
      )
      setWorkflowNodes((currentNodes) => {
        const next = { ...currentNodes }
        delete next[nodeId]
        return next
      })
    },
    [setNodes, setEdges, setWorkflowNodes],
  )

  const editorContextValue: EditorContextValue = useMemo(
    () => ({
      workflowNodes,
      schemas,
      updateNode,
      updateNodeConfig,
      removeNodeConfig,
      addNodeConfig,
      deleteNode,
      nodeRunByNodeId,
      rollbackingRunId,
      onRollbackRun: handleRollbackRun,
      onViewNodeError: openNodeErrorModal,
    }),
    [workflowNodes, schemas, updateNode, updateNodeConfig, removeNodeConfig, addNodeConfig, deleteNode, nodeRunByNodeId, rollbackingRunId, handleRollbackRun, openNodeErrorModal],
  )

  const getNewNodePosition = useCallback((nodeCount: number): EditorNode['position'] => {
    const fallbackPosition = {
      x: NODE_INSERT_FALLBACK_START.x + ((nodeCount % 3) * NODE_INSERT_FALLBACK_COLUMN_GAP),
      y: NODE_INSERT_FALLBACK_START.y + (Math.floor(nodeCount / 3) * NODE_INSERT_FALLBACK_ROW_GAP),
    }
    const container = canvasViewportRef.current
    const reactFlow = reactFlowRef.current
    if (!container || !reactFlow) return fallbackPosition

    const rect = container.getBoundingClientRect()
    if (rect.width <= 0 || rect.height <= 0) return fallbackPosition

    const centerPosition = reactFlow.screenToFlowPosition({
      x: rect.left + (rect.width / 2),
      y: rect.top + (rect.height / 2),
    })
    const offset = NODE_INSERT_STAGGER_OFFSETS[nodeCount % NODE_INSERT_STAGGER_OFFSETS.length]

    return {
      x: centerPosition.x - (NODE_INSERT_ESTIMATED_SIZE.width / 2) + offset.x,
      y: centerPosition.y - (NODE_INSERT_ESTIMATED_SIZE.height / 2) + offset.y,
    }
  }, [])

  // ─── Add node ────────────────────────────────────────────────────────────────

  function addNode(schema: NodeSchema) {
    setNodes((currentNodes) => {
      const position = getNewNodePosition(currentNodes.length)
      const nodeId = buildNodeId(schema.type, currentNodes)
      const nextNode: EditorNode = {
        id: nodeId,
        type: 'workflowNode',
        position,
        data: { label: schema.label, type: schema.type, enabled: true, schema },
      }
      setWorkflowNodes((currentWorkflowNodes) => ({
        ...currentWorkflowNodes,
        [nodeId]: {
          id: nodeId,
          type: schema.type,
          label: schema.label,
          config: {},
          inputs: {},
          ui_position: nextNode.position,
          enabled: true,
        },
      }))
      // #region agent log
      sendAgentDebugLog({
        runId: 'pre-fix',
        hypothesisId: 'H2',
        location: 'WorkflowEditorPage.tsx:addNode',
        message: 'node added in editor',
        data: {
          nodeId,
          nodeType: schema.type,
          nodesCountAfterAdd: currentNodes.length + 1,
        },
      })
      // #endregion
      return [...currentNodes, nextNode]
    })
  }

  function deleteSelectedEdge() {
    if (!selectedEdgeId) return
    setEdges((currentEdges) => currentEdges.filter((edge) => edge.id !== selectedEdgeId))
    setSelectedEdgeId(null)
  }

  async function handleSave() {
    await persistGraph(true)
  }

  async function handleRunWorkflow() {
    if (!workflowDefId || isRunning) return
    setIsRunning(true)
    setError(null)
    setNotice(null)
    try {
      let autoSaved = false
      try {
        const saveResult = await persistGraph(false, { throwOnError: true })
        autoSaved = saveResult.changed
      } catch (saveError) {
        if (saveError instanceof ApiRequestError) {
          setError(`运行前保存失败：${saveError.message}`)
        } else {
          const message = saveError instanceof Error ? saveError.message : '保存失败'
          setError(`运行前保存失败：${message}`)
        }
        return
      }

      const liveGraph = nodesToGraph(nodesRef.current, edgesRef.current, workflowNodesRef.current, schemaMapRef.current)
      const savedGraph = safeParseGraph(workflowDefRef.current?.graph_json ?? '')
      // #region agent log
      sendAgentDebugLog({
        runId: 'pre-fix',
        hypothesisId: 'H1',
        location: 'WorkflowEditorPage.tsx:handleRunWorkflow:beforeStart',
        message: 'run requested',
        data: {
          workflowDefId,
          isSaving,
          liveNodes: liveGraph.nodes.length,
          liveEdges: liveGraph.edges.length,
          savedNodes: savedGraph.nodes.length,
          savedEdges: savedGraph.edges.length,
        },
      })
      // #endregion
      const res = await startWorkflowJob({ workflow_def_id: workflowDefId })
      // #region agent log
      sendAgentDebugLog({
        runId: 'pre-fix',
        hypothesisId: 'H4',
        location: 'WorkflowEditorPage.tsx:handleRunWorkflow:afterStart',
        message: 'run started',
        data: {
          workflowDefId,
          jobId: res.job_id,
        },
      })
      // #endregion
      void bindLatestLaunch(workflowDefId, res.job_id)
      setNotice(autoSaved ? '检测到未保存改动，已自动保存并启动' : '工作流已启动')
    } catch (runError) {
      if (runError instanceof ApiRequestError) {
        setError(runError.message)
      } else {
        setError(runError instanceof Error ? runError.message : '启动失败')
      }
    } finally {
      setIsRunning(false)
    }
  }

  if (isLoading) {
    return (
      <div className="flex h-screen items-center justify-center text-sm font-black tracking-widest text-foreground">
        LOADING EDITOR...
      </div>
    )
  }

  return (
    <EditorContext.Provider value={editorContextValue}>
      {/* overflow-hidden ensures the page itself never scrolls */}
      <div className="flex h-screen overflow-hidden flex-col bg-background text-foreground">
        {/* ── Header ─────────────────────────────────────────────────────── */}
        <header className="shrink-0 border-b-2 border-foreground bg-primary text-primary-foreground">
          <div className="flex items-center justify-between gap-4 px-6 py-4">
            <div className="flex items-center gap-4">
              <button
                type="button"
                onClick={() => navigate('/workflow-defs')}
                className="inline-flex items-center gap-2 border-2 border-transparent px-3 py-2 text-sm font-bold transition-all hover:border-primary-foreground hover:bg-foreground hover:text-background"
              >
                <ArrowLeft className="h-4 w-4" />
                返回列表
              </button>
              <div>
                <p className="text-[10px] font-bold uppercase tracking-widest">
                  工作流编辑器
                </p>
                <h1 className="text-xl font-black tracking-tight">
                  {workflowDef?.name ?? '未命名工作流'}
                </h1>
              </div>
            </div>

            <div className="flex items-center gap-4">
              {error && <span className="text-sm font-bold text-red-300">{error}</span>}
              {notice && <span className="text-sm font-bold text-green-300">{notice}</span>}
              <button
                type="button"
                onClick={() => void handleRunWorkflow()}
                disabled={isRunning || isSaving}
                className="inline-flex items-center gap-2 border-2 border-foreground bg-background px-4 py-2 text-sm font-bold text-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-background disabled:hover:text-foreground disabled:hover:shadow-none disabled:hover:translate-y-0"
              >
                {isRunning ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                {isRunning ? '启动中...' : '运行'}
              </button>
              <button
                type="button"
                onClick={() => void handleSave()}
                disabled={isSaving}
                className="inline-flex items-center gap-2 border-2 border-foreground bg-foreground px-4 py-2 text-sm font-bold text-background transition-all hover:bg-background hover:text-foreground hover:shadow-hard hover:-translate-y-0.5 disabled:opacity-50 disabled:hover:bg-foreground disabled:hover:text-background disabled:hover:shadow-none disabled:hover:translate-y-0"
              >
                <Save className="h-4 w-4" />
                {isSaving ? '保存中...' : '保存工作流'}
              </button>
            </div>
          </div>
        </header>

        {editorRunCardView && (
          <div className="pointer-events-none fixed right-6 top-24 z-30 w-[360px] max-w-[calc(100vw-2rem)]">
            <div className="pointer-events-auto">
              <WorkflowRunStatusCard
                view={editorRunCardView}
                title="当前运行卡片"
                onOpenJobs={() => navigate(buildJobHistoryLink(editorRunCardView.jobId, editorRunCardView.workflowRunId))}
              />
            </div>
          </div>
        )}

        {/* ── Body ───────────────────────────────────────────────────────── */}
        <div className="flex min-h-0 flex-1 overflow-hidden">
          {/* ── Node panel sidebar (collapsible) ─────────────────────────── */}
          <aside
            className={cn(
              'relative shrink-0 border-r-2 border-foreground bg-card transition-[width] duration-300 ease-out',
              isNodePanelOpen ? 'w-72' : 'w-12',
            )}
          >
            {/* Toggle button */}
            <button
              type="button"
              onClick={() => setIsNodePanelOpen((open) => !open)}
              title={isNodePanelOpen ? '收起节点面板' : '展开节点面板'}
              className={cn(
                'absolute top-4 z-10 flex h-8 w-8 items-center justify-center border-2 border-foreground bg-background text-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard',
                isNodePanelOpen ? 'right-4' : 'left-2',
              )}
            >
              {isNodePanelOpen ? <ChevronLeft className="h-4 w-4" /> : <ChevronRight className="h-4 w-4" />}
            </button>

            {/* Panel content — only rendered when open */}
            {isNodePanelOpen && (
              <div className="flex h-full flex-col pt-16">
                <div className="border-b-2 border-foreground px-5 pb-4">
                  <div className="flex items-center gap-2 text-base font-black tracking-tight">
                    <Wand2 className="h-5 w-5 text-foreground" />
                    节点面板
                  </div>
                  <p className="mt-1 text-xs font-bold text-muted-foreground">点击节点将其添加到画布。</p>
                </div>
                <div className="flex-1 overflow-y-auto p-4 bg-muted/10">
                  {NODE_CATEGORIES.map((category) => {
                    const categorySchemas = schemas.filter((schema) => category.types.has(schema.type))
                    if (categorySchemas.length === 0) return null
                    return (
                      <div key={category.label} className="mb-6">
                        <div className="mb-3 flex items-center gap-2">
                          <span className={cn('h-2 w-2 rounded-full border-2 border-foreground', category.iconColor.replace('text-', 'bg-'))} />
                          <p className="text-xs font-black tracking-widest text-foreground">{category.label}</p>
                        </div>
                        <div className="space-y-2">
                          {categorySchemas.map((schema) => (
                            <button
                              key={schema.type}
                              type="button"
                              onClick={() => addNode(schema)}
                              className="w-full border-2 border-foreground bg-background px-4 py-3 text-left transition-all hover:-translate-y-0.5 hover:shadow-hard"
                            >
                              <div className="flex items-center justify-between gap-2">
                                <span className="text-sm font-bold">{schema.label}</span>
                                <Plus className="h-4 w-4 shrink-0 text-foreground" />
                              </div>
                              {schema.description && (
                                <p className="mt-1 text-[10px] font-medium text-muted-foreground line-clamp-2">
                                  {schema.description}
                                </p>
                              )}
                            </button>
                          ))}
                        </div>
                      </div>
                    )
                  })}
                  {/* Schemas not matched by any category */}
                  {(() => {
                    const knownTypes = new Set(NODE_CATEGORIES.flatMap((c) => [...c.types]))
                    const unknownSchemas = schemas.filter((schema) => !knownTypes.has(schema.type))
                    if (unknownSchemas.length === 0) return null
                    return (
                      <div className="mb-6">
                        <div className="mb-3 flex items-center gap-2">
                          <span className="h-2 w-2 rounded-full border-2 border-foreground bg-gray-400" />
                          <p className="text-xs font-black tracking-widest text-foreground">其他</p>
                        </div>
                        <div className="space-y-2">
                          {unknownSchemas.map((schema) => (
                            <button
                              key={schema.type}
                              type="button"
                              onClick={() => addNode(schema)}
                              className="w-full border-2 border-foreground bg-background px-4 py-3 text-left transition-all hover:-translate-y-0.5 hover:shadow-hard"
                            >
                              <div className="flex items-center justify-between gap-2">
                                <span className="text-sm font-bold">{schema.label}</span>
                                <Plus className="h-4 w-4 shrink-0 text-foreground" />
                              </div>
                            </button>
                          ))}
                        </div>
                      </div>
                    )
                  })()}
                </div>
              </div>
            )}
          </aside>

          {/* ── Canvas ───────────────────────────────────────────────────── */}
          <div ref={canvasViewportRef} className="relative min-w-0 flex-1">
            <button
              type="button"
              onClick={() => setSelectionModeOn((v) => !v)}
              title={selectionModeOn ? '切换为拖拽模式' : '切换为框选模式'}
              className={cn(
                'absolute right-6 top-6 z-10 flex items-center gap-2 border-2 px-4 py-2 text-xs font-bold transition-all hover:shadow-hard hover:-translate-y-0.5',
                selectionModeOn
                  ? 'border-foreground bg-foreground text-background'
                  : 'border-foreground bg-background text-foreground',
              )}
            >
              <MousePointer className="h-4 w-4" />
              {selectionModeOn ? '框选模式' : '拖拽模式'}
            </button>
            <ReactFlow
              nodes={nodes}
              edges={edges}
              nodeTypes={NODE_TYPES}
              onInit={(instance) => { reactFlowRef.current = instance }}
              onNodesChange={onNodesChange}
              onEdgesChange={onEdgesChange}
              onConnectStart={onConnectStart}
              onConnectEnd={onConnectEnd}
              onConnect={onConnect}
              onReconnect={onReconnect}
              onReconnectEnd={onReconnectEnd}
              isValidConnection={isValidConnection}
              edgesReconnectable
              onNodeClick={() => setSelectedEdgeId(null)}
              onPaneClick={() => setSelectedEdgeId(null)}
              onEdgeClick={(_, edge) => setSelectedEdgeId(edge.id)}
              fitView
              panOnScroll={!selectionModeOn}
              panOnScrollMode={PanOnScrollMode.Free}
              panOnDrag={!selectionModeOn}
              selectionOnDrag={selectionModeOn}
              selectionMode={SelectionMode.Partial}
              zoomOnScroll={false}
              zoomOnPinch
              preventScrolling
              colorMode={theme}
              className="bg-background"
              connectionLineStyle={{ strokeWidth: 3, stroke: 'hsl(var(--primary))' }}
              connectionLineComponent={connectionLineComponent}
              defaultEdgeOptions={{ style: { strokeWidth: 3, stroke: 'hsl(var(--foreground))' } }}
            >
              <Background gap={20} size={2} color="hsl(var(--foreground))" variant={BackgroundVariant.Dots} style={{ opacity: 0.15 }} />
              <MiniMap className="!bg-background/90 !border-2 !border-foreground" pannable zoomable />
              <Controls className="!border-2 !border-foreground !shadow-hard" />
            </ReactFlow>

            {/* Edge selected — delete bar */}
            {selectedEdgeId && (
              <div className="absolute bottom-6 left-6 z-10 border-2 border-foreground bg-card px-5 py-4 shadow-hard">
                <div className="flex items-center gap-6">
                  <div>
                    <p className="text-sm font-black">已选中连线</p>
                    <p className="text-xs font-bold text-muted-foreground mt-1">
                      删除后会同时清除目标节点上的 link_source。
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={deleteSelectedEdge}
                    className="border-2 border-red-900 bg-red-100 px-4 py-2 text-sm font-bold text-red-900 transition-all hover:bg-red-900 hover:text-red-100 hover:shadow-hard hover:-translate-y-0.5"
                  >
                    删除连线
                  </button>
                </div>
              </div>
            )}
          </div>
        </div>
      </div>
      <Modal
        open={nodeErrorModal !== null}
        title={nodeErrorModal ? `节点执行失败：${nodeErrorModal.nodeLabel}` : '节点执行失败'}
        description={nodeErrorModal ? `节点 ID：${nodeErrorModal.nodeId}` : undefined}
        onClose={() => setNodeErrorModal(null)}
        size="lg"
      >
        <div className="space-y-3">
          <p className="text-sm font-bold text-foreground">错误详情</p>
          <pre className="max-h-[50vh] overflow-auto whitespace-pre-wrap break-words border-2 border-red-900 bg-red-50 p-3 font-mono text-xs font-bold text-red-900">
            {nodeErrorModal?.error ?? ''}
          </pre>
        </div>
      </Modal>
    </EditorContext.Provider>
  )
}

// ─── Page entry ───────────────────────────────────────────────────────────────

export default function WorkflowEditorPage() {
  return (
    <ReactFlowProvider>
      <WorkflowEditorScreen />
    </ReactFlowProvider>
  )
}
