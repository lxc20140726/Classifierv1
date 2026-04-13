import type { WorkflowGraph, WorkflowGraphNode } from '@/types'

interface FolderPickerConfig {
  source_mode?: unknown
  saved_folder_id?: unknown
  saved_folder_ids?: unknown
  folder_ids?: unknown
  path?: unknown
  paths?: unknown
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function isGraphNodeCandidate(value: unknown): value is Record<string, unknown> {
  if (!isRecord(value)) return false
  return typeof value.id === 'string'
    && typeof value.type === 'string'
}

function normalizeGraphNode(value: unknown): WorkflowGraphNode | null {
  if (!isGraphNodeCandidate(value)) return null
  return {
    ...value,
    id: value.id as string,
    type: value.type as string,
    config: isRecord(value.config) ? value.config : {},
    enabled: typeof value.enabled === 'boolean' ? value.enabled : true,
  }
}

function parseGraphJson(graphJson: string): WorkflowGraph {
  let parsed: unknown
  try {
    parsed = JSON.parse(graphJson)
  } catch {
    throw new Error('工作流图 JSON 解析失败')
  }

  if (!isRecord(parsed)) {
    throw new Error('工作流图格式无效')
  }

  const { nodes, edges } = parsed
  if (!Array.isArray(nodes) || !Array.isArray(edges)) {
    throw new Error('工作流图格式无效')
  }

  const normalizedNodes = nodes
    .map((node) => normalizeGraphNode(node))
    .filter((node): node is WorkflowGraphNode => node !== null)

  return { nodes: normalizedNodes, edges }
}

function normalizeFolderIds(raw: unknown): string[] {
  if (!Array.isArray(raw)) return []
  return raw.filter((item): item is string => typeof item === 'string')
}

function normalizeFirstFolderID(config: FolderPickerConfig | undefined): string {
  if (!config) return ''
  if (typeof config.saved_folder_id === 'string') return config.saved_folder_id
  const saved = normalizeFolderIds(config.saved_folder_ids)
  if (saved.length > 0) return saved[0]
  const legacy = normalizeFolderIds(config.folder_ids)
  if (legacy.length > 0) return legacy[0]
  return ''
}

function isEnabledFolderPicker(node: WorkflowGraphNode): boolean {
  return node.type === 'folder-picker' && node.enabled
}

export interface FolderPickerLaunchCheckResult {
  enabledPickerCount: number
  initialSelectedFolderId: string
}

export function checkLaunchableFolderPickers(graphJson: string): FolderPickerLaunchCheckResult {
  const graph = parseGraphJson(graphJson)
  const enabledPickers = graph.nodes.filter(isEnabledFolderPicker)

  const initialSelectedFolderId = enabledPickers.length > 0
    ? normalizeFirstFolderID(enabledPickers[0].config as FolderPickerConfig | undefined)
    : ''

  return {
    enabledPickerCount: enabledPickers.length,
    initialSelectedFolderId,
  }
}

export function applyFolderSelectionToEnabledPickers(graphJson: string, selectedFolderId: string): string {
  const graph = parseGraphJson(graphJson)
  const enabledPickers = graph.nodes.filter(isEnabledFolderPicker)
  if (enabledPickers.length === 0) {
    throw new Error('该工作流缺少文件夹选择器节点，无法直接启动')
  }

  const normalizedFolderId = selectedFolderId.trim()
  if (normalizedFolderId === '') {
    throw new Error('请选择一条文件夹记录')
  }

  const nextNodes = graph.nodes.map((node) => {
    if (!isEnabledFolderPicker(node)) return node
    const currentConfig = isRecord(node.config) ? node.config : {}
    return {
      ...node,
      config: {
        ...currentConfig,
        source_mode: 'folders',
        saved_folder_id: normalizedFolderId,
        saved_folder_ids: [],
        folder_ids: [],
        path: '',
        paths: [],
      },
    }
  })

  return JSON.stringify({
    nodes: nextNodes,
    edges: graph.edges,
  })
}
