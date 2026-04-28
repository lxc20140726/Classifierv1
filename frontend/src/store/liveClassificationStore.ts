import { create } from 'zustand'

import { getFolder, getFolderClassificationTree } from '@/api/folders'
import type {
  Folder,
  FolderClassificationLiveEvent,
  FolderClassificationTreeEntry,
  LiveClassificationItem,
  LiveClassificationStatus,
  ScanProgressEvent,
  WorkflowNodeEvent,
  WorkflowRunUpdatedEvent,
} from '@/types'

function toMillis(value: string | undefined): number {
  if (!value) return 0
  const parsed = Date.parse(value)
  return Number.isNaN(parsed) ? 0 : parsed
}

function isNewer(next: string, prev: string): boolean {
  return toMillis(next) >= toMillis(prev)
}

function mapWorkflowStageToLiveStatus(stage: Folder['workflow_summary']['classification']['status']): LiveClassificationStatus {
  switch (stage) {
    case 'running':
      return 'classifying'
    case 'waiting_input':
      return 'waiting_input'
    case 'succeeded':
      return 'completed'
    case 'failed':
    case 'partial':
    case 'rolled_back':
    case 'cancelled':
      return 'failed'
    default:
      return 'completed'
  }
}

function mapRunStatusToLiveStatus(status: WorkflowRunUpdatedEvent['status']): LiveClassificationStatus {
  switch (status) {
    case 'running':
    case 'pending':
      return 'classifying'
    case 'waiting_input':
      return 'waiting_input'
    case 'succeeded':
      return 'completed'
    default:
      return 'failed'
  }
}

const CATEGORY_VALUES: ReadonlySet<LiveClassificationItem['category']> = new Set([
  'photo',
  'video',
  'mixed',
  'manga',
  'other',
])

function normalizeCategory(
  value: string | undefined,
  fallback: LiveClassificationItem['category'] | undefined,
): LiveClassificationItem['category'] {
  const normalized = value?.trim()
  if (normalized && CATEGORY_VALUES.has(normalized as LiveClassificationItem['category'])) {
    return normalized as LiveClassificationItem['category']
  }
  return fallback ?? 'other'
}

function createItemFromFolder(folder: Folder): LiveClassificationItem {
  return {
    folder_id: folder.id,
    job_id: folder.workflow_summary.classification.job_id ?? '',
    workflow_run_id: folder.workflow_summary.classification.workflow_run_id ?? '',
    folder_name: folder.name,
    folder_path: folder.path,
    source_dir: folder.source_dir,
    relative_path: folder.relative_path,
    category: folder.category,
    category_source: folder.category_source,
    classification_status: mapWorkflowStageToLiveStatus(folder.workflow_summary.classification.status),
    node_id: '',
    node_type: '',
    error: '',
    entered_at: folder.scanned_at || folder.updated_at || new Date().toISOString(),
    last_event_at: folder.updated_at || folder.scanned_at || new Date().toISOString(),
  }
}

function isFocusedFolder(focusFolderId: string, folderId: string): boolean {
  if (focusFolderId.trim() === '') return true
  return focusFolderId === folderId
}

interface LiveClassificationStore {
  itemsById: Record<string, LiveClassificationItem>
  runToFolderId: Record<string, string>
  currentScanJobId: string
  focusFolderId: string
  treeByFolderId: Record<string, FolderClassificationTreeEntry>
  treeLoadingFolderId: string
  treeError: string | null
  isLoading: boolean
  error: string | null
  setFocusFolder: (folderId: string) => void
  clearFocusFolder: () => void
  loadFolderLiveState: (folderId: string) => Promise<void>
  syncFolder: (folderId: string) => Promise<void>
  loadTree: (folderId: string, force?: boolean) => Promise<void>
  handleScanStarted: (payload: { job_id: string }) => void
  handleScanDone: () => void
  handleScanProgress: (payload: ScanProgressEvent) => void
  handleScanError: (payload: ScanProgressEvent) => void
  handleWorkflowNodeEvent: (payload: WorkflowNodeEvent, status: 'classifying' | 'waiting_input' | 'failed') => void
  handleWorkflowRunUpdated: (payload: WorkflowRunUpdatedEvent) => void
  handleFolderClassificationUpdated: (payload: FolderClassificationLiveEvent) => void
}

export const useLiveClassificationStore = create<LiveClassificationStore>((set, get) => ({
  itemsById: {},
  runToFolderId: {},
  currentScanJobId: '',
  focusFolderId: '',
  treeByFolderId: {},
  treeLoadingFolderId: '',
  treeError: null,
  isLoading: false,
  error: null,

  setFocusFolder(folderId) {
    const normalizedFolderID = folderId.trim()
    set((state) => ({
      focusFolderId: normalizedFolderID,
      itemsById: normalizedFolderID === '' ? state.itemsById : {},
      runToFolderId: normalizedFolderID === '' ? state.runToFolderId : {},
      treeError: null,
      error: null,
    }))
  },

  clearFocusFolder() {
    set({
      focusFolderId: '',
      currentScanJobId: '',
      treeLoadingFolderId: '',
      treeError: null,
      error: null,
    })
  },

  async loadFolderLiveState(folderId) {
    const targetID = folderId.trim()
    if (!targetID) {
      set({
        isLoading: false,
        error: '目录 ID 无效',
      })
      return
    }

    set({
      isLoading: true,
      error: null,
      treeError: null,
    })

    try {
      const response = await getFolder(targetID)
      set((state) => {
        const item = createItemFromFolder(response.data)
        const runToFolderId = { ...state.runToFolderId }
        if (item.workflow_run_id) {
          runToFolderId[item.workflow_run_id] = targetID
        }
        return {
          itemsById: {
            [targetID]: item,
          },
          runToFolderId,
        }
      })
      await get().loadTree(targetID, true)
    } catch (error) {
      set({
        error: error instanceof Error ? error.message : '加载实时分类目录失败',
      })
    } finally {
      set({ isLoading: false })
    }
  },

  async syncFolder(folderId) {
    const normalizedFolderID = folderId.trim()
    if (!normalizedFolderID) return

    const focusFolderID = get().focusFolderId
    if (!isFocusedFolder(focusFolderID, normalizedFolderID)) return

    try {
      const response = await getFolder(normalizedFolderID)
      set((state) => {
        const item = createItemFromFolder(response.data)
        const runToFolderId = { ...state.runToFolderId }
        const workflowRunID = response.data.workflow_summary.classification.workflow_run_id?.trim() ?? ''
        if (workflowRunID) {
          runToFolderId[workflowRunID] = normalizedFolderID
        }
        return {
          itemsById: {
            ...state.itemsById,
            [normalizedFolderID]: item,
          },
          runToFolderId,
        }
      })
    } catch {
      // Keep the live panel resilient when a single folder sync fails.
    }
  },

  async loadTree(folderId, force = false) {
    const targetID = folderId.trim()
    if (!targetID) return

    const focusFolderID = get().focusFolderId
    if (!isFocusedFolder(focusFolderID, targetID)) return
    if (!force && get().treeByFolderId[targetID]) return

    set({ treeLoadingFolderId: targetID, treeError: null })
    try {
      const response = await getFolderClassificationTree(targetID)
      set((state) => ({
        treeByFolderId: {
          ...state.treeByFolderId,
          [targetID]: response.data,
        },
        treeLoadingFolderId: state.treeLoadingFolderId === targetID ? '' : state.treeLoadingFolderId,
      }))
    } catch (error) {
      set((state) => ({
        treeError: error instanceof Error ? error.message : '加载目录分类树失败',
        treeLoadingFolderId: state.treeLoadingFolderId === targetID ? '' : state.treeLoadingFolderId,
      }))
    }
  },

  handleScanStarted(payload) {
    set({ currentScanJobId: payload.job_id ?? '' })
  },

  handleScanDone() {
    set({ currentScanJobId: '' })
  },

  handleScanProgress(payload) {
    if (!payload.folder_id) return
    const folderID = payload.folder_id

    if (!isFocusedFolder(get().focusFolderId, folderID)) return

    set((state) => {
      const now = new Date().toISOString()
      const existing = state.itemsById[folderID]
      const nextItem: LiveClassificationItem = {
        folder_id: folderID,
        job_id: payload.job_id ?? existing?.job_id ?? '',
        workflow_run_id: existing?.workflow_run_id ?? '',
        folder_name: payload.folder_name ?? existing?.folder_name ?? '',
        folder_path: payload.folder_path ?? existing?.folder_path ?? '',
        source_dir: payload.source_dir ?? existing?.source_dir ?? '',
        relative_path: payload.relative_path ?? existing?.relative_path ?? '',
        category: normalizeCategory(payload.category, existing?.category),
        category_source: existing?.category_source ?? 'auto',
        classification_status: 'scanning',
        node_id: existing?.node_id ?? '',
        node_type: existing?.node_type ?? '',
        error: '',
        entered_at: existing?.entered_at ?? now,
        last_event_at: now,
      }
      return {
        itemsById: {
          ...state.itemsById,
          [folderID]: nextItem,
        },
      }
    })
  },

  handleScanError(payload) {
    const fallbackFolderID = payload.folder_path ? `scan_error:${payload.folder_path}` : ''
    const folderID = payload.folder_id ?? fallbackFolderID
    if (!folderID) return

    if (!isFocusedFolder(get().focusFolderId, folderID)) return

    set((state) => {
      const now = new Date().toISOString()
      const existing = state.itemsById[folderID]
      const nextItem: LiveClassificationItem = {
        folder_id: folderID,
        job_id: payload.job_id ?? existing?.job_id ?? '',
        workflow_run_id: existing?.workflow_run_id ?? '',
        folder_name: payload.folder_name ?? existing?.folder_name ?? '',
        folder_path: payload.folder_path ?? existing?.folder_path ?? '',
        source_dir: payload.source_dir ?? existing?.source_dir ?? '',
        relative_path: payload.relative_path ?? existing?.relative_path ?? '',
        category: existing?.category ?? 'other',
        category_source: existing?.category_source ?? 'auto',
        classification_status: 'failed',
        node_id: existing?.node_id ?? '',
        node_type: existing?.node_type ?? '',
        error: payload.error ?? existing?.error ?? '',
        entered_at: existing?.entered_at ?? now,
        last_event_at: now,
      }
      return {
        itemsById: {
          ...state.itemsById,
          [folderID]: nextItem,
        },
      }
    })
  },

  handleWorkflowNodeEvent(payload, status) {
    if (!payload.folder_id) return
    const folderID = payload.folder_id

    if (!isFocusedFolder(get().focusFolderId, folderID)) return

    set((state) => {
      const now = new Date().toISOString()
      const existing = state.itemsById[folderID]
      const nextItem: LiveClassificationItem = {
        folder_id: folderID,
        job_id: payload.job_id ?? existing?.job_id ?? '',
        workflow_run_id: payload.workflow_run_id ?? existing?.workflow_run_id ?? '',
        folder_name: existing?.folder_name ?? '',
        folder_path: existing?.folder_path ?? '',
        source_dir: existing?.source_dir ?? '',
        relative_path: existing?.relative_path ?? '',
        category: existing?.category ?? 'other',
        category_source: existing?.category_source ?? 'auto',
        classification_status: status,
        node_id: payload.node_id ?? existing?.node_id ?? '',
        node_type: payload.node_type ?? existing?.node_type ?? '',
        error: payload.error ?? '',
        entered_at: existing?.entered_at ?? now,
        last_event_at: now,
      }
      const runToFolderId = { ...state.runToFolderId }
      if (payload.workflow_run_id) {
        runToFolderId[payload.workflow_run_id] = folderID
      }
      return {
        itemsById: {
          ...state.itemsById,
          [folderID]: nextItem,
        },
        runToFolderId,
      }
    })

    if (get().focusFolderId === folderID) {
      void get().loadTree(folderID, true)
    }
  },

  handleWorkflowRunUpdated(payload) {
    const state = get()
    const folderID = payload.folder_id || state.runToFolderId[payload.workflow_run_id]
    if (!folderID) return
    if (!isFocusedFolder(state.focusFolderId, folderID)) return

    set((currentState) => {
      const existing = currentState.itemsById[folderID]
      if (!existing) {
        return {
          runToFolderId: {
            ...currentState.runToFolderId,
            [payload.workflow_run_id]: folderID,
          },
        }
      }
      const now = new Date().toISOString()
      const nextItem: LiveClassificationItem = {
        ...existing,
        classification_status: mapRunStatusToLiveStatus(payload.status),
        node_id: payload.last_node_id ?? existing.node_id,
        error: payload.error ?? existing.error,
        last_event_at: now,
      }
      const runToFolderId = { ...currentState.runToFolderId }
      if (payload.workflow_run_id) {
        runToFolderId[payload.workflow_run_id] = folderID
      }
      return {
        itemsById: {
          ...currentState.itemsById,
          [folderID]: nextItem,
        },
        runToFolderId,
      }
    })

    const existingItem = get().itemsById[folderID]
    if (!existingItem) {
      void get().syncFolder(folderID)
    }

    if (get().focusFolderId === folderID) {
      void get().loadTree(folderID, true)
    }
  },

  handleFolderClassificationUpdated(payload) {
    if (!payload.folder_id) return
    if (!isFocusedFolder(get().focusFolderId, payload.folder_id)) return

    set((state) => {
      const existing = state.itemsById[payload.folder_id]
      const receivedAt = new Date().toISOString()
      const eventTime = payload.updated_at || receivedAt
      const existingRunID = existing?.workflow_run_id.trim() ?? ''
      const payloadRunID = payload.workflow_run_id?.trim() ?? ''
      const sameOrNewRun = payloadRunID !== '' && (existingRunID === '' || payloadRunID === existingRunID)
      if (existing && !sameOrNewRun && !isNewer(eventTime, existing.last_event_at)) {
        return state
      }
      const lastEventAt = existing && !isNewer(eventTime, existing.last_event_at) ? receivedAt : eventTime

      const nextItem: LiveClassificationItem = {
        folder_id: payload.folder_id,
        job_id: payload.job_id ?? existing?.job_id ?? '',
        workflow_run_id: payload.workflow_run_id ?? existing?.workflow_run_id ?? '',
        folder_name: payload.folder_name ?? existing?.folder_name ?? '',
        folder_path: payload.folder_path ?? existing?.folder_path ?? '',
        source_dir: payload.source_dir ?? existing?.source_dir ?? '',
        relative_path: payload.relative_path ?? existing?.relative_path ?? '',
        category: payload.category ?? existing?.category ?? 'other',
        category_source: payload.category_source ?? existing?.category_source ?? 'auto',
        classification_status: payload.classification_status ?? existing?.classification_status ?? 'classifying',
        node_id: payload.node_id ?? existing?.node_id ?? '',
        node_type: payload.node_type ?? existing?.node_type ?? '',
        error: payload.error ?? existing?.error ?? '',
        entered_at: existing?.entered_at ?? lastEventAt,
        last_event_at: lastEventAt,
      }

      const runToFolderId = { ...state.runToFolderId }
      if (payload.workflow_run_id) {
        runToFolderId[payload.workflow_run_id] = payload.folder_id
      }
      return {
        itemsById: {
          ...state.itemsById,
          [payload.folder_id]: nextItem,
        },
        runToFolderId,
      }
    })

    if (get().focusFolderId === payload.folder_id) {
      void get().loadTree(payload.folder_id, true)
    }
  },
}))
