import { create } from 'zustand'

import { getFolderClassificationTree, listFolders } from '@/api/folders'
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

const LIVE_WINDOW_SIZE = 300

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

function upsertAndTrim(itemsByID: Record<string, LiveClassificationItem>): { itemsById: Record<string, LiveClassificationItem>; orderedIds: string[] } {
  const ordered = Object.values(itemsByID)
    .sort((a, b) => toMillis(b.last_event_at) - toMillis(a.last_event_at))
    .slice(0, LIVE_WINDOW_SIZE)

  const nextItemsById: Record<string, LiveClassificationItem> = {}
  const orderedIDs: string[] = []
  for (const item of ordered) {
    nextItemsById[item.folder_id] = item
    orderedIDs.push(item.folder_id)
  }

  return { itemsById: nextItemsById, orderedIds: orderedIDs }
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

interface LiveClassificationStore {
  itemsById: Record<string, LiveClassificationItem>
  orderedIds: string[]
  runToFolderId: Record<string, string>
  currentScanJobId: string
  selectedFolderId: string
  treeByFolderId: Record<string, FolderClassificationTreeEntry>
  treeLoadingFolderId: string
  treeError: string | null
  isLoading: boolean
  error: string | null
  loadInitial: () => Promise<void>
  selectFolder: (folderId: string) => Promise<void>
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
  orderedIds: [],
  runToFolderId: {},
  currentScanJobId: '',
  selectedFolderId: '',
  treeByFolderId: {},
  treeLoadingFolderId: '',
  treeError: null,
  isLoading: false,
  error: null,

  async loadInitial() {
    set({ isLoading: true, error: null })
    try {
      const response = await listFolders({ page: 1, limit: LIVE_WINDOW_SIZE, top_level_only: true })
      const itemsById: Record<string, LiveClassificationItem> = {}
      for (const folder of response.data) {
        itemsById[folder.id] = createItemFromFolder(folder)
      }

      const next = upsertAndTrim(itemsById)
      const runToFolderId: Record<string, string> = {}
      for (const item of Object.values(next.itemsById)) {
        if (item.workflow_run_id) {
          runToFolderId[item.workflow_run_id] = item.folder_id
        }
      }

      const defaultFolderID = get().selectedFolderId || next.orderedIds[0] || ''
      set({
        ...next,
        runToFolderId,
        selectedFolderId: defaultFolderID,
        isLoading: false,
      })
      if (defaultFolderID) {
        void get().loadTree(defaultFolderID)
      }
    } catch (error) {
      set({
        isLoading: false,
        error: error instanceof Error ? error.message : '加载实时分类目录失败',
      })
    }
  },

  async selectFolder(folderId) {
    const nextID = folderId.trim()
    set({ selectedFolderId: nextID })
    if (!nextID) return
    await get().loadTree(nextID)
  },

  async loadTree(folderId, force = false) {
    const targetID = folderId.trim()
    if (!targetID) return
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
        category: (payload.category as LiveClassificationItem['category']) ?? existing?.category ?? 'other',
        category_source: existing?.category_source ?? 'auto',
        classification_status: 'scanning',
        node_id: existing?.node_id ?? '',
        node_type: existing?.node_type ?? '',
        error: '',
        entered_at: existing?.entered_at ?? now,
        last_event_at: now,
      }
      const merged = { ...state.itemsById, [folderID]: nextItem }
      const next = upsertAndTrim(merged)
      const selectedFolderID = state.selectedFolderId || next.orderedIds[0] || ''
      return { ...next, selectedFolderId: selectedFolderID }
    })
  },

  handleScanError(payload) {
    const fallbackFolderID = payload.folder_path ? `scan_error:${payload.folder_path}` : ''
    const folderID = payload.folder_id ?? fallbackFolderID
    if (!folderID) return
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
      const merged = { ...state.itemsById, [folderID]: nextItem }
      const next = upsertAndTrim(merged)
      const selectedFolderID = state.selectedFolderId || next.orderedIds[0] || ''
      return { ...next, selectedFolderId: selectedFolderID }
    })
  },

  handleWorkflowNodeEvent(payload, status) {
    if (!payload.folder_id) return
    const folderID = payload.folder_id
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
      const merged = { ...state.itemsById, [folderID]: nextItem }
      const next = upsertAndTrim(merged)
      const runToFolderId = { ...state.runToFolderId }
      if (payload.workflow_run_id) {
        runToFolderId[payload.workflow_run_id] = folderID
      }
      return { ...next, runToFolderId }
    })

    if (get().selectedFolderId === folderID) {
      void get().loadTree(folderID, true)
    }
  },

  handleWorkflowRunUpdated(payload) {
    const folderID = payload.folder_id || get().runToFolderId[payload.workflow_run_id]
    if (!folderID) return

    set((state) => {
      const existing = state.itemsById[folderID]
      if (!existing) return state
      const now = new Date().toISOString()
      const nextItem: LiveClassificationItem = {
        ...existing,
        classification_status: mapRunStatusToLiveStatus(payload.status),
        node_id: payload.last_node_id ?? existing.node_id,
        error: payload.error ?? existing.error,
        last_event_at: now,
      }
      const merged = { ...state.itemsById, [folderID]: nextItem }
      const next = upsertAndTrim(merged)
      const runToFolderId = { ...state.runToFolderId }
      if (payload.workflow_run_id) {
        runToFolderId[payload.workflow_run_id] = folderID
      }
      return { ...next, runToFolderId }
    })

    if (get().selectedFolderId === folderID) {
      void get().loadTree(folderID, true)
    }
  },

  handleFolderClassificationUpdated(payload) {
    if (!payload.folder_id) return

    set((state) => {
      const existing = state.itemsById[payload.folder_id]
      const eventTime = payload.updated_at || new Date().toISOString()
      if (existing && !isNewer(eventTime, existing.last_event_at)) {
        return state
      }

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
        entered_at: existing?.entered_at ?? eventTime,
        last_event_at: eventTime,
      }

      const merged = { ...state.itemsById, [payload.folder_id]: nextItem }
      const next = upsertAndTrim(merged)
      const runToFolderId = { ...state.runToFolderId }
      if (payload.workflow_run_id) {
        runToFolderId[payload.workflow_run_id] = payload.folder_id
      }
      const selectedFolderID = state.selectedFolderId || next.orderedIds[0] || ''
      return { ...next, runToFolderId, selectedFolderId: selectedFolderID }
    })

    if (get().selectedFolderId === payload.folder_id) {
      void get().loadTree(payload.folder_id, true)
    }
  },
}))
