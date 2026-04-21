import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getFolder, getFolderClassificationTree } from '@/api/folders'
import { useLiveClassificationStore } from '@/store/liveClassificationStore'
import type { Folder, FolderClassificationTreeEntry } from '@/types'

vi.mock('@/api/folders', () => ({
  getFolder: vi.fn(),
  getFolderClassificationTree: vi.fn(),
}))

const getFolderMock = vi.mocked(getFolder)
const getFolderClassificationTreeMock = vi.mocked(getFolderClassificationTree)

const folder: Folder = {
  id: 'folder-1',
  path: 'E:/TEST/sample/yourpersonalwaifu',
  source_dir: 'E:/TEST/sample',
  relative_path: 'yourpersonalwaifu',
  name: 'yourpersonalwaifu',
  category: 'mixed',
  category_source: 'workflow',
  status: 'pending',
  image_count: 1,
  video_count: 1,
  other_file_count: 0,
  has_other_files: false,
  total_files: 2,
  total_size: 1024,
  marked_for_move: false,
  deleted_at: null,
  delete_staging_path: null,
  scanned_at: '2026-04-20T10:00:00.000Z',
  updated_at: '2026-04-20T10:00:00.000Z',
  workflow_summary: {
    classification: {
      status: 'running',
      workflow_run_id: 'run-1',
      job_id: 'job-1',
      updated_at: '2026-04-20T10:00:00.000Z',
    },
    processing: {
      status: 'not_run',
      workflow_run_id: undefined,
      job_id: undefined,
      updated_at: undefined,
    },
  },
}

const tree: FolderClassificationTreeEntry = {
  folder_id: 'folder-1',
  path: 'E:/TEST/sample/yourpersonalwaifu',
  name: 'yourpersonalwaifu',
  category: 'mixed',
  category_source: 'workflow',
  status: 'pending',
  has_other_files: false,
  total_files: 2,
  image_count: 1,
  video_count: 1,
  other_file_count: 0,
  files: [],
  subtree: [],
}

function resetStore() {
  useLiveClassificationStore.setState({
    itemsById: {},
    runToFolderId: {},
    currentScanJobId: '',
    focusFolderId: '',
    treeByFolderId: {},
    treeLoadingFolderId: '',
    treeError: null,
    isLoading: false,
    error: null,
  })
}

describe('liveClassificationStore', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    getFolderMock.mockResolvedValue({ data: folder })
    getFolderClassificationTreeMock.mockResolvedValue({ data: tree })
    resetStore()
  })

  it('仅处理焦点文件夹的实时事件', () => {
    const store = useLiveClassificationStore.getState()
    store.setFocusFolder('folder-1')

    store.handleWorkflowNodeEvent({
      job_id: 'job-2',
      workflow_run_id: 'run-2',
      folder_id: 'folder-2',
      node_id: 'writer',
      node_type: 'classification-writer',
    }, 'classifying')

    expect(useLiveClassificationStore.getState().itemsById['folder-2']).toBeUndefined()
  })

  it('清理焦点后恢复接收其他文件夹事件', () => {
    const store = useLiveClassificationStore.getState()
    store.setFocusFolder('folder-1')
    store.clearFocusFolder()

    store.handleScanProgress({
      job_id: 'job-3',
      folder_id: 'folder-2',
      folder_name: 'folder-2',
      folder_path: 'E:/TEST/sample/folder-2',
      source_dir: 'E:/TEST/sample',
      relative_path: 'folder-2',
      category: 'other',
      done: 1,
      total: 1,
    })

    expect(useLiveClassificationStore.getState().itemsById['folder-2']).toBeDefined()
  })

  it('按 folderId 初始化单文件夹实时状态', async () => {
    await useLiveClassificationStore.getState().loadFolderLiveState('folder-1')

    const state = useLiveClassificationStore.getState()
    expect(getFolderMock).toHaveBeenCalledWith('folder-1')
    expect(getFolderClassificationTreeMock).toHaveBeenCalledWith('folder-1')
    expect(state.itemsById['folder-1']).toBeDefined()
    expect(state.treeByFolderId['folder-1']).toBeDefined()
  })
})
