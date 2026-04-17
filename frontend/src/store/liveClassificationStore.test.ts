import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getFolderClassificationTree } from '@/api/folders'
import { useLiveClassificationStore } from '@/store/liveClassificationStore'
import type { FolderClassificationTreeEntry } from '@/types'

vi.mock('@/api/folders', () => ({
  getFolderClassificationTree: vi.fn(),
  listFolders: vi.fn(),
}))

const getFolderClassificationTreeMock = vi.mocked(getFolderClassificationTree)

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
    orderedIds: [],
    runToFolderId: {},
    currentScanJobId: '',
    selectedFolderId: '',
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
    getFolderClassificationTreeMock.mockResolvedValue({ data: tree })
    resetStore()
  })

  it('节点事件创建首个实时项时自动选择并刷新分类树', async () => {
    useLiveClassificationStore.getState().handleWorkflowNodeEvent({
      job_id: 'job-1',
      workflow_run_id: 'run-1',
      folder_id: 'folder-1',
      node_id: 'writer',
      node_type: 'classification-writer',
    }, 'classifying')

    expect(useLiveClassificationStore.getState().selectedFolderId).toBe('folder-1')
    await vi.waitFor(() => {
      expect(getFolderClassificationTreeMock).toHaveBeenCalledWith('folder-1')
    })
  })

  it('同一工作流的最终分类事件不会被较早的服务端时间戳丢弃', async () => {
    useLiveClassificationStore.getState().handleWorkflowNodeEvent({
      job_id: 'job-1',
      workflow_run_id: 'run-1',
      folder_id: 'folder-1',
      node_id: 'writer',
      node_type: 'classification-writer',
    }, 'classifying')
    getFolderClassificationTreeMock.mockClear()

    useLiveClassificationStore.getState().handleFolderClassificationUpdated({
      folder_id: 'folder-1',
      job_id: 'job-1',
      workflow_run_id: 'run-1',
      folder_name: 'yourpersonalwaifu',
      folder_path: 'E:/TEST/sample/yourpersonalwaifu',
      source_dir: 'E:/TEST/sample',
      relative_path: 'yourpersonalwaifu',
      category: 'mixed',
      category_source: 'workflow',
      classification_status: 'completed',
      node_id: 'writer',
      node_type: 'classification-writer',
      updated_at: '2020-01-01T00:00:00.000Z',
    })

    const item = useLiveClassificationStore.getState().itemsById['folder-1']
    expect(item.folder_name).toBe('yourpersonalwaifu')
    expect(item.classification_status).toBe('completed')
    await vi.waitFor(() => {
      expect(getFolderClassificationTreeMock).toHaveBeenCalledWith('folder-1')
    })
  })
})
