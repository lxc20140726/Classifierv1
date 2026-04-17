import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getFolder, listFolders, updateFolderStatus } from '@/api/folders'
import { notifyFolderActivityUpdated } from '@/lib/folderActivityEvents'
import { useFolderStore } from '@/store/folderStore'
import type { Folder } from '@/types'

vi.mock('@/api/folders', () => ({
  getFolder: vi.fn(),
  listFolders: vi.fn(),
  scanFolders: vi.fn(),
  suppressFolder: vi.fn(),
  unsuppressFolder: vi.fn(),
  updateFolderCategory: vi.fn(),
  updateFolderStatus: vi.fn(),
}))

vi.mock('@/lib/folderActivityEvents', () => ({
  notifyFolderActivityUpdated: vi.fn(),
}))

function makeFolder(overrides: Partial<Folder> = {}): Folder {
  return {
    id: 'folder-1',
    path: '/media/folder-1',
    source_dir: '/media',
    relative_path: 'folder-1',
    name: 'folder-1',
    category: 'photo',
    category_source: 'auto',
    status: 'pending',
    image_count: 1,
    video_count: 0,
    other_file_count: 0,
    has_other_files: false,
    total_files: 1,
    total_size: 100,
    marked_for_move: false,
    deleted_at: null,
    delete_staging_path: null,
    scanned_at: '2026-01-01T00:00:00Z',
    updated_at: '2026-01-01T00:00:00Z',
    workflow_summary: {
      classification: { status: 'not_run' },
      processing: { status: 'not_run' },
    },
    ...overrides,
  }
}

describe('folderStore', () => {
  beforeEach(() => {
    useFolderStore.setState(useFolderStore.getInitialState(), true)
    vi.clearAllMocks()
  })

  it('fetchFolders 成功后写入列表与分页信息', async () => {
    vi.mocked(listFolders).mockResolvedValue({
      data: [makeFolder()],
      total: 1,
      page: 1,
      limit: 20,
    })

    await useFolderStore.getState().fetchFolders()

    const state = useFolderStore.getState()
    expect(state.folders).toHaveLength(1)
    expect(state.total).toBe(1)
    expect(state.page).toBe(1)
    expect(state.limit).toBe(20)
    expect(state.error).toBeNull()
    expect(state.isLoading).toBe(false)
  })

  it('fetchFolders 失败时设置错误信息', async () => {
    vi.mocked(listFolders).mockRejectedValue(new Error('网络失败'))

    await useFolderStore.getState().fetchFolders()

    const state = useFolderStore.getState()
    expect(state.error).toBe('网络失败')
    expect(state.isLoading).toBe(false)
  })

  it('updateFolderStatus 成功时更新目标项并通知活动刷新', async () => {
    useFolderStore.setState({ folders: [makeFolder()] })
    vi.mocked(updateFolderStatus).mockResolvedValue({
      data: makeFolder({ status: 'done' }),
    })

    await useFolderStore.getState().updateFolderStatus('folder-1', 'done')

    const state = useFolderStore.getState()
    expect(state.folders[0]?.status).toBe('done')
    expect(notifyFolderActivityUpdated).toHaveBeenCalledTimes(1)
  })

  it('handleScanError 会累加 failed 并保留来源目录', () => {
    useFolderStore.setState({
      isScanning: true,
      scanProgress: {
        jobId: 'job-1',
        scanned: 1,
        total: 10,
        failed: 2,
        currentFolderName: 'old',
        sourceDirs: ['/source-a'],
      },
    })

    useFolderStore.getState().handleScanError({
      job_id: 'job-1',
      done: 3,
      total: 10,
      folder_name: 'new-folder',
      error: 'step failed',
    })

    const state = useFolderStore.getState()
    expect(state.scanProgress?.failed).toBe(3)
    expect(state.scanProgress?.currentFolderName).toBe('new-folder')
    expect(state.scanProgress?.sourceDirs).toEqual(['/source-a'])
    expect(state.error).toBe('step failed')
  })

  it('syncFolder 只替换现有行且不改变分页信息', async () => {
    useFolderStore.setState({
      folders: [makeFolder(), makeFolder({ id: 'folder-2', name: 'folder-2' })],
      total: 99,
      page: 3,
      limit: 30,
    })
    vi.mocked(getFolder).mockResolvedValue({
      data: makeFolder({ id: 'folder-1', status: 'done', name: 'folder-1-updated' }),
    })

    await useFolderStore.getState().syncFolder('folder-1')

    const state = useFolderStore.getState()
    expect(state.folders).toHaveLength(2)
    expect(state.folders[0]?.id).toBe('folder-1')
    expect(state.folders[0]?.status).toBe('done')
    expect(state.folders[0]?.name).toBe('folder-1-updated')
    expect(state.folders[1]?.id).toBe('folder-2')
    expect(state.total).toBe(99)
    expect(state.page).toBe(3)
    expect(state.limit).toBe(30)
    expect(state.isLoading).toBe(false)
  })

  it('syncFolder 目标不在当前列表时不插入新行', async () => {
    useFolderStore.setState({
      folders: [makeFolder({ id: 'folder-a' })],
    })
    vi.mocked(getFolder).mockResolvedValue({
      data: makeFolder({ id: 'folder-b', status: 'done' }),
    })

    await useFolderStore.getState().syncFolder('folder-b')

    const state = useFolderStore.getState()
    expect(state.folders).toHaveLength(1)
    expect(state.folders[0]?.id).toBe('folder-a')
  })

  it('syncFolder 同步后即使状态变更也保留当前行', async () => {
    useFolderStore.setState({
      folders: [makeFolder({ id: 'folder-1', status: 'pending' }), makeFolder({ id: 'folder-2' })],
    })
    vi.mocked(getFolder).mockResolvedValue({
      data: makeFolder({ id: 'folder-1', status: 'done' }),
    })

    await useFolderStore.getState().syncFolder('folder-1')

    const state = useFolderStore.getState()
    expect(state.folders.map((item) => item.id)).toEqual(['folder-1', 'folder-2'])
    expect(state.folders[0]?.status).toBe('done')
  })
})
