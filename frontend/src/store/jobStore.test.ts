import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getJobProgress } from '@/api/jobs'
import { notifyFolderActivityUpdated } from '@/lib/folderActivityEvents'
import { useJobStore } from '@/store/jobStore'

const syncFolderMock = vi.fn<(...args: unknown[]) => Promise<void>>()
const fetchFoldersMock = vi.fn<(...args: unknown[]) => Promise<void>>()
const fetchLogsMock = vi.fn<(...args: unknown[]) => Promise<void>>()

vi.mock('@/api/jobs', () => ({
  getJobProgress: vi.fn(),
  listJobs: vi.fn(),
}))

vi.mock('@/lib/folderActivityEvents', () => ({
  notifyFolderActivityUpdated: vi.fn(),
}))

vi.mock('@/store/folderStore', () => ({
  useFolderStore: {
    getState: () => ({
      syncFolder: syncFolderMock,
      fetchFolders: fetchFoldersMock,
      scanProgress: null,
      isScanning: false,
      handleScanDone: vi.fn(),
    }),
  },
}))

vi.mock('@/store/activityStore', () => ({
  useActivityStore: {
    getState: () => ({
      fetchLogs: fetchLogsMock,
    }),
  },
}))

describe('jobStore.startPolling', () => {
  beforeEach(() => {
    useJobStore.getState().stopAllPolling()
    useJobStore.setState(useJobStore.getInitialState(), true)
    vi.clearAllMocks()
    syncFolderMock.mockResolvedValue()
    fetchFoldersMock.mockResolvedValue()
    fetchLogsMock.mockResolvedValue()
  })

  it('工作流 waiting_input 只同步传入 folderIds，不整表刷新', async () => {
    vi.mocked(getJobProgress).mockResolvedValue({
      job_id: 'job-1',
      status: 'waiting_input',
      done: 1,
      total: 1,
      failed: 0,
      updated_at: '2026-01-01T00:00:00Z',
    })

    useJobStore.getState().startPolling('job-1', {
      jobType: 'workflow',
      folderIds: ['folder-1', 'folder-1', '  ', 'folder-2'],
    })

    await vi.waitFor(() => {
      expect(syncFolderMock).toHaveBeenCalledTimes(2)
    })
    expect(syncFolderMock).toHaveBeenNthCalledWith(1, 'folder-1')
    expect(syncFolderMock).toHaveBeenNthCalledWith(2, 'folder-2')
    expect(fetchFoldersMock).not.toHaveBeenCalled()
    expect(notifyFolderActivityUpdated).toHaveBeenCalledTimes(1)
  })

  it('工作流终态只同步传入 folderIds，不整表刷新', async () => {
    vi.mocked(getJobProgress).mockResolvedValue({
      job_id: 'job-2',
      status: 'succeeded',
      done: 1,
      total: 1,
      failed: 0,
      updated_at: '2026-01-01T00:00:00Z',
    })

    useJobStore.getState().startPolling('job-2', {
      jobType: 'workflow',
      folderIds: ['folder-9'],
    })

    await vi.waitFor(() => {
      expect(syncFolderMock).toHaveBeenCalledWith('folder-9')
    })
    expect(fetchFoldersMock).not.toHaveBeenCalled()
    expect(notifyFolderActivityUpdated).toHaveBeenCalledTimes(1)
  })

  it('缺少上下文时保持原行为，终态整表刷新', async () => {
    vi.mocked(getJobProgress).mockResolvedValue({
      job_id: 'job-3',
      status: 'succeeded',
      done: 1,
      total: 1,
      failed: 0,
      updated_at: '2026-01-01T00:00:00Z',
    })

    useJobStore.getState().startPolling('job-3')

    await vi.waitFor(() => {
      expect(fetchFoldersMock).toHaveBeenCalledTimes(1)
    })
    expect(syncFolderMock).not.toHaveBeenCalled()
    expect(notifyFolderActivityUpdated).toHaveBeenCalledTimes(1)
  })
})
