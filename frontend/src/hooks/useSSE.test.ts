import { act, createElement } from 'react'
import { createRoot, type Root } from 'react-dom/client'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { useSSE } from '@/hooks/useSSE'
import { notifyFolderActivityUpdated } from '@/lib/folderActivityEvents'

const folderStoreState = {
  handleScanStarted: vi.fn(),
  handleScanProgress: vi.fn(),
  handleScanError: vi.fn(),
  handleScanDone: vi.fn(),
  fetchFolders: vi.fn<() => Promise<void>>(),
  syncFolder: vi.fn<(folderId: string) => Promise<void>>(),
}

const jobStoreState = {
  handleJobProgress: vi.fn(),
  handleJobDone: vi.fn(),
  handleJobError: vi.fn(),
  fetchJobs: vi.fn<() => Promise<void>>(),
  stopAllPolling: vi.fn(),
}

const workflowRunStoreState = {
  handleNodeEvent: vi.fn(),
  handleNodeProgress: vi.fn(),
  handleRunUpdated: vi.fn(),
  handleReviewEvent: vi.fn(),
  runsById: {} as Record<string, { folder_id?: string }>,
}

const activityStoreState = {
  fetchLogs: vi.fn<() => Promise<void>>(),
}

const liveStoreState = {
  handleScanStarted: vi.fn(),
  handleScanProgress: vi.fn(),
  handleScanError: vi.fn(),
  handleScanDone: vi.fn(),
  handleWorkflowNodeEvent: vi.fn(),
  handleWorkflowRunUpdated: vi.fn(),
  handleFolderClassificationUpdated: vi.fn(),
}

const notificationStoreState = {
  pushNotification: vi.fn(),
}

vi.mock('@/lib/folderActivityEvents', () => ({
  notifyFolderActivityUpdated: vi.fn(),
}))

vi.mock('@/store/folderStore', () => ({
  useFolderStore: {
    getState: () => folderStoreState,
  },
}))

vi.mock('@/store/jobStore', () => ({
  useJobStore: {
    getState: () => jobStoreState,
  },
}))

vi.mock('@/store/workflowRunStore', () => ({
  useWorkflowRunStore: {
    getState: () => workflowRunStoreState,
  },
}))

vi.mock('@/store/activityStore', () => ({
  useActivityStore: {
    getState: () => activityStoreState,
  },
}))

vi.mock('@/store/liveClassificationStore', () => ({
  useLiveClassificationStore: {
    getState: () => liveStoreState,
  },
}))

vi.mock('@/store/notificationStore', () => ({
  useNotificationStore: {
    getState: () => notificationStoreState,
  },
}))

class MockEventSource {
  static instances: MockEventSource[] = []

  onerror: ((this: EventSource, ev: Event) => unknown) | null = null

  private readonly listeners = new Map<string, Array<(event: MessageEvent<string>) => void>>()

  constructor(_url: string) {
    MockEventSource.instances.push(this)
  }

  addEventListener(type: string, callback: EventListenerOrEventListenerObject) {
    const normalized = this.listeners.get(type) ?? []
    normalized.push(callback as (event: MessageEvent<string>) => void)
    this.listeners.set(type, normalized)
  }

  close() {}

  emit(type: string, payload: unknown) {
    const listeners = this.listeners.get(type) ?? []
    listeners.forEach((listener) => {
      listener({ data: JSON.stringify(payload) } as MessageEvent<string>)
    })
  }
}

function HookHost() {
  useSSE()
  return null
}

describe('useSSE', () => {
  let container: HTMLDivElement
  let root: Root

  beforeEach(() => {
    vi.clearAllMocks()
    folderStoreState.fetchFolders.mockResolvedValue()
    folderStoreState.syncFolder.mockResolvedValue()
    jobStoreState.fetchJobs.mockResolvedValue()
    activityStoreState.fetchLogs.mockResolvedValue()
    workflowRunStoreState.runsById = {}
    MockEventSource.instances = []
    vi.stubGlobal('EventSource', MockEventSource as unknown as typeof EventSource)

    container = document.createElement('div')
    document.body.appendChild(container)
    root = createRoot(container)
    act(() => {
      root.render(createElement(HookHost))
    })
  })

  afterEach(() => {
    act(() => {
      root.unmount()
    })
    container.remove()
    vi.unstubAllGlobals()
  })

  it('workflow_run.updated 终态时仅同步单条目录', async () => {
    const source = MockEventSource.instances[0]
    expect(source).toBeDefined()

    source.emit('workflow_run.updated', {
      job_id: 'job-1',
      workflow_run_id: 'run-1',
      workflow_def_id: 'wf-1',
      folder_id: 'folder-1',
      status: 'succeeded',
    })

    await vi.waitFor(() => {
      expect(folderStoreState.syncFolder).toHaveBeenCalledWith('folder-1')
    })
    expect(folderStoreState.fetchFolders).not.toHaveBeenCalled()
  })

  it('workflow_run.updated 缺少 folder_id 时不触发整表刷新', async () => {
    const source = MockEventSource.instances[0]
    expect(source).toBeDefined()

    source.emit('workflow_run.updated', {
      job_id: 'job-2',
      workflow_run_id: 'run-2',
      workflow_def_id: 'wf-2',
      status: 'failed',
    })

    await vi.waitFor(() => {
      expect(workflowRunStoreState.handleRunUpdated).toHaveBeenCalledTimes(1)
    })
    expect(folderStoreState.syncFolder).not.toHaveBeenCalled()
    expect(folderStoreState.fetchFolders).not.toHaveBeenCalled()
    expect(notifyFolderActivityUpdated).not.toHaveBeenCalled()
  })

  it('workflow_run.review_pending 有本地 folder_id 时仅同步单条目录', async () => {
    workflowRunStoreState.runsById = {
      'run-3': { folder_id: 'folder-3' },
    }
    const source = MockEventSource.instances[0]
    expect(source).toBeDefined()

    source.emit('workflow_run.review_pending', { workflow_run_id: 'run-3' })

    await vi.waitFor(() => {
      expect(folderStoreState.syncFolder).toHaveBeenCalledWith('folder-3')
    })
    expect(jobStoreState.fetchJobs).toHaveBeenCalledTimes(1)
    expect(folderStoreState.fetchFolders).not.toHaveBeenCalled()
  })

  it('workflow_run.review_updated 无本地 folder_id 时只刷新运行与作业', async () => {
    const source = MockEventSource.instances[0]
    expect(source).toBeDefined()

    source.emit('workflow_run.review_updated', { workflow_run_id: 'run-4' })

    await vi.waitFor(() => {
      expect(workflowRunStoreState.handleReviewEvent).toHaveBeenCalledWith('run-4')
    })
    expect(jobStoreState.fetchJobs).toHaveBeenCalledTimes(1)
    expect(folderStoreState.syncFolder).not.toHaveBeenCalled()
    expect(folderStoreState.fetchFolders).not.toHaveBeenCalled()
  })
})
