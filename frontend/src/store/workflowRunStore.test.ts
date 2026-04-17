import { beforeEach, describe, expect, it, vi } from 'vitest'

import { getWorkflowRunDetail, listWorkflowRunsByJob } from '@/api/workflowRuns'
import { useWorkflowRunStore } from '@/store/workflowRunStore'
import type { PaginatedResponse, WorkflowRun } from '@/types'

vi.mock('@/api/jobs', () => ({
  listJobs: vi.fn(),
}))

vi.mock('@/api/workflowRuns', () => ({
  approveAllWorkflowRunPendingReviews: vi.fn(),
  approveWorkflowRunReview: vi.fn(),
  getWorkflowRunDetail: vi.fn(),
  listWorkflowRunReviews: vi.fn(),
  listWorkflowRunsByJob: vi.fn(),
  provideWorkflowRunInput: vi.fn(),
  provideWorkflowRunRawInput: vi.fn(),
  resumeWorkflowRun: vi.fn(),
  rollbackAllWorkflowRunPendingReviews: vi.fn(),
  rollbackWorkflowRun: vi.fn(),
  rollbackWorkflowRunReview: vi.fn(),
}))

function makeRun(overrides: Partial<WorkflowRun> = {}): WorkflowRun {
  return {
    id: 'run-1',
    job_id: 'job-1',
    folder_id: 'folder-1',
    source_dir: '/source',
    workflow_def_id: 'wf-1',
    status: 'running',
    resume_node_id: null,
    last_node_id: 'node-1',
    error: '',
    started_at: null,
    finished_at: null,
    created_at: '2026-01-01T00:00:00Z',
    updated_at: '2026-01-01T00:00:00Z',
    ...overrides,
  }
}

function makeRunPage(data: WorkflowRun[]): PaginatedResponse<WorkflowRun> {
  return {
    data,
    total: data.length,
    page: 1,
    limit: 100,
  }
}

describe('workflowRunStore', () => {
  beforeEach(() => {
    window.localStorage.clear()
    useWorkflowRunStore.setState(useWorkflowRunStore.getInitialState(), true)
    vi.clearAllMocks()
    vi.mocked(getWorkflowRunDetail).mockResolvedValue({
      data: makeRun(),
      node_runs: [],
    })
  })

  it('文件夹运行卡片不会回退显示全局旧运行', () => {
    useWorkflowRunStore.setState({
      recentLaunchByScope: {
        'global:wf-1': {
          jobId: 'job-a',
          workflowRunId: 'run-a',
          updatedAt: new Date().toISOString(),
        },
      },
      runsById: {
        'run-a': makeRun({
          id: 'run-a',
          job_id: 'job-a',
          folder_id: 'folder-a',
          workflow_def_id: 'wf-1',
          status: 'running',
        }),
      },
    })

    const folderView = useWorkflowRunStore.getState().buildRunCardView('wf-1', 3, 'folder-b')
    const globalView = useWorkflowRunStore.getState().buildRunCardView('wf-1', 3)

    expect(folderView).toBeNull()
    expect(globalView?.workflowRunId).toBe('run-a')
  })

  it('文件夹运行卡片会忽略 folder_id 不匹配的旧运行', () => {
    useWorkflowRunStore.setState({
      recentLaunchByScope: {
        'folder:folder-b:wf-1': {
          jobId: 'job-a',
          workflowRunId: 'run-a',
          updatedAt: new Date().toISOString(),
        },
      },
      runsById: {
        'run-a': makeRun({
          id: 'run-a',
          job_id: 'job-a',
          folder_id: 'folder-a',
          workflow_def_id: 'wf-1',
          status: 'running',
        }),
      },
    })

    const view = useWorkflowRunStore.getState().buildRunCardView('wf-1', 3, 'folder-b')

    expect(view).toBeNull()
  })

  it('启动新文件夹任务时不会继承旧 workflow_run_id', async () => {
    vi.mocked(listWorkflowRunsByJob).mockResolvedValue(makeRunPage([
      makeRun({
        id: 'run-b',
        job_id: 'job-b',
        folder_id: 'folder-b',
        workflow_def_id: 'wf-1',
        status: 'running',
      }),
    ]))

    useWorkflowRunStore.setState({
      recentLaunchByScope: {
        'global:wf-1': {
          jobId: 'job-a',
          workflowRunId: 'run-a',
          updatedAt: new Date().toISOString(),
        },
      },
    })

    const binding = useWorkflowRunStore.getState().bindLatestLaunch('wf-1', 'job-b', 'folder-b')
    const pendingRecord = useWorkflowRunStore.getState().recentLaunchByScope['folder:folder-b:wf-1']

    expect(pendingRecord?.jobId).toBe('job-b')
    expect(pendingRecord?.workflowRunId).toBeUndefined()

    await binding

    const boundRecord = useWorkflowRunStore.getState().recentLaunchByScope['folder:folder-b:wf-1']
    expect(boundRecord?.workflowRunId).toBe('run-b')
  })

  it('SSE 运行更新会记录 folder_id 并生成文件夹级卡片', () => {
    vi.useFakeTimers()
    try {
      useWorkflowRunStore.getState().handleRunUpdated({
        job_id: 'job-b',
        workflow_run_id: 'run-b',
        workflow_def_id: 'wf-1',
        folder_id: 'folder-b',
        status: 'running',
      })

      const state = useWorkflowRunStore.getState()
      const view = state.buildRunCardView('wf-1', 3, 'folder-b')

      expect(state.runsById['run-b']?.folder_id).toBe('folder-b')
      expect(view?.workflowRunId).toBe('run-b')
    } finally {
      vi.clearAllTimers()
      vi.useRealTimers()
    }
  })
})
