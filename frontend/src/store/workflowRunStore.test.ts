import { beforeEach, describe, expect, it, vi } from 'vitest'
import { act, createElement } from 'react'
import { createRoot } from 'react-dom/client'

import { getWorkflowRunDetail, listWorkflowRunReviews, listWorkflowRunsByJob } from '@/api/workflowRuns'
import { useWorkflowRunCardView, useWorkflowRunStore } from '@/store/workflowRunStore'
import type { NodeRun, PaginatedResponse, WorkflowRun, WorkflowRunDetail } from '@/types'

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

function makeNodeRun(overrides: Partial<NodeRun> = {}): NodeRun {
  return {
    id: 'node-run-1',
    workflow_run_id: 'run-1',
    node_id: 'node-1',
    node_type: 'compress-node',
    sequence: 1,
    status: 'running',
    input_json: '',
    output_json: '',
    error: '',
    started_at: '2026-01-01T00:00:00Z',
    finished_at: null,
    created_at: '2026-01-01T00:00:00Z',
    ...overrides,
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
    vi.mocked(listWorkflowRunReviews).mockResolvedValue({
      data: [],
      summary: {
        total: 0,
        pending: 0,
        approved: 0,
        rolled_back: 0,
        rejected: 0,
        failed_step_runs: 0,
      },
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

  it('node_progress 事件按 node_run_id 更新节点进度且不触发详情刷新', () => {
    const detailSpy = vi.spyOn(useWorkflowRunStore.getState(), 'fetchRunDetail')
    useWorkflowRunStore.setState({
      nodesByRunId: {
        'run-1': [{
          id: 'nr-1',
          workflow_run_id: 'run-1',
          node_id: 'node-1',
          node_type: 'compress-node',
          sequence: 1,
          status: 'running',
          input_json: '',
          output_json: '',
          error: '',
          started_at: null,
          finished_at: null,
          created_at: '2026-01-01T00:00:00Z',
        }],
      },
    })

    useWorkflowRunStore.getState().handleNodeProgress({
      job_id: 'job-1',
      workflow_run_id: 'run-1',
      node_run_id: 'nr-1',
      node_id: 'node-1',
      node_type: 'compress-node',
      percent: 75,
      done: 3,
      total: 4,
      stage: 'writing',
      message: '已打包 3/4',
      source_path: '/source/a.jpg',
      target_path: '/target/a.cbz',
    })

    const node = useWorkflowRunStore.getState().nodesByRunId['run-1'][0]
    expect(node.progress_percent).toBe(75)
    expect(node.progress_done).toBe(3)
    expect(node.progress_total).toBe(4)
    expect(node.progress_stage).toBe('writing')
    expect(node.progress_source_path).toBe('/source/a.jpg')
    expect(node.progress_target_path).toBe('/target/a.cbz')
    expect(node.sequence).toBe(1)
    expect(detailSpy).not.toHaveBeenCalled()
  })

  it('相同 node_id 但不同 node_run_id 的 node_progress 不会覆盖旧 node run', () => {
    useWorkflowRunStore.setState({
      nodesByRunId: {
        'run-1': [
          makeNodeRun({
            id: 'nr-old',
            node_id: 'node-1',
            sequence: 1,
            status: 'running',
            progress_percent: 15,
          }),
        ],
      },
    })

    useWorkflowRunStore.getState().handleNodeProgress({
      job_id: 'job-1',
      workflow_run_id: 'run-1',
      node_run_id: 'nr-new',
      node_id: 'node-1',
      node_type: 'compress-node',
      sequence: 2,
      percent: 35,
      done: 7,
      total: 20,
      stage: 'writing',
    })

    const nodes = useWorkflowRunStore.getState().nodesByRunId['run-1']
    expect(nodes).toHaveLength(2)
    const oldNode = nodes.find((node) => node.id === 'nr-old')
    const newNode = nodes.find((node) => node.id === 'nr-new')
    expect(oldNode?.progress_percent).toBe(15)
    expect(newNode?.progress_percent).toBe(35)
    expect(newNode?.sequence).toBe(2)
  })

  it('placeholder 先收到 node_started，后续带 node_run_id 的 node_progress 会回填同一条并保留 sequence', () => {
    useWorkflowRunStore.getState().handleNodeEvent({
      job_id: 'job-1',
      workflow_run_id: 'run-1',
      node_id: 'node-1',
      node_type: 'compress-node',
      sequence: 9,
      status: 'running',
    })

    useWorkflowRunStore.getState().handleNodeProgress({
      job_id: 'job-1',
      workflow_run_id: 'run-1',
      node_run_id: 'nr-1',
      node_id: 'node-1',
      node_type: 'compress-node',
      sequence: 9,
      percent: 60,
      done: 6,
      total: 10,
      stage: 'writing',
    })

    const nodes = useWorkflowRunStore.getState().nodesByRunId['run-1']
    expect(nodes).toHaveLength(1)
    expect(nodes[0].id).toBe('nr-1')
    expect(nodes[0].sequence).toBe(9)
    expect(nodes[0].progress_percent).toBe(60)
  })

  it('useWorkflowRunCardView 会在 nodesByRunId 更新后重新计算', () => {
    useWorkflowRunStore.setState({
      recentLaunchByScope: {
        'folder:folder-1:wf-1': {
          jobId: 'job-1',
          workflowRunId: 'run-1',
          updatedAt: new Date().toISOString(),
        },
      },
      runsById: {
        'run-1': makeRun({
          id: 'run-1',
          job_id: 'job-1',
          folder_id: 'folder-1',
          workflow_def_id: 'wf-1',
          status: 'running',
          last_node_id: 'node-1',
        }),
      },
      nodesByRunId: {
        'run-1': [
          makeNodeRun({
            id: 'nr-1',
            node_id: 'node-1',
            sequence: 1,
            status: 'running',
            progress_percent: 10,
            progress_done: 1,
            progress_total: 10,
          }),
        ],
      },
    })

    const views: Array<number | undefined> = []
    const container = document.createElement('div')
    const root = createRoot(container)

    function Probe() {
      const view = useWorkflowRunCardView('wf-1', 1, 'folder-1')
      views.push(view?.currentNodeProgressPercent)
      return null
    }

    act(() => {
      root.render(createElement(Probe))
    })

    act(() => {
      useWorkflowRunStore.setState((state) => ({
        nodesByRunId: {
          ...state.nodesByRunId,
          'run-1': [
            {
              ...state.nodesByRunId['run-1'][0],
              progress_percent: 55,
              progress_done: 5,
            },
          ],
        },
      }))
    })

    expect(views[views.length - 1]).toBe(55)

    act(() => {
      root.unmount()
    })
  })

  it('最终待确认事件会覆盖前面的节点刷新并拉取最新详情', async () => {
    vi.useFakeTimers()
    try {
      vi.mocked(listWorkflowRunReviews).mockResolvedValue({
        data: [],
        summary: {
          total: 1,
          pending: 1,
          approved: 0,
          rolled_back: 0,
          rejected: 0,
          failed_step_runs: 0,
        },
      })
      vi.mocked(getWorkflowRunDetail).mockResolvedValue({
        data: makeRun({
          id: 'run-1',
          status: 'waiting_input',
          last_node_id: '',
          resume_node_id: null,
        }),
        node_runs: [
          makeNodeRun({
            id: 'nr-compress',
            node_id: 'compress-node-13',
            status: 'succeeded',
            progress_percent: 100,
            progress_done: 141,
            progress_total: 141,
            progress_stage: 'completed',
            finished_at: '2026-01-01T00:01:00Z',
          }),
        ],
        review_summary: {
          total: 1,
          pending: 1,
          approved: 0,
          rolled_back: 0,
          rejected: 0,
          failed_step_runs: 0,
        },
      })

      useWorkflowRunStore.setState({
        runsById: {
          'run-1': makeRun({ status: 'running' }),
        },
        nodesByRunId: {
          'run-1': [
            makeNodeRun({
              id: 'nr-compress',
              node_id: 'compress-node-13',
              status: 'running',
              progress_percent: 0,
              progress_done: 0,
              progress_total: 141,
              progress_stage: 'discovering',
            }),
          ],
        },
      })

      useWorkflowRunStore.getState().handleNodeEvent({
        job_id: 'job-1',
        workflow_run_id: 'run-1',
        node_run_id: 'nr-compress',
        node_id: 'compress-node-13',
        node_type: 'compress-node',
        status: 'running',
      })
      await vi.advanceTimersByTimeAsync(200)

      useWorkflowRunStore.getState().handleRunUpdated({
        job_id: 'job-1',
        workflow_run_id: 'run-1',
        workflow_def_id: 'wf-1',
        folder_id: 'folder-1',
        status: 'waiting_input',
        last_node_id: '',
        resume_node_id: null,
      })

      await vi.advanceTimersByTimeAsync(349)
      expect(getWorkflowRunDetail).not.toHaveBeenCalled()

      await vi.advanceTimersByTimeAsync(1)
      await vi.waitFor(() => {
        expect(getWorkflowRunDetail).toHaveBeenCalledTimes(1)
      })

      const view = useWorkflowRunStore.getState().buildRunCardView('wf-1', 1, 'folder-1')
      expect(view?.status).toBe('waiting_input')
      expect(view?.currentNodeId).toBe('compress-node-13')
      expect(view?.completedNodes).toBe(1)
      expect(view?.currentNodeProgressPercent).toBe(100)
      expect(view?.currentNodeProgressDone).toBe(141)
      expect(view?.pendingReviewCount).toBe(1)
    } finally {
      vi.clearAllTimers()
      vi.useRealTimers()
    }
  })

  it('详情刷新进行中收到新刷新时会在结束后补拉一次', async () => {
    let resolveFirst: (value: WorkflowRunDetail) => void = () => undefined
    const firstDetail = new Promise<WorkflowRunDetail>((resolve) => {
      resolveFirst = resolve
    })

    vi.mocked(getWorkflowRunDetail)
      .mockReturnValueOnce(firstDetail)
      .mockResolvedValueOnce({
        data: makeRun({ status: 'waiting_input' }),
        node_runs: [],
      })

    const firstFetch = useWorkflowRunStore.getState().fetchRunDetail('run-1')
    void useWorkflowRunStore.getState().fetchRunDetail('run-1')

    expect(getWorkflowRunDetail).toHaveBeenCalledTimes(1)

    resolveFirst({
      data: makeRun({ status: 'running' }),
      node_runs: [],
    })
    await firstFetch

    await vi.waitFor(() => {
      expect(getWorkflowRunDetail).toHaveBeenCalledTimes(2)
    })
  })
})
