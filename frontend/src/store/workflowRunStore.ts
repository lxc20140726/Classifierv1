import { create } from 'zustand'

import { listJobs } from '@/api/jobs'
import {
  approveAllWorkflowRunPendingReviews,
  approveWorkflowRunReview,
  getWorkflowRunDetail,
  listWorkflowRunReviews,
  listWorkflowRunsByJob,
  provideWorkflowRunInput,
  provideWorkflowRunRawInput,
  rollbackAllWorkflowRunPendingReviews,
  resumeWorkflowRun,
  rollbackWorkflowRun,
  rollbackWorkflowRunReview,
} from '@/api/workflowRuns'
import type {
  NodeRun,
  NodeRunStatus,
  NodeType,
  PaginatedResponse,
  ProcessingReviewItem,
  ProcessingReviewSummary,
  ProvideInputBody,
  WorkflowNodeEvent,
  WorkflowRun,
  WorkflowRunStatus,
  WorkflowRunUpdatedEvent,
} from '@/types'

interface RecentLaunchRecord {
  jobId: string
  workflowRunId?: string
  updatedAt: string
}

export interface WorkflowRunCardView {
  workflowDefId: string
  jobId: string
  workflowRunId: string
  status: WorkflowRunStatus
  currentNodeId: string
  currentNodeType: string
  completedNodes: number
  totalNodes: number
  failureSummary: string
  reviewSummary?: ProcessingReviewSummary
  reviewProgressText: string
  isBinding: boolean
}

interface WorkflowRunStore {
  runsByJobId: Record<string, WorkflowRun[]>
  runsTotalByJobId: Record<string, number>
  runsPageByJobId: Record<string, number>
  runsLimitByJobId: Record<string, number>
  runsById: Record<string, WorkflowRun>
  nodesByRunId: Record<string, NodeRun[]>
  reviewsByRunId: Record<string, ProcessingReviewItem[]>
  reviewSummaryByRunId: Record<string, ProcessingReviewSummary>
  recentLaunchByScope: Record<string, RecentLaunchRecord>
  fetchingJobIds: Set<string>
  fetchingRunIds: Set<string>
  fetchRunsForJob: (jobId: string, params?: { page?: number; limit?: number }) => Promise<void>
  fetchRunDetail: (runId: string) => Promise<void>
  fetchRunReviews: (runId: string) => Promise<void>
  approveReview: (runId: string, reviewId: string) => Promise<void>
  rollbackReview: (runId: string, reviewId: string) => Promise<void>
  approveAllPendingReviews: (runId: string) => Promise<number>
  rollbackAllPendingReviews: (runId: string) => Promise<number>
  resumeRun: (runId: string) => Promise<void>
  rollbackRun: (runId: string) => Promise<void>
  provideInput: (runId: string, category: ProvideInputBody['category']) => Promise<void>
  provideRawInput: (runId: string, body: Record<string, unknown>) => Promise<void>
  bindLatestLaunch: (workflowDefId: string, jobId: string, folderId?: string) => Promise<void>
  restoreLatestLaunch: (workflowDefId: string, folderId?: string) => Promise<void>
  handleRunUpdated: (event: WorkflowRunUpdatedEvent) => void
  handleReviewEvent: (workflowRunId: string) => void
  refreshRunFromEvent: (runId: string) => void
  handleNodeEvent: (event: WorkflowNodeEvent) => void
  buildRunCardView: (workflowDefId: string, totalNodes: number, folderId?: string) => WorkflowRunCardView | null
}

const RECENT_LAUNCH_STORAGE_KEY = 'classifier-workflow-recent-launch-v2'
const RUN_REFRESH_DEBOUNCE_MS = 350

const TERMINAL_NODE_STATUSES = new Set<NodeRunStatus>(['succeeded', 'failed', 'skipped', 'waiting_input'])
const ACTIVE_WORKFLOW_RUN_STATUSES = new Set<WorkflowRunStatus>(['pending', 'running', 'waiting_input'])
const RECENT_BINDING_MAX_AGE_MS = 30 * 1000

const refreshTimers = new Map<string, number>()

function delay(ms: number) {
  return new Promise<void>((resolve) => {
    window.setTimeout(resolve, ms)
  })
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function parseRecentLaunchRecord(value: unknown): RecentLaunchRecord | null {
  if (!isRecord(value)) return null
  if (typeof value.jobId !== 'string' || value.jobId.trim() === '') return null
  if (typeof value.updatedAt !== 'string' || value.updatedAt.trim() === '') return null
  const workflowRunId = typeof value.workflowRunId === 'string' && value.workflowRunId.trim() !== ''
    ? value.workflowRunId
    : undefined
  return {
    jobId: value.jobId,
    workflowRunId,
    updatedAt: value.updatedAt,
  }
}

function loadRecentLaunches(): Record<string, RecentLaunchRecord> {
  if (typeof window === 'undefined') return {}
  try {
    const raw = window.localStorage.getItem(RECENT_LAUNCH_STORAGE_KEY)
    if (!raw) return {}
    const parsed = JSON.parse(raw) as unknown
    if (!isRecord(parsed)) return {}

    const out: Record<string, RecentLaunchRecord> = {}
    for (const [workflowDefId, value] of Object.entries(parsed)) {
      if (typeof workflowDefId !== 'string' || workflowDefId.trim() === '') continue
      const record = parseRecentLaunchRecord(value)
      if (!record) continue
      out[workflowDefId] = record
    }
    return out
  } catch {
    return {}
  }
}

function persistRecentLaunches(next: Record<string, RecentLaunchRecord>) {
  if (typeof window === 'undefined') return
  try {
    window.localStorage.setItem(RECENT_LAUNCH_STORAGE_KEY, JSON.stringify(next))
  } catch {
    // 忽略持久化异常，避免影响主流程
  }
}

function buildRecentLaunchScopeKey(workflowDefId: string, folderId?: string) {
  const normalizedWorkflowDefId = workflowDefId.trim()
  const normalizedFolderId = folderId?.trim() ?? ''
  return normalizedFolderId === ''
    ? `global:${normalizedWorkflowDefId}`
    : `folder:${normalizedFolderId}:${normalizedWorkflowDefId}`
}

function getRecentLaunchRecord(
  recentLaunchByScope: Record<string, RecentLaunchRecord>,
  workflowDefId: string,
  folderId?: string,
) {
  const normalizedFolderId = folderId?.trim() ?? ''
  if (normalizedFolderId !== '') {
    const scopedRecord = recentLaunchByScope[buildRecentLaunchScopeKey(workflowDefId, normalizedFolderId)]
    if (scopedRecord) return scopedRecord
    return undefined
  }

  return recentLaunchByScope[buildRecentLaunchScopeKey(workflowDefId)]
}

function setRecentLaunchRecord(
  recentLaunchByScope: Record<string, RecentLaunchRecord>,
  workflowDefId: string,
  jobId: string,
  workflowRunId?: string,
  folderId?: string,
) {
  const updatedAt = new Date().toISOString()
  const nextRecent = {
    ...recentLaunchByScope,
    [buildRecentLaunchScopeKey(workflowDefId)]: {
      jobId,
      workflowRunId,
      updatedAt,
    },
  }

  const normalizedFolderId = folderId?.trim() ?? ''
  if (normalizedFolderId !== '') {
    nextRecent[buildRecentLaunchScopeKey(workflowDefId, normalizedFolderId)] = {
      jobId,
      workflowRunId,
      updatedAt,
    }
  }

  return nextRecent
}

function sortRunsDesc(runs: WorkflowRun[]) {
  return [...runs].sort((a, b) => {
    const aTime = Date.parse(a.updated_at || a.created_at || '')
    const bTime = Date.parse(b.updated_at || b.created_at || '')
    if (!Number.isNaN(aTime) && !Number.isNaN(bTime) && aTime !== bTime) {
      return bTime - aTime
    }
    return b.created_at.localeCompare(a.created_at)
  })
}

function upsertRunByJob(currentRuns: WorkflowRun[], run: WorkflowRun) {
  const idx = currentRuns.findIndex((item) => item.id === run.id)
  if (idx === -1) {
    return sortRunsDesc([...currentRuns, run])
  }
  return sortRunsDesc(currentRuns.map((item, index) => (index === idx ? run : item)))
}

function chooseCurrentNode(run: WorkflowRun, nodeRuns: NodeRun[]) {
  const runningNode = [...nodeRuns]
    .filter((nodeRun) => nodeRun.status === 'running')
    .sort((a, b) => b.sequence - a.sequence)[0]
  if (runningNode) return runningNode

  const waitingNode = [...nodeRuns]
    .filter((nodeRun) => nodeRun.status === 'waiting_input')
    .sort((a, b) => b.sequence - a.sequence)[0]
  if (waitingNode) return waitingNode

  const lastNodeID = (run.last_node_id ?? '').trim()
  if (lastNodeID !== '') {
    const matched = [...nodeRuns]
      .filter((nodeRun) => nodeRun.node_id === lastNodeID)
      .sort((a, b) => b.sequence - a.sequence)[0]
    if (matched) return matched
  }

  return [...nodeRuns].sort((a, b) => b.sequence - a.sequence)[0] ?? null
}

function scheduleRunRefresh(runId: string, runFetch: (runID: string) => Promise<void>) {
  if (!runId || refreshTimers.has(runId)) return
  const timer = window.setTimeout(() => {
    refreshTimers.delete(runId)
    void runFetch(runId)
  }, RUN_REFRESH_DEBOUNCE_MS)
  refreshTimers.set(runId, timer)
}

function isRecentBindingRecord(record: RecentLaunchRecord) {
  const updatedAtTs = Date.parse(record.updatedAt)
  if (Number.isNaN(updatedAtTs)) return false
  return Date.now() - updatedAtTs <= RECENT_BINDING_MAX_AGE_MS
}

const initialRecentLaunches = loadRecentLaunches()

export const useWorkflowRunStore = create<WorkflowRunStore>((set, get) => ({
  runsByJobId: {},
  runsTotalByJobId: {},
  runsPageByJobId: {},
  runsLimitByJobId: {},
  runsById: {},
  nodesByRunId: {},
  reviewsByRunId: {},
  reviewSummaryByRunId: {},
  recentLaunchByScope: initialRecentLaunches,
  fetchingJobIds: new Set(),
  fetchingRunIds: new Set(),

  async fetchRunsForJob(jobId, params = {}) {
    if (!jobId || get().fetchingJobIds.has(jobId)) return
    set((state) => ({ fetchingJobIds: new Set([...state.fetchingJobIds, jobId]) }))
    try {
      const existingPage = get().runsPageByJobId[jobId] ?? 1
      const existingLimit = get().runsLimitByJobId[jobId] ?? 100
      const page = params.page ?? existingPage
      const limit = params.limit ?? existingLimit
      const response: PaginatedResponse<WorkflowRun> = await listWorkflowRunsByJob(jobId, { page, limit })
      set((state) => {
        const nextRunsById = { ...state.runsById }
        response.data.forEach((run) => {
          nextRunsById[run.id] = run
        })

        let nextRecent = state.recentLaunchByScope
        let recentChanged = false
        response.data.forEach((run) => {
          const workflowDefId = run.workflow_def_id?.trim()
          if (!workflowDefId) return
          const scopeKey = buildRecentLaunchScopeKey(workflowDefId, run.folder_id)
          const current = nextRecent[scopeKey]
          if (!current || current.jobId === jobId) {
            nextRecent = setRecentLaunchRecord(nextRecent, workflowDefId, jobId, run.id, run.folder_id)
            recentChanged = true
          }
        })

        if (recentChanged) {
          persistRecentLaunches(nextRecent)
        }

        return {
          runsByJobId: { ...state.runsByJobId, [jobId]: sortRunsDesc(response.data) },
          runsTotalByJobId: { ...state.runsTotalByJobId, [jobId]: response.total },
          runsPageByJobId: { ...state.runsPageByJobId, [jobId]: response.page },
          runsLimitByJobId: { ...state.runsLimitByJobId, [jobId]: response.limit },
          runsById: nextRunsById,
          recentLaunchByScope: nextRecent,
          fetchingJobIds: new Set([...state.fetchingJobIds].filter((id) => id !== jobId)),
        }
      })
    } catch (error) {
      console.error(`fetchRunsForJob ${jobId}:`, error)
      set((state) => ({
        fetchingJobIds: new Set([...state.fetchingJobIds].filter((id) => id !== jobId)),
      }))
    }
  },

  async fetchRunDetail(runId) {
    if (!runId || get().fetchingRunIds.has(runId)) return
    set((state) => ({ fetchingRunIds: new Set([...state.fetchingRunIds, runId]) }))
    try {
      const response = await getWorkflowRunDetail(runId)
      const run = response.data
      const jobId = run.job_id

      set((state) => {
        const currentRuns = state.runsByJobId[jobId] ?? []
        const updatedRuns = upsertRunByJob(currentRuns, run)

        const nextRunsById = { ...state.runsById, [run.id]: run }

        const workflowDefId = run.workflow_def_id?.trim()
        let nextRecent = state.recentLaunchByScope
        if (workflowDefId) {
          nextRecent = setRecentLaunchRecord(nextRecent, workflowDefId, run.job_id, run.id, run.folder_id)
          persistRecentLaunches(nextRecent)
        }

        const nextReviewSummaryByRunId = { ...state.reviewSummaryByRunId }
        if (response.review_summary) {
          nextReviewSummaryByRunId[runId] = response.review_summary
        } else {
          delete nextReviewSummaryByRunId[runId]
        }

        return {
          runsByJobId: { ...state.runsByJobId, [jobId]: updatedRuns },
          runsById: nextRunsById,
          nodesByRunId: { ...state.nodesByRunId, [runId]: response.node_runs },
          reviewSummaryByRunId: nextReviewSummaryByRunId,
          recentLaunchByScope: nextRecent,
          fetchingRunIds: new Set([...state.fetchingRunIds].filter((id) => id !== runId)),
        }
      })

      if (run.status === 'waiting_input') {
        void get().fetchRunReviews(runId)
      }
    } catch (error) {
      console.error(`fetchRunDetail ${runId}:`, error)
      set((state) => ({
        fetchingRunIds: new Set([...state.fetchingRunIds].filter((id) => id !== runId)),
      }))
    }
  },

  async fetchRunReviews(runId) {
    if (!runId) return
    const response = await listWorkflowRunReviews(runId)
    set((state) => ({
      reviewsByRunId: { ...state.reviewsByRunId, [runId]: response.data },
      reviewSummaryByRunId: { ...state.reviewSummaryByRunId, [runId]: response.summary },
    }))
  },

  async approveReview(runId, reviewId) {
    await approveWorkflowRunReview(runId, reviewId)
    await Promise.all([get().fetchRunDetail(runId), get().fetchRunReviews(runId)])
  },

  async rollbackReview(runId, reviewId) {
    await rollbackWorkflowRunReview(runId, reviewId)
    await Promise.all([get().fetchRunDetail(runId), get().fetchRunReviews(runId)])
  },

  async approveAllPendingReviews(runId) {
    const response = await approveAllWorkflowRunPendingReviews(runId)
    await Promise.all([get().fetchRunDetail(runId), get().fetchRunReviews(runId)])
    return response.approved
  },

  async rollbackAllPendingReviews(runId) {
    const response = await rollbackAllWorkflowRunPendingReviews(runId)
    await Promise.all([get().fetchRunDetail(runId), get().fetchRunReviews(runId)])
    return response.rolled_back
  },

  async resumeRun(runId) {
    await resumeWorkflowRun(runId)
  },

  async rollbackRun(runId) {
    await rollbackWorkflowRun(runId)
    await get().fetchRunDetail(runId)
  },

  async provideInput(runId, category) {
    await provideWorkflowRunInput(runId, { category })
    await get().fetchRunDetail(runId)
  },

  async provideRawInput(runId, body) {
    await provideWorkflowRunRawInput(runId, body)
    await get().fetchRunDetail(runId)
  },

  async bindLatestLaunch(workflowDefId, jobId, folderId) {
    if (!workflowDefId || !jobId) return

    set((state) => {
      const nextRecent = setRecentLaunchRecord(
        state.recentLaunchByScope,
        workflowDefId,
        jobId,
        undefined,
        folderId,
      )
      persistRecentLaunches(nextRecent)
      return { recentLaunchByScope: nextRecent }
    })

    for (let attempt = 0; attempt < 8; attempt += 1) {
      await get().fetchRunsForJob(jobId)
      const runs = get().runsByJobId[jobId] ?? []
      const normalizedFolderId = folderId?.trim() ?? ''
      const matchedRun = runs.find((run) => (
        run.workflow_def_id === workflowDefId
        && (normalizedFolderId === '' || run.folder_id === normalizedFolderId)
      ))
      if (matchedRun) {
        set((state) => {
          const nextRecent = setRecentLaunchRecord(
            state.recentLaunchByScope,
            workflowDefId,
            jobId,
            matchedRun.id,
            matchedRun.folder_id || folderId,
          )
          persistRecentLaunches(nextRecent)
          return { recentLaunchByScope: nextRecent }
        })
        void get().fetchRunDetail(matchedRun.id)
        if (matchedRun.status === 'waiting_input') {
          void get().fetchRunReviews(matchedRun.id)
        }
        return
      }
      await delay(250)
    }
  },

  async restoreLatestLaunch(workflowDefId, folderId) {
    if (!workflowDefId) return

    const normalizedFolderId = folderId?.trim() ?? ''
    const localRecord = getRecentLaunchRecord(get().recentLaunchByScope, workflowDefId, folderId)
    if (localRecord?.workflowRunId) {
      void get().fetchRunDetail(localRecord.workflowRunId)
      return
    }

    if (localRecord?.jobId) {
      await get().fetchRunsForJob(localRecord.jobId)
      const localRuns = get().runsByJobId[localRecord.jobId] ?? []
      const normalizedFolderId = folderId?.trim() ?? ''
      const matchedRun = localRuns.find((run) => (
        run.workflow_def_id === workflowDefId
        && (normalizedFolderId === '' || run.folder_id === normalizedFolderId)
      ))
      if (matchedRun) {
        set((state) => {
          const nextRecent = setRecentLaunchRecord(
            state.recentLaunchByScope,
            workflowDefId,
            localRecord.jobId,
            matchedRun.id,
            matchedRun.folder_id || folderId,
          )
          persistRecentLaunches(nextRecent)
          return { recentLaunchByScope: nextRecent }
        })
        void get().fetchRunDetail(matchedRun.id)
        return
      }
    }

    if (normalizedFolderId !== '') return

    try {
      const jobResp = await listJobs({ page: 1, limit: 100 })
      const fallbackJob = jobResp.data.find(
        (job) => job.type === 'workflow' && job.workflow_def_id === workflowDefId,
      )
      if (!fallbackJob) return
      await get().bindLatestLaunch(workflowDefId, fallbackJob.id, folderId)
    } catch (error) {
      console.error(`restoreLatestLaunch ${workflowDefId}:`, error)
    }
  },

  handleRunUpdated(event) {
    const workflowDefId = event.workflow_def_id?.trim() ?? ''
    const workflowRunId = event.workflow_run_id?.trim() ?? ''
    const jobId = event.job_id?.trim() ?? ''

    if (!workflowRunId || !jobId || !workflowDefId) return

    set((state) => {
      const now = new Date().toISOString()
      const existing = state.runsById[workflowRunId]
      const eventFolderId = event.folder_id?.trim() ?? ''
      const nextRun: WorkflowRun = {
        id: workflowRunId,
        job_id: jobId,
        folder_id: eventFolderId || existing?.folder_id || '',
        source_dir: existing?.source_dir,
        workflow_def_id: workflowDefId,
        status: (event.status ?? existing?.status ?? 'pending') as WorkflowRunStatus,
        resume_node_id: event.resume_node_id ?? existing?.resume_node_id ?? null,
        last_node_id: event.last_node_id ?? existing?.last_node_id ?? '',
        error: event.error ?? existing?.error ?? '',
        started_at: existing?.started_at ?? null,
        finished_at: existing?.finished_at ?? null,
        created_at: existing?.created_at ?? now,
        updated_at: now,
      }

      const currentRuns = state.runsByJobId[jobId] ?? []
      const nextRuns = upsertRunByJob(currentRuns, nextRun)

      const nextRecent = setRecentLaunchRecord(
        state.recentLaunchByScope,
        workflowDefId,
        jobId,
        workflowRunId,
        eventFolderId || existing?.folder_id,
      )
      persistRecentLaunches(nextRecent)

      return {
        runsById: { ...state.runsById, [workflowRunId]: nextRun },
        runsByJobId: { ...state.runsByJobId, [jobId]: nextRuns },
        recentLaunchByScope: nextRecent,
      }
    })

    get().refreshRunFromEvent(workflowRunId)
  },

  handleReviewEvent(workflowRunId) {
    if (!workflowRunId) return
    get().refreshRunFromEvent(workflowRunId)
    void get().fetchRunReviews(workflowRunId)
  },

  refreshRunFromEvent(runId) {
    if (!runId) return
    const knownRun = get().runsById[runId]
    if (!knownRun) {
      void get().fetchRunDetail(runId)
      return
    }
    scheduleRunRefresh(runId, get().fetchRunDetail)
  },

  handleNodeEvent(event) {
    const workflowRunID = event.workflow_run_id
    const nodeID = event.node_id
    const nodeType = event.node_type
    const status: NodeRunStatus = event.error ? 'failed' : (event.status ?? 'running')

    set((state) => {
      const existing = state.nodesByRunId[workflowRunID] ?? []
      const idx = existing.findIndex((nodeRun) => nodeRun.node_id === nodeID)

      const now = new Date().toISOString()
      let updatedNodes: NodeRun[]
      if (idx !== -1) {
        updatedNodes = existing.map((nodeRun, index) => {
          if (index !== idx) return nodeRun
          return {
            ...nodeRun,
            node_type: (nodeType as NodeType) || nodeRun.node_type,
            status,
            error: event.error ?? nodeRun.error,
            started_at: status === 'running' ? (nodeRun.started_at ?? now) : nodeRun.started_at,
            finished_at: status !== 'running' ? now : nodeRun.finished_at,
          }
        })
      } else {
        const placeholder: NodeRun = {
          id: '',
          workflow_run_id: workflowRunID,
          node_id: nodeID,
          node_type: nodeType as NodeType,
          sequence: 0,
          status,
          input_json: '',
          output_json: '',
          error: event.error ?? '',
          started_at: status === 'running' ? now : null,
          finished_at: status !== 'running' ? now : null,
          created_at: now,
        }
        updatedNodes = [...existing, placeholder]
      }

      return {
        nodesByRunId: { ...state.nodesByRunId, [workflowRunID]: updatedNodes },
      }
    })

    get().refreshRunFromEvent(workflowRunID)
  },

  buildRunCardView(workflowDefId, totalNodes, folderId) {
    const record = getRecentLaunchRecord(get().recentLaunchByScope, workflowDefId, folderId)
    if (!record) return null

    const runID = record.workflowRunId
    const fromRunID = runID ? get().runsById[runID] : undefined
    const normalizedFolderId = folderId?.trim() ?? ''
    const fromRunIDMatchesFolder = !fromRunID
      || normalizedFolderId === ''
      || fromRunID.folder_id === normalizedFolderId
    const scopedRun = fromRunIDMatchesFolder ? fromRunID : undefined
    const fromJobRuns = (get().runsByJobId[record.jobId] ?? []).find(
      (run) => run.workflow_def_id === workflowDefId
        && (normalizedFolderId === '' || run.folder_id === normalizedFolderId),
    )
    const run = scopedRun ?? fromJobRuns ?? null

    if (!run) {
      if (fromRunID && !fromRunIDMatchesFolder) return null
      if (!isRecentBindingRecord(record)) return null
      return {
        workflowDefId,
        jobId: record.jobId,
        workflowRunId: record.workflowRunId ?? '',
        status: 'pending',
        currentNodeId: '',
        currentNodeType: '',
        completedNodes: 0,
        totalNodes,
        failureSummary: '',
        reviewProgressText: '等待关联运行记录',
        isBinding: true,
      }
    }

    if (!ACTIVE_WORKFLOW_RUN_STATUSES.has(run.status)) {
      return null
    }

    const nodeRuns = get().nodesByRunId[run.id] ?? []
    const latestNodeRunsByID: Record<string, NodeRun> = {}
    nodeRuns.forEach((nodeRun) => {
      const prev = latestNodeRunsByID[nodeRun.node_id]
      if (!prev || nodeRun.sequence > prev.sequence) {
        latestNodeRunsByID[nodeRun.node_id] = nodeRun
      }
    })
    const latestNodeRuns = Object.values(latestNodeRunsByID)
    const currentNode = chooseCurrentNode(run, nodeRuns)
    const completedNodesRaw = latestNodeRuns.filter((nodeRun) => TERMINAL_NODE_STATUSES.has(nodeRun.status)).length
    const normalizedTotalNodes = totalNodes > 0 ? totalNodes : Math.max(latestNodeRuns.length, completedNodesRaw)
    const completedNodes = normalizedTotalNodes > 0 ? Math.min(completedNodesRaw, normalizedTotalNodes) : completedNodesRaw

    const reviewSummary = get().reviewSummaryByRunId[run.id]
    const reviewProgressText = reviewSummary
      ? `${reviewSummary.approved + reviewSummary.rolled_back} / ${reviewSummary.total}`
      : '—'

    const latestFailedNode = [...nodeRuns]
      .filter((nodeRun) => nodeRun.status === 'failed' && nodeRun.error.trim() !== '')
      .sort((a, b) => b.sequence - a.sequence)[0]

    return {
      workflowDefId,
      jobId: run.job_id,
      workflowRunId: run.id,
      status: run.status,
      currentNodeId: currentNode?.node_id ?? (run.last_node_id?.trim() ?? ''),
      currentNodeType: currentNode?.node_type ?? '',
      completedNodes,
      totalNodes: normalizedTotalNodes,
      failureSummary: run.error?.trim() || latestFailedNode?.error?.trim() || '',
      reviewSummary,
      reviewProgressText,
      isBinding: false,
    }
  },
}))
