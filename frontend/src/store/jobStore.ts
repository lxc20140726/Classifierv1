import { create } from 'zustand'

import { getJobProgress, listJobs, type JobQueryParams } from '@/api/jobs'
import { notifyFolderActivityUpdated } from '@/lib/folderActivityEvents'
import type { Job, JobDoneEvent, JobProgress } from '@/types'

interface JobStore {
  jobs: Job[]
  total: number
  page: number
  limit: number
  isLoading: boolean
  error: string | null
  pollingJobIds: Set<string>
  pollingTimers: Map<string, number>
  fetchJobs: (params?: JobQueryParams) => Promise<void>
  addJob: (job: Job) => void
  updateJob: (jobId: string, updates: Partial<Job>) => void
  handleJobProgress: (progress: JobProgress) => void
  handleJobDone: (payload: JobDoneEvent) => void
  handleJobError: (jobId: string, error: string) => void
  startPolling: (jobId: string) => void
  /** Poll a scan job and notify folderStore on completion (SSE fallback). */
  startScanPolling: (jobId: string) => void
  stopPolling: (jobId: string) => void
  stopAllPolling: () => void
}

export const useJobStore = create<JobStore>((set, get) => ({
  jobs: [],
  total: 0,
  page: 1,
  limit: 20,
  isLoading: false,
  error: null,
  pollingJobIds: new Set(),
  pollingTimers: new Map(),

  async fetchJobs(params = {}) {
    set({ isLoading: true, error: null })

    try {
      const response = await listJobs({ page: get().page, limit: get().limit, ...params })
      set({
        jobs: response.data,
        total: response.total,
        page: response.page,
        limit: response.limit,
        isLoading: false,
      })

      response.data.forEach((job) => {
        if (job.status === 'running') {
          get().startPolling(job.id)
        }
      })
    } catch (error) {
      set({
        isLoading: false,
        error: error instanceof Error ? error.message : 'Failed to load jobs',
      })
    }
  },

  addJob(job) {
    set((state) => ({
      jobs: [job, ...state.jobs],
      total: state.total + 1,
    }))

    if (job.status === 'running') {
      get().startPolling(job.id)
    }
  },

  updateJob(jobId, updates) {
    set((state) => ({
      jobs: state.jobs.map((job) => (job.id === jobId ? { ...job, ...updates } : job)),
    }))

    const job = get().jobs.find((j) => j.id === jobId)
    if (job && ['succeeded', 'failed', 'partial', 'cancelled', 'rolled_back'].includes(job.status)) {
      get().stopPolling(jobId)
    }
  },

  handleJobProgress(progress) {
    const existing = get().jobs.find((job) => job.id === progress.job_id)
    if (!existing) {
      get().addJob({
        id: progress.job_id,
        type: 'scan',
        status: progress.status,
        folder_ids: [],
        total: progress.total,
        done: progress.done,
        failed: progress.failed,
        error: '',
        started_at: progress.updated_at,
        finished_at: null,
        created_at: progress.updated_at,
        updated_at: progress.updated_at,
      })
      return
    }

    get().updateJob(progress.job_id, {
      status: progress.status,
      done: progress.done,
      total: progress.total,
      failed: progress.failed,
      updated_at: progress.updated_at,
    })
  },

  handleJobDone(payload) {
    const now = new Date().toISOString()
    const existing = get().jobs.find((job) => job.id === payload.job_id)
    if (!existing) {
      get().addJob({
        id: payload.job_id,
        type: 'scan',
        status: payload.status,
        folder_ids: [],
        total: payload.total,
        done: payload.processed ?? payload.total,
        failed: payload.failed ?? 0,
        error: '',
        started_at: now,
        finished_at: now,
        created_at: now,
        updated_at: now,
      })
    } else {
      get().updateJob(payload.job_id, {
        status: payload.status,
        done: payload.processed ?? existing.done,
        failed: payload.failed ?? existing.failed,
        total: payload.total,
        finished_at: now,
        updated_at: now,
      })
    }
    get().stopPolling(payload.job_id)
  },

  handleJobError(jobId, error) {
    get().updateJob(jobId, { status: 'failed', error })
    get().stopPolling(jobId)
  },

  startPolling(jobId) {
    const { pollingJobIds, pollingTimers } = get()

    if (pollingJobIds.has(jobId)) {
      return
    }

    pollingJobIds.add(jobId)

    const poll = async () => {
      try {
        const progress = await getJobProgress(jobId)
        get().handleJobProgress(progress)

        if (progress.status === 'waiting_input') {
          const { useActivityStore } = await import('@/store/activityStore')
          const { useFolderStore } = await import('@/store/folderStore')
          void useActivityStore.getState().fetchLogs({ limit: 20 })
          void useFolderStore.getState().fetchFolders()
          notifyFolderActivityUpdated()
        }

        if (['succeeded', 'failed', 'partial', 'cancelled', 'rolled_back'].includes(progress.status)) {
          get().stopPolling(jobId)
          const { useActivityStore } = await import('@/store/activityStore')
          const { useFolderStore } = await import('@/store/folderStore')
          void useActivityStore.getState().fetchLogs({ limit: 20 })
          void useFolderStore.getState().fetchFolders()
          notifyFolderActivityUpdated()
        } else {
          const timer = window.setTimeout(poll, 2000)
          pollingTimers.set(jobId, timer)
        }
      } catch (error) {
        console.error(`Failed to poll job ${jobId}:`, error)
        get().stopPolling(jobId)
      }
    }

    void poll()
  },

  startScanPolling(jobId) {
    const { pollingJobIds } = get()
    if (pollingJobIds.has(jobId)) return

    pollingJobIds.add(jobId)

    const poll = async () => {
      try {
        const progress = await getJobProgress(jobId)
        get().handleJobProgress(progress)

        if (['succeeded', 'failed', 'partial', 'cancelled', 'rolled_back'].includes(progress.status)) {
          get().stopPolling(jobId)
          // SSE fallback: notify folderStore that the scan job is done.
          // Lazy import avoids a circular dependency at module evaluation time.
          const { useFolderStore } = await import('@/store/folderStore')
          const folderStore = useFolderStore.getState()
          if (folderStore.scanProgress?.jobId === jobId || folderStore.isScanning) {
            folderStore.handleScanDone()
            void folderStore.fetchFolders()
          }
        } else {
          const timer = window.setTimeout(poll, 2000)
          get().pollingTimers.set(jobId, timer)
        }
      } catch (error) {
        console.error(`Failed to poll scan job ${jobId}:`, error)
        get().stopPolling(jobId)
      }
    }

    void poll()
  },

  stopPolling(jobId) {
    const { pollingJobIds, pollingTimers } = get()

    pollingJobIds.delete(jobId)

    const timer = pollingTimers.get(jobId)
    if (timer !== undefined) {
      window.clearTimeout(timer)
      pollingTimers.delete(jobId)
    }
  },

  stopAllPolling() {
    const { pollingTimers } = get()

    pollingTimers.forEach((timer) => {
      window.clearTimeout(timer)
    })

    set({ pollingJobIds: new Set(), pollingTimers: new Map() })
  },
}))
