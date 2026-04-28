import { useEffect } from 'react'

import { notifyFolderActivityUpdated } from '@/lib/folderActivityEvents'
import { useActivityStore } from '@/store/activityStore'
import { useFolderStore } from '@/store/folderStore'
import { useJobStore } from '@/store/jobStore'
import { useLiveClassificationStore } from '@/store/liveClassificationStore'
import { useNotificationStore } from '@/store/notificationStore'
import { useWorkflowRunStore } from '@/store/workflowRunStore'
import type {
  FolderClassificationLiveEvent,
  JobDoneEvent,
  ScanProgressEvent,
  WorkflowNodeEvent,
  WorkflowRunUpdatedEvent,
} from '@/types'

interface JobProgressEvent extends ScanProgressEvent {
  status: JobDoneEvent['status']
  failed?: number
  started_at?: string | null
  finished_at?: string | null
  updated_at?: string
}

interface JobErrorEvent extends ScanProgressEvent {
  error: string
}

export function useSSE() {
  useEffect(() => {
    let eventSource: EventSource | null = null
    let reconnectTimer: number | null = null

    const connect = () => {
      eventSource = new EventSource('/api/events')

      eventSource.addEventListener('scan.started', (event) => {
        const payload = JSON.parse(event.data) as {
          job_id: string
          source_dirs: string[]
        }
        useFolderStore.getState().handleScanStarted({
          started: true,
          job_id: payload.job_id,
          source_dirs: payload.source_dirs,
        })
        useLiveClassificationStore.getState().handleScanStarted({ job_id: payload.job_id })
      })

      eventSource.addEventListener('scan.progress', (event) => {
        const payload = JSON.parse(event.data) as ScanProgressEvent
        useFolderStore.getState().handleScanProgress(payload)
        useLiveClassificationStore.getState().handleScanProgress(payload)
      })

      eventSource.addEventListener('scan.error', (event) => {
        const payload = JSON.parse(event.data) as JobErrorEvent
        useFolderStore.getState().handleScanError(payload)
        useLiveClassificationStore.getState().handleScanError(payload)
      })

      eventSource.addEventListener('scan.done', () => {
        const store = useFolderStore.getState()
        store.handleScanDone()
        useLiveClassificationStore.getState().handleScanDone()
        void store.fetchFolders()
        void useActivityStore.getState().fetchLogs({ limit: 20 })
        notifyFolderActivityUpdated()
      })

      eventSource.addEventListener('job.progress', (event) => {
        const payload = JSON.parse(event.data) as JobProgressEvent
        useJobStore.getState().handleJobProgress({
          job_id: payload.job_id,
          status: payload.status,
          done: payload.done,
          total: payload.total,
          failed: payload.failed ?? 0,
          started_at: payload.started_at,
          finished_at: payload.finished_at,
          updated_at: payload.updated_at ?? new Date().toISOString(),
        })
      })

      eventSource.addEventListener('job.done', (event) => {
        const payload = JSON.parse(event.data) as JobDoneEvent
        useJobStore.getState().handleJobDone(payload)
        useFolderStore.getState().handleScanDone()
        const isCancelled = payload.status === 'cancelled'
        useNotificationStore.getState().pushNotification({
          level: payload.status === 'partial' || isCancelled ? 'info' : 'success',
          title: isCancelled ? '任务已停止' : payload.status === 'partial' ? '任务部分完成' : '任务完成',
          message:
            isCancelled
              ? `任务 ${payload.job_id} 已停止。`
              : payload.status === 'partial'
              ? `任务 ${payload.job_id} 已完成，但有 ${payload.failed ?? 0} 个目录失败。`
              : `任务 ${payload.job_id} 已完成，共处理 ${payload.total} 个目录。`,
          jobId: payload.job_id,
        })
        void useFolderStore.getState().fetchFolders()
        void useActivityStore.getState().fetchLogs({ limit: 20 })
        notifyFolderActivityUpdated()
      })

      eventSource.addEventListener('job.error', (event) => {
        const payload = JSON.parse(event.data) as JobErrorEvent
        useJobStore.getState().handleJobError(payload.job_id, payload.error)
        useNotificationStore.getState().pushNotification({
          level: 'error',
          title: '任务报错',
          message: payload.error,
          jobId: payload.job_id,
        })
        void useActivityStore.getState().fetchLogs({ limit: 20 })
      })

      eventSource.addEventListener('workflow_run.node_started', (event) => {
        const payload = JSON.parse(event.data) as WorkflowNodeEvent
        useWorkflowRunStore.getState().handleNodeEvent({ ...payload, status: 'running' })
        useLiveClassificationStore.getState().handleWorkflowNodeEvent(payload, 'classifying')
      })

      eventSource.addEventListener('workflow_run.node_done', (event) => {
        const payload = JSON.parse(event.data) as WorkflowNodeEvent
        useWorkflowRunStore.getState().handleNodeEvent({ ...payload, status: 'succeeded' })
        useLiveClassificationStore.getState().handleWorkflowNodeEvent(payload, 'classifying')
      })

      eventSource.addEventListener('workflow_run.node_failed', (event) => {
        const payload = JSON.parse(event.data) as WorkflowNodeEvent
        useWorkflowRunStore.getState().handleNodeEvent({ ...payload, status: 'failed' })
        useLiveClassificationStore.getState().handleWorkflowNodeEvent(payload, 'failed')
      })

      eventSource.addEventListener('workflow_run.node_progress', (event) => {
        const payload = JSON.parse(event.data) as WorkflowNodeEvent
        useWorkflowRunStore.getState().handleNodeProgress(payload)
      })

      eventSource.addEventListener('workflow_run.node_pending', (event) => {
        const payload = JSON.parse(event.data) as WorkflowNodeEvent
        useWorkflowRunStore.getState().handleNodeEvent({ ...payload, status: 'waiting_input' })
        useLiveClassificationStore.getState().handleWorkflowNodeEvent(payload, 'waiting_input')
      })

      eventSource.addEventListener('workflow_run.updated', (event) => {
        const payload = JSON.parse(event.data) as WorkflowRunUpdatedEvent
        useWorkflowRunStore.getState().handleRunUpdated(payload)
        useLiveClassificationStore.getState().handleWorkflowRunUpdated(payload)
        if (
          payload.status === 'succeeded'
          || payload.status === 'waiting_input'
          || payload.status === 'rolled_back'
          || payload.status === 'partial'
          || payload.status === 'cancelled'
          || payload.status === 'failed'
        ) {
          const folderId = payload.folder_id?.trim() ?? ''
          if (folderId !== '') {
            const focusFolderId = useLiveClassificationStore.getState().focusFolderId.trim()
            if (focusFolderId === '' || focusFolderId === folderId) {
              void useLiveClassificationStore.getState().syncFolder(folderId)
            }
            void useFolderStore.getState().syncFolder(folderId).finally(() => {
              notifyFolderActivityUpdated()
            })
          }
        }
      })

      const refreshRunReviews = (event: MessageEvent<string>) => {
        const payload = JSON.parse(event.data) as { workflow_run_id: string }
        if (!payload.workflow_run_id) return
        const workflowRunStore = useWorkflowRunStore.getState()
        workflowRunStore.handleReviewEvent(payload.workflow_run_id)
        void useJobStore.getState().fetchJobs()
        const run = workflowRunStore.runsById[payload.workflow_run_id]
        const folderId = run?.folder_id?.trim() ?? ''
        if (folderId !== '') {
          void useFolderStore.getState().syncFolder(folderId).finally(() => {
            notifyFolderActivityUpdated()
          })
        }
      }

      eventSource.addEventListener('folder.classification.updated', (event) => {
        const payload = JSON.parse(event.data) as FolderClassificationLiveEvent
        useLiveClassificationStore.getState().handleFolderClassificationUpdated(payload)
      })

      eventSource.addEventListener('workflow_run.review_pending', refreshRunReviews)
      eventSource.addEventListener('workflow_run.review_updated', refreshRunReviews)

      eventSource.onerror = () => {
        eventSource?.close()
        reconnectTimer = window.setTimeout(connect, 3000)
      }
    }

    connect()

    return () => {
      if (reconnectTimer !== null) {
        window.clearTimeout(reconnectTimer)
      }

      eventSource?.close()
      useJobStore.getState().stopAllPolling()
    }
  }, [])
}
