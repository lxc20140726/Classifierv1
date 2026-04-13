import { create } from 'zustand'

import { listAuditLogs } from '@/api/auditLogs'
import type { AuditLog } from '@/types'

interface ActivityStore {
  logs: AuditLog[]
  total: number
  isLoading: boolean
  error: string | null
  fetchLogs: (params?: { jobId?: string; folderId?: string; limit?: number }) => Promise<void>
}

export const useActivityStore = create<ActivityStore>((set) => ({
  logs: [],
  total: 0,
  isLoading: false,
  error: null,
  async fetchLogs(params = {}) {
    set({ isLoading: true, error: null })

    try {
      const response = await listAuditLogs({ page: 1, limit: params.limit ?? 20, ...params })
      set({ logs: response.data, total: response.total, isLoading: false })
    } catch (error) {
      set({
        logs: [],
        total: 0,
        isLoading: false,
        error: error instanceof Error ? error.message : '加载日志失败',
      })
    }
  },
}))
