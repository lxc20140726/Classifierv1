import { create } from 'zustand'

import {
  createWorkflowDef,
  deleteWorkflowDef,
  listWorkflowDefs,
  updateWorkflowDef,
} from '@/api/workflowDefs'
import type { WorkflowDefinition } from '@/types'

interface WorkflowDefStore {
  defs: WorkflowDefinition[]
  total: number
  isLoading: boolean
  error: string | null
  fetchDefs: () => Promise<void>
  createDef: (name: string, graphJson: string) => Promise<void>
  updateDef: (id: string, patch: { name?: string; graph_json?: string; is_active?: boolean }) => Promise<void>
  deleteDef: (id: string) => Promise<void>
  setActive: (id: string) => Promise<void>
}

export const useWorkflowDefStore = create<WorkflowDefStore>((set, get) => ({
  defs: [],
  total: 0,
  isLoading: false,
  error: null,

  async fetchDefs() {
    set({ isLoading: true, error: null })
    try {
      const res = await listWorkflowDefs({ limit: 100 })
      set({ defs: res.data, total: res.total, isLoading: false })
    } catch (err) {
      set({ isLoading: false, error: err instanceof Error ? err.message : '加载失败' })
    }
  },

  async createDef(name, graphJson) {
    await createWorkflowDef({ name, graph_json: graphJson })
    await get().fetchDefs()
  },

  async updateDef(id, patch) {
    await updateWorkflowDef(id, patch)
    await get().fetchDefs()
  },

  async deleteDef(id) {
    await deleteWorkflowDef(id)
    await get().fetchDefs()
  },

  async setActive(id) {
    const current = get().defs
    for (const def of current) {
      if (def.is_active && def.id !== id) {
        await updateWorkflowDef(def.id, { is_active: false })
      }
    }
    await updateWorkflowDef(id, { is_active: true })
    await get().fetchDefs()
  },
}))
