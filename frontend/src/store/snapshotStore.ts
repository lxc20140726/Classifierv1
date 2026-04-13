import { create } from 'zustand'

import { listSnapshots } from '@/api/snapshots'
import type { Snapshot } from '@/types'

interface SnapshotStore {
  snapshots: Snapshot[]
  isLoading: boolean
  error: string | null
  fetchSnapshots: (folderId: string) => Promise<void>
  handleRevertDone: (snapshotId: string) => void
}

export const useSnapshotStore = create<SnapshotStore>((set) => ({
  snapshots: [],
  isLoading: false,
  error: null,
  async fetchSnapshots(folderId) {
    set({ isLoading: true, error: null })

    try {
      const snapshots = await listSnapshots({ folderId })
      set({ snapshots, isLoading: false })
    } catch (error) {
      set({
        snapshots: [],
        isLoading: false,
        error: error instanceof Error ? error.message : 'Failed to load snapshots',
      })
    }
  },
  handleRevertDone(snapshotId) {
    set((state) => ({
      snapshots: state.snapshots.map((snapshot) =>
        snapshot.id === snapshotId ? { ...snapshot, status: 'reverted' } : snapshot,
      ),
    }))
  },
}))
