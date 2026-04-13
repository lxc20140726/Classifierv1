import { create } from 'zustand'

import { getConfig } from '@/api/config'
import type { AppConfig } from '@/types'

interface ConfigState {
  scanInputDirs: string[]
  outputDirs: NonNullable<AppConfig['output_dirs']>
  loaded: boolean
  load: (force?: boolean) => Promise<void>
}

export const useConfigStore = create<ConfigState>((set, get) => ({
  scanInputDirs: [],
  outputDirs: {
    video: [],
    manga: [],
    photo: [],
    other: [],
    mixed: [],
  },
  loaded: false,
  load: async (force = false) => {
    if (get().loaded && !force) return
    try {
      const res = await getConfig()
      set({
        scanInputDirs: res.data.scan_input_dirs ?? [],
        outputDirs: {
          video: res.data.output_dirs?.video ?? [],
          manga: res.data.output_dirs?.manga ?? [],
          photo: res.data.output_dirs?.photo ?? [],
          other: res.data.output_dirs?.other ?? [],
          mixed: res.data.output_dirs?.mixed ?? [],
        },
        loaded: true,
      })
    } catch {
      set({ loaded: true })
    }
  },
}))
