import { create } from 'zustand'
import { persist } from 'zustand/middleware'

type Theme = 'light' | 'dark'

interface ThemeState {
  theme: Theme
  toggleTheme: () => void
  setTheme: (theme: Theme) => void
}

function applyThemeToDocument(theme: Theme) {
  if (theme === 'dark') {
    document.documentElement.classList.add('dark')
  } else {
    document.documentElement.classList.remove('dark')
  }
}

export const useThemeStore = create<ThemeState>()(
  persist(
    (set) => ({
      theme: 'light',
      toggleTheme: () =>
        set((state) => {
          const nextTheme: Theme = state.theme === 'light' ? 'dark' : 'light'
          applyThemeToDocument(nextTheme)
          return { theme: nextTheme }
        }),
      setTheme: (theme) => {
        applyThemeToDocument(theme)
        set({ theme })
      },
    }),
    {
      name: 'classifier-theme',
      onRehydrateStorage: () => (state) => {
        if (state) {
          applyThemeToDocument(state.theme)
        }
      },
    },
  ),
)
