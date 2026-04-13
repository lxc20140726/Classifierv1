import { create } from 'zustand'

type NotificationLevel = 'success' | 'error' | 'info'

export interface AppNotification {
  id: string
  level: NotificationLevel
  title: string
  message: string
  jobId?: string
  createdAt: string
}

interface NotificationStore {
  notifications: AppNotification[]
  pushNotification: (notification: Omit<AppNotification, 'id' | 'createdAt'>) => void
  dismissNotification: (id: string) => void
  clearNotifications: () => void
}

export const useNotificationStore = create<NotificationStore>((set) => ({
  notifications: [],
  pushNotification(notification) {
    set((state) => ({
      notifications: [
        {
          ...notification,
          id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
          createdAt: new Date().toISOString(),
        },
        ...state.notifications,
      ].slice(0, 6),
    }))
  },
  dismissNotification(id) {
    set((state) => ({
      notifications: state.notifications.filter((item) => item.id !== id),
    }))
  },
  clearNotifications() {
    set({ notifications: [] })
  },
}))
