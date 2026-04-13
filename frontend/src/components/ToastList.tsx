import { useEffect, useRef } from 'react'
import { AlertCircle, CheckCircle, Info, X } from 'lucide-react'
import gsap from 'gsap'

import { cn } from '@/lib/utils'
import { useNotificationStore, type AppNotification } from '@/store/notificationStore'

const LEVEL_CONFIG = {
  success: {
    icon: CheckCircle,
    bgClass: 'bg-primary border-foreground',
    iconClass: 'text-primary-foreground',
    textClass: 'text-primary-foreground',
  },
  error: {
    icon: AlertCircle,
    bgClass: 'bg-red-500 border-foreground',
    iconClass: 'text-white',
    textClass: 'text-white',
  },
  info: {
    icon: Info,
    bgClass: 'bg-background border-foreground',
    iconClass: 'text-foreground',
    textClass: 'text-foreground',
  },
}

function Toast({ notification }: { notification: AppNotification }) {
  const dismissNotification = useNotificationStore((store) => store.dismissNotification)
  const config = LEVEL_CONFIG[notification.level]
  const Icon = config.icon
  const toastRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (toastRef.current) {
      gsap.fromTo(toastRef.current, { x: 50, opacity: 0 }, { x: 0, opacity: 1, duration: 0.3, ease: 'back.out(1.5)' })
    }
    
    const timer = window.setTimeout(() => {
      if (toastRef.current) {
        gsap.to(toastRef.current, { x: 50, opacity: 0, duration: 0.2, onComplete: () => dismissNotification(notification.id) })
      } else {
        dismissNotification(notification.id)
      }
    }, 6000)

    return () => window.clearTimeout(timer)
  }, [dismissNotification, notification.id])

  return (
    <div
      ref={toastRef}
      className={cn(
        'pointer-events-auto flex w-full max-w-sm items-start gap-3 border-2 p-4 shadow-hard',
        config.bgClass,
      )}
    >
      <Icon className={cn('mt-0.5 h-5 w-5 shrink-0', config.iconClass)} />
      <div className="flex-1 space-y-1">
        <p className={cn('text-sm font-bold tracking-tight', config.textClass)}>{notification.title}</p>
        <p className={cn('text-sm font-medium', config.textClass)}>{notification.message}</p>
      </div>
      <button
        type="button"
        onClick={() => {
          if (toastRef.current) {
            gsap.to(toastRef.current, { x: 50, opacity: 0, duration: 0.2, onComplete: () => dismissNotification(notification.id) })
          } else {
            dismissNotification(notification.id)
          }
        }}
        className={cn('shrink-0 border-2 border-transparent p-1 transition-colors hover:border-current', config.textClass)}
        aria-label="关闭通知"
      >
        <X className="h-4 w-4" />
      </button>
    </div>
  )
}

export function ToastList() {
  const notifications = useNotificationStore((store) => store.notifications)

  if (notifications.length === 0) {
    return null
  }

  return (
    <div className="pointer-events-none fixed right-6 top-6 z-50 flex flex-col gap-4">
      {notifications.map((notification) => (
        <Toast key={notification.id} notification={notification} />
      ))}
    </div>
  )
}
