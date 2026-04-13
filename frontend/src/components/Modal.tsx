import { useEffect, type ReactNode } from 'react'
import { X } from 'lucide-react'

import { cn } from '@/lib/utils'

type ModalSize = 'sm' | 'md' | 'lg' | 'xl'

const MODAL_SIZE_CLASS_MAP: Record<ModalSize, string> = {
  sm: 'max-w-md',
  md: 'max-w-lg',
  lg: 'max-w-2xl',
  xl: 'max-w-4xl',
}

export interface ModalProps {
  open: boolean
  title?: string
  description?: string
  children: ReactNode
  footer?: ReactNode
  onClose: () => void
  size?: ModalSize
  closeOnOverlayClick?: boolean
  closeOnEsc?: boolean
  showCloseButton?: boolean
  panelClassName?: string
  contentClassName?: string
  overlayClassName?: string
}

export function Modal({
  open,
  title,
  description,
  children,
  footer,
  onClose,
  size = 'md',
  closeOnOverlayClick = true,
  closeOnEsc = true,
  showCloseButton = true,
  panelClassName,
  contentClassName,
  overlayClassName,
}: ModalProps) {
  useEffect(() => {
    if (!open || !closeOnEsc) return

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === 'Escape') {
        onClose()
      }
    }

    window.addEventListener('keydown', handleKeyDown)
    return () => {
      window.removeEventListener('keydown', handleKeyDown)
    }
  }, [closeOnEsc, onClose, open])

  if (!open) return null

  return (
    <div
      role="presentation"
      className={cn('fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4', overlayClassName)}
      onClick={() => {
        if (closeOnOverlayClick) {
          onClose()
        }
      }}
    >
      <div
        role="dialog"
        aria-modal="true"
        aria-label={title}
        className={cn(
          'flex w-full flex-col border-2 border-foreground bg-background shadow-hard-lg',
          MODAL_SIZE_CLASS_MAP[size],
          panelClassName,
        )}
        onClick={(event) => event.stopPropagation()}
      >
        {(title || description || showCloseButton) && (
          <div className="flex items-center justify-between gap-4 border-b-2 border-foreground bg-primary px-5 py-4 text-primary-foreground">
            <div className="min-w-0">
              {title && <h2 className="text-base font-black tracking-tight">{title}</h2>}
              {description && <p className="mt-1 text-xs font-medium text-primary-foreground/90">{description}</p>}
            </div>
            {showCloseButton && (
              <button
                type="button"
                onClick={onClose}
                className="shrink-0 border-2 border-transparent p-1.5 transition-all hover:border-primary-foreground hover:bg-foreground hover:text-background"
                aria-label="关闭"
              >
                <X className="h-4 w-4" />
              </button>
            )}
          </div>
        )}

        <div className={cn('px-5 py-5', contentClassName)}>{children}</div>

        {footer && <div className="border-t-2 border-foreground bg-muted/30 px-5 py-4">{footer}</div>}
      </div>
    </div>
  )
}
