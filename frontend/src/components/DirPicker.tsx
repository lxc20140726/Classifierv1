import { useCallback, useEffect, useState, useRef } from 'react'
import { ChevronRight, Folder, FolderOpen, Loader2, X } from 'lucide-react'
import gsap from 'gsap'

import { listDirs, type FsDirEntry } from '@/api/fs'
import { cn } from '@/lib/utils'

export interface DirPickerProps {
  open: boolean
  initialPath?: string
  onConfirm: (path: string) => void
  onCancel: () => void
  title?: string
}

interface NavEntry {
  path: string
  parent: string
  entries: FsDirEntry[]
}

export function DirPicker({ open, initialPath = '', onConfirm, onCancel, title }: DirPickerProps) {
  const [current, setCurrent] = useState<NavEntry | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<string | null>(null)
  const [pathInput, setPathInput] = useState(initialPath)
  
  const overlayRef = useRef<HTMLDivElement | null>(null)
  const modalRef = useRef<HTMLDivElement | null>(null)

  const navigate = useCallback(async (path: string) => {
    setIsLoading(true)
    setError(null)
    try {
      const res = await listDirs(path)
      setCurrent({ path: res.path, parent: res.parent, entries: res.entries })
      setSelected(res.path)
      setPathInput(res.path)
    } catch (e) {
      setError(e instanceof Error ? e.message : '无法读取目录')
    } finally {
      setIsLoading(false)
    }
  }, [])

  useEffect(() => {
    if (open) {
      void navigate(initialPath.trim())
      
      // GSAP Animation
      if (overlayRef.current && modalRef.current) {
        gsap.fromTo(overlayRef.current, { opacity: 0 }, { opacity: 1, duration: 0.2 })
        gsap.fromTo(modalRef.current, { scale: 0.8, opacity: 0 }, { scale: 1, opacity: 1, duration: 0.4, ease: "back.out(1.7)" })
      }
    }
  }, [open, initialPath, navigate])

  function handleInputKeyDown(e: { key: string }) {
    if (e.key === 'Enter') {
      void navigate(pathInput.trim())
    }
  }

  if (!open) return null

  return (
    <div ref={overlayRef} className="fixed inset-0 z-50 flex items-center justify-center bg-black/50 p-4">
      <div ref={modalRef} className="flex w-full max-w-lg flex-col border-2 border-foreground bg-card shadow-hard-lg">
        {/* Header */}
        <div className="flex items-center justify-between border-b-2 border-foreground px-5 py-4 bg-primary text-primary-foreground">
          <h2 className="text-base font-bold tracking-tight">{title ?? '选择目录'}</h2>
          <button
            type="button"
            onClick={onCancel}
            className="border-2 border-transparent p-1.5 transition-all hover:border-primary-foreground hover:bg-foreground hover:text-background"
            aria-label="关闭"
          >
            <X className="h-4 w-4" />
          </button>
        </div>

        {/* Current path input */}
        <div className="border-b-2 border-foreground px-5 py-4 bg-muted/30">
          <div className="flex flex-col gap-2 sm:flex-row">
            <input
              type="text"
              value={pathInput}
              placeholder="/path/to/dir"
              className="flex-1 border-2 border-foreground bg-background px-3 py-2 text-sm font-mono outline-none focus:ring-2 focus:ring-foreground focus:ring-offset-2 focus:ring-offset-background"
              onChange={(e) => setPathInput(e.target.value)}
              onKeyDown={handleInputKeyDown}
            />
            <button
              type="button"
              onClick={() => { void navigate(pathInput.trim()) }}
              className="border-2 border-foreground bg-background px-4 py-2 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 sm:shrink-0"
            >
              前往
            </button>
          </div>
          <p className="mt-2 text-xs font-medium text-muted-foreground">按 Enter 或点击「前往」跳转到指定路径</p>
        </div>

        {/* Directory listing */}
        <div className="max-h-72 overflow-y-auto bg-background">
          {/* Parent dir navigation */}
          {current && current.parent !== '' && (
            <button
              type="button"
              onClick={() => {
                void navigate(current.parent)
              }}
              className="group flex w-full items-center gap-2 border-b-2 border-foreground px-5 py-3 text-sm font-medium transition-colors hover:bg-foreground hover:text-background"
            >
              <ChevronRight className="h-4 w-4 rotate-180" />
              <span>上级目录</span>
            </button>
          )}

          {isLoading && (
            <div className="flex items-center justify-center py-12">
              <Loader2 className="h-6 w-6 animate-spin text-foreground" />
            </div>
          )}

          {error && (
            <div className="px-5 py-4 text-sm font-bold text-red-600 border-b-2 border-foreground">{error}</div>
          )}

          {!isLoading && !error && current && (
            <>
              {/* Current dir selectable row */}
              <button
                type="button"
                onClick={() => setSelected(current.path)}
                className={cn(
                  'flex w-full items-center gap-2 border-b-2 border-foreground px-5 py-3 text-sm font-mono transition-colors',
                  selected === current.path ? 'bg-primary text-primary-foreground' : 'hover:bg-muted',
                )}
              >
                <FolderOpen className="h-4 w-4 shrink-0" />
                <span className="break-all text-left">{current.path}</span>
              </button>

              {current.entries.length === 0 && (
                <p className="px-5 py-4 text-sm font-medium text-muted-foreground border-b-2 border-foreground">此目录下没有子目录。</p>
              )}

              {current.entries.map((entry) => (
                <div key={entry.path} className="flex items-stretch border-b-2 border-foreground last:border-b-0">
                  <button
                    type="button"
                    onClick={() => setSelected(entry.path)}
                    className={cn(
                      'flex flex-1 items-center gap-2 px-5 py-3 text-sm font-mono transition-colors',
                      selected === entry.path ? 'bg-primary text-primary-foreground' : 'hover:bg-muted',
                    )}
                  >
                    <Folder className="h-4 w-4 shrink-0" />
                    <span className="break-all text-left">{entry.name}</span>
                  </button>
                  <button
                    type="button"
                    onClick={() => void navigate(entry.path)}
                    title="进入此目录"
                    className="flex items-center justify-center border-l-2 border-foreground px-4 text-foreground transition-colors hover:bg-foreground hover:text-background"
                  >
                    <ChevronRight className="h-4 w-4" />
                  </button>
                </div>
              ))}
            </>
          )}
        </div>

        {/* Footer */}
        <div className="flex flex-col gap-3 border-t-2 border-foreground bg-muted/30 px-5 py-4 sm:flex-row sm:items-center sm:justify-between">
          <p className="w-full break-all text-xs font-mono font-bold text-foreground sm:max-w-[50%]">
            {selected ?? '未选择'}
          </p>
          <div className="flex w-full gap-3 sm:w-auto">
            <button
              type="button"
              onClick={onCancel}
              className="flex-1 border-2 border-foreground bg-background px-4 py-2 text-sm font-bold transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 sm:flex-none"
            >
              取消
            </button>
            <button
              type="button"
              disabled={selected === null}
              onClick={() => { if (selected) onConfirm(selected) }}
              className="flex-1 border-2 border-foreground bg-primary px-4 py-2 text-sm font-bold text-primary-foreground transition-all hover:bg-foreground hover:text-background hover:shadow-hard hover:-translate-y-0.5 disabled:cursor-not-allowed disabled:opacity-50 disabled:hover:bg-primary disabled:hover:text-primary-foreground disabled:hover:shadow-none disabled:hover:translate-y-0 sm:flex-none"
            >
              确认选择
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
