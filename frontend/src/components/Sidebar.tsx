import { useEffect, useRef, type ComponentType } from 'react'
import {
  Briefcase,
  ChevronLeft,
  ChevronRight,
  FolderClock,
  FolderKanban,
  GitBranch,
  Moon,
  ScrollText,
  Settings,
  Sun,
  X,
} from 'lucide-react'
import { NavLink } from 'react-router-dom'
import gsap from 'gsap'

import { cn } from '@/lib/utils'
import { useThemeStore } from '@/store/themeStore'

interface NavItem {
  to: string
  label: string
  icon: ComponentType<{ className?: string }>
  end?: boolean
}

const SIDEBAR_ITEMS: NavItem[] = [
  { to: '/', label: '\u6587\u4ef6\u5939', icon: FolderKanban, end: true },
  { to: '/job-history', label: '\u6267\u884c\u5386\u53f2', icon: FolderClock },
  { to: '/workflow-defs', label: '\u5de5\u4f5c\u6d41', icon: GitBranch },
  { to: '/audit-logs', label: '\u5ba1\u8ba1\u65e5\u5fd7', icon: ScrollText },
  { to: '/jobs', label: '\u8ba1\u5212\u4efb\u52a1', icon: Briefcase },
  { to: '/settings', label: '\u8bbe\u7f6e', icon: Settings },
]

const APP_NAME = '\u5a92\u4f53\u6574\u7406\u5de5\u5177'

export interface SidebarProps {
  collapsed: boolean
  mobileOpen: boolean
  onToggleCollapsed: () => void
  onCloseMobile: () => void
}

function ActiveDot() {
  const dotRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (dotRef.current) {
      gsap.fromTo(
        dotRef.current,
        {
          scale: 0,
          x: -20,
          opacity: 0,
        },
        {
          scale: 1,
          x: 0,
          opacity: 1,
          duration: 0.5,
          ease: 'expo.out',
        },
      )
    }
  }, [])

  return <div ref={dotRef} className="absolute -left-2 h-1.5 w-1.5 rounded-full bg-foreground" />
}

function SidebarNav({
  collapsed,
  onNavigate,
}: {
  collapsed: boolean
  onNavigate?: () => void
}) {
  return (
    <nav className={cn('flex flex-1 flex-col gap-2 p-3', collapsed && 'items-center')}>
      {SIDEBAR_ITEMS.map((item) => {
        const Icon = item.icon
        return (
          <NavLink
            key={item.to}
            to={item.to}
            end={item.end}
            onClick={onNavigate}
            aria-label={item.label}
            title={collapsed ? item.label : undefined}
            className={({ isActive }) =>
              cn(
                'group relative flex border-2 px-3 py-2.5 text-sm font-bold transition-all',
                collapsed ? 'w-12 justify-center' : 'w-full items-center gap-3',
                isActive
                  ? 'border-foreground bg-primary text-primary-foreground shadow-hard -translate-y-0.5'
                  : 'border-transparent text-muted-foreground hover:border-foreground hover:bg-accent hover:text-accent-foreground hover:shadow-hard hover:-translate-y-0.5',
              )
            }
          >
            {({ isActive }) => (
              <>
                <div className="relative flex h-4 w-4 items-center justify-center">
                  {isActive && <ActiveDot />}
                  <Icon className="h-4 w-4" />
                </div>
                {!collapsed && <span className="truncate">{item.label}</span>}
              </>
            )}
          </NavLink>
        )
      })}
    </nav>
  )
}

function ThemeSwitchButton({
  collapsed,
  onClick,
  theme,
}: {
  collapsed: boolean
  onClick: () => void
  theme: 'light' | 'dark'
}) {
  const label = theme === 'dark' ? '\u5207\u6362\u5230\u6d45\u8272' : '\u5207\u6362\u5230\u6df1\u8272'

  return (
    <button
      type="button"
      onClick={onClick}
      aria-label={label}
      title={collapsed ? label : undefined}
      className={cn(
        'flex border-2 border-transparent px-3 py-2 text-sm font-bold text-muted-foreground transition-all hover:border-foreground hover:bg-accent hover:text-accent-foreground hover:shadow-hard hover:-translate-y-0.5',
        collapsed ? 'mx-auto w-12 justify-center' : 'w-full items-center gap-3',
      )}
    >
      {theme === 'dark' ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
      {!collapsed && <span>{label}</span>}
    </button>
  )
}

export function Sidebar({
  collapsed,
  mobileOpen,
  onToggleCollapsed,
  onCloseMobile,
}: SidebarProps) {
  const { theme, toggleTheme } = useThemeStore()

  return (
    <>
      <aside
        className={cn(
          'hidden shrink-0 flex-col border-r-2 border-border bg-muted/30 lg:flex',
          'transition-[width] duration-300 ease-out',
          collapsed ? 'w-20' : 'w-64',
        )}
      >
        <div className={cn('border-b-2 border-border', collapsed ? 'px-3 py-4' : 'px-6 py-5')}>
          <div className={cn('flex items-start gap-2', collapsed ? 'flex-col items-center' : 'justify-between')}>
            <div className={cn('min-w-0', collapsed && 'text-center')}>
              <p className="text-[10px] font-black uppercase tracking-[0.2em] text-foreground">CLASSIFIER</p>
              {!collapsed && <h1 className="mt-2 text-xl font-black tracking-tight">{APP_NAME}</h1>}
            </div>
            <button
              type="button"
              onClick={onToggleCollapsed}
              className="inline-flex h-8 w-8 items-center justify-center border-2 border-foreground bg-background transition-all hover:bg-foreground hover:text-background"
              aria-label={collapsed ? '\u5c55\u5f00\u4fa7\u8fb9\u680f' : '\u6536\u8d77\u4fa7\u8fb9\u680f'}
              title={collapsed ? '\u5c55\u5f00\u4fa7\u8fb9\u680f' : '\u6536\u8d77\u4fa7\u8fb9\u680f'}
            >
              {collapsed ? <ChevronRight className="h-4 w-4" /> : <ChevronLeft className="h-4 w-4" />}
            </button>
          </div>
        </div>
        <SidebarNav collapsed={collapsed} />
        <div className="border-t-2 border-border p-3">
          <ThemeSwitchButton collapsed={collapsed} theme={theme} onClick={toggleTheme} />
        </div>
      </aside>

      <div
        className={cn(
          'fixed inset-0 z-40 bg-black/45 transition-opacity duration-200 lg:hidden',
          mobileOpen ? 'pointer-events-auto opacity-100' : 'pointer-events-none opacity-0',
        )}
        onClick={onCloseMobile}
        aria-hidden={!mobileOpen}
      />

      <aside
        className={cn(
          'fixed inset-y-0 left-0 z-50 flex w-72 max-w-[85vw] flex-col border-r-2 border-border bg-muted/95 backdrop-blur transition-transform duration-300 lg:hidden',
          mobileOpen ? 'translate-x-0' : '-translate-x-full',
        )}
        aria-hidden={!mobileOpen}
      >
        <div className="flex items-start justify-between border-b-2 border-border px-5 py-4">
          <div>
            <p className="text-[10px] font-black uppercase tracking-[0.2em] text-foreground">CLASSIFIER</p>
            <h1 className="mt-1 text-lg font-black tracking-tight">{APP_NAME}</h1>
          </div>
          <button
            type="button"
            onClick={onCloseMobile}
            className="inline-flex h-8 w-8 items-center justify-center border-2 border-foreground bg-background transition-all hover:bg-foreground hover:text-background"
            aria-label={'\u5173\u95ed\u4fa7\u8fb9\u680f'}
            title={'\u5173\u95ed\u4fa7\u8fb9\u680f'}
          >
            <X className="h-4 w-4" />
          </button>
        </div>
        <SidebarNav collapsed={false} onNavigate={onCloseMobile} />
        <div className="border-t-2 border-border p-3">
          <ThemeSwitchButton collapsed={false} theme={theme} onClick={toggleTheme} />
        </div>
      </aside>
    </>
  )
}
