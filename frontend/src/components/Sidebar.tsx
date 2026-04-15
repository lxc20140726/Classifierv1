import { useEffect, useRef } from 'react'
import { Briefcase, FolderClock, FolderKanban, GitBranch, Moon, ScrollText, Settings, Sun } from 'lucide-react'
import { NavLink } from 'react-router-dom'
import gsap from 'gsap'

import { cn } from '@/lib/utils'
import { useThemeStore } from '@/store/themeStore'

const navItems = [
  { to: '/', label: '文件夹', icon: FolderKanban, end: true },
  { to: '/live-classification', label: '实时分类', icon: FolderKanban },
  { to: '/jobs', label: '计划任务', icon: Briefcase },
  { to: '/job-history', label: '执行历史', icon: FolderClock },
  { to: '/workflow-defs', label: '工作流', icon: GitBranch },
  { to: '/audit-logs', label: '审计日志', icon: ScrollText },
  { to: '/settings', label: '设置', icon: Settings },
]

const sidebarNavItems = navItems.filter((item) => item.to !== '/live-classification')

function ActiveDot() {
  const dotRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (dotRef.current) {
      gsap.fromTo(dotRef.current,
        { 
          scale: 0,
          x: -20,
          opacity: 0
        },
        { 
          scale: 1, 
          x: 0,
          opacity: 1,
          duration: 0.5, 
          ease: "expo.out" 
        }
      )
    }
  }, [])

  return <div ref={dotRef} className="absolute -left-3 h-1.5 w-1.5 rounded-full bg-foreground" />
}

export function Sidebar() {
  const { theme, toggleTheme } = useThemeStore()

  return (
    <aside className="flex w-64 shrink-0 flex-col border-r-2 border-border bg-muted/30">
      <div className="border-b-2 border-border px-6 py-5">
        <p className="text-xs font-bold uppercase tracking-[0.24em] text-foreground">
          CLASSIFIER
        </p>
        <h1 className="mt-2 text-xl font-black tracking-tight">媒体整理工具</h1>
      </div>
      <nav className="flex flex-1 flex-col gap-3 p-4">
        {sidebarNavItems.map((item) => {
          const Icon = item.icon

          return (
            <NavLink
              key={item.to}
              to={item.to}
              end={item.end}
              className={({ isActive }) =>
                cn(
                  'group flex items-center gap-3 border-2 px-3 py-2 text-sm font-bold transition-all',
                  isActive
                    ? 'border-foreground bg-primary text-primary-foreground shadow-hard translate-y-[-2px]'
                    : 'border-transparent text-muted-foreground hover:border-foreground hover:bg-accent hover:text-accent-foreground hover:shadow-hard hover:translate-y-[-2px]',
                )
              }
            >
              {({ isActive }) => (
                <>
                  <div className="relative flex items-center justify-center">
                    {isActive && <ActiveDot />}
                    <Icon className="h-4 w-4" />
                  </div>
                  <span>{item.label}</span>
                </>
              )}
            </NavLink>
          )
        })}
      </nav>
      <div className="border-t-2 border-border p-4">
        <button
          type="button"
          onClick={toggleTheme}
          className="flex w-full items-center gap-3 border-2 border-transparent px-3 py-2 text-sm font-bold text-muted-foreground transition-all hover:border-foreground hover:bg-accent hover:text-accent-foreground hover:shadow-hard hover:translate-y-[-2px]"
        >
          {theme === 'dark' ? <Sun className="h-4 w-4" /> : <Moon className="h-4 w-4" />}
          <span>{theme === 'dark' ? '切换到浅色' : '切换到暗色'}</span>
        </button>
      </div>
    </aside>
  )
}
