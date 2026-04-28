import { useEffect, useRef, useState } from 'react'
import { Menu } from 'lucide-react'
import { Outlet, useLocation } from 'react-router-dom'
import gsap from 'gsap'

import { Sidebar } from '@/components/Sidebar'
import { ToastList } from '@/components/ToastList'

function resolvePageTitle(pathname: string) {
  if (pathname.startsWith('/job-history')) return '\u6267\u884c\u5386\u53f2'
  if (pathname.startsWith('/workflow-defs')) return '\u5de5\u4f5c\u6d41'
  if (pathname.startsWith('/audit-logs')) return '\u5ba1\u8ba1\u65e5\u5fd7'
  if (pathname.startsWith('/jobs')) return '\u8ba1\u5212\u4efb\u52a1'
  if (pathname.startsWith('/settings')) return '\u8bbe\u7f6e'
  if (pathname.startsWith('/folders/') && pathname.endsWith('/live-classification')) {
    return '\u5b9e\u65f6\u5206\u7c7b'
  }
  if (pathname.startsWith('/folders/') && pathname.endsWith('/lineage')) {
    return '\u8def\u5f84\u6eaf\u6e90'
  }
  return '\u6587\u4ef6\u5939'
}

export function Layout() {
  const location = useLocation()
  const mainRef = useRef<HTMLElement | null>(null)
  const [desktopSidebarCollapsed, setDesktopSidebarCollapsed] = useState(false)
  const [mobileSidebarOpen, setMobileSidebarOpen] = useState(false)
  const [mobileSidebarPath, setMobileSidebarPath] = useState(location.pathname)
  const isMobileSidebarOpen = mobileSidebarOpen && mobileSidebarPath === location.pathname
  const pageTitle = resolvePageTitle(location.pathname)

  useEffect(() => {
    if (mainRef.current) {
      gsap.fromTo(
        mainRef.current,
        { opacity: 0, y: 10 },
        { opacity: 1, y: 0, duration: 0.4, ease: 'power2.out' },
      )
    }
  }, [location.pathname])

  useEffect(() => {
    if (typeof document === 'undefined') return
    const originalOverflow = document.body.style.overflow
    if (isMobileSidebarOpen) {
      document.body.style.overflow = 'hidden'
    }
    return () => {
      document.body.style.overflow = originalOverflow
    }
  }, [isMobileSidebarOpen])

  return (
    <div className="relative flex min-h-screen min-w-0 max-w-full overflow-x-hidden bg-background text-foreground">
      <Sidebar
        collapsed={desktopSidebarCollapsed}
        mobileOpen={isMobileSidebarOpen}
        onToggleCollapsed={() => setDesktopSidebarCollapsed((prev) => !prev)}
        onCloseMobile={() => setMobileSidebarOpen(false)}
      />
      <header className="fixed left-4 right-4 top-4 z-40 flex items-center justify-between border-2 border-foreground bg-card px-3 py-2 shadow-hard sm:left-5 sm:right-5 lg:hidden">
        <button
          type="button"
          onClick={() => {
            setMobileSidebarPath(location.pathname)
            setMobileSidebarOpen(true)
          }}
          className="inline-flex h-9 w-9 items-center justify-center border-2 border-foreground bg-background transition-all hover:bg-foreground hover:text-background"
          aria-label={'\u6253\u5f00\u4fa7\u8fb9\u680f'}
          title={'\u6253\u5f00\u4fa7\u8fb9\u680f'}
        >
          <Menu className="h-4 w-4" />
        </button>
        <div className="min-w-0 text-right">
          <p className="text-[10px] font-black uppercase tracking-[0.2em] text-muted-foreground">CLASSIFIER</p>
          <p className="truncate text-sm font-black">{pageTitle}</p>
        </div>
      </header>
      <main
        ref={mainRef}
        className="relative flex-1 min-w-0 max-w-full overflow-y-auto overflow-x-hidden px-4 pb-4 pt-[5.75rem] sm:px-5 sm:pb-5 sm:pt-[6rem] md:px-6 md:pb-6 md:pt-[6rem] lg:px-8 lg:py-8"
      >
        <Outlet />
      </main>
      <ToastList />
    </div>
  )
}
