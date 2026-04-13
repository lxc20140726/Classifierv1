import { useEffect, useRef } from 'react'
import { Outlet, useLocation } from 'react-router-dom'
import gsap from 'gsap'

import { Sidebar } from '@/components/Sidebar'
import { ToastList } from '@/components/ToastList'

export function Layout() {
  const location = useLocation()
  const mainRef = useRef<HTMLElement | null>(null)

  useEffect(() => {
    if (mainRef.current) {
      gsap.fromTo(
        mainRef.current,
        { opacity: 0, y: 10 },
        { opacity: 1, y: 0, duration: 0.4, ease: 'power2.out' }
      )
    }
  }, [location.pathname])

  return (
    <div className="flex min-h-screen bg-background text-foreground">
      <Sidebar />
      <main ref={mainRef} className="relative flex-1 overflow-auto p-6 md:p-10">
        <Outlet />
      </main>
      <ToastList />
    </div>
  )
}
