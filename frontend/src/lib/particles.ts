import gsap from 'gsap'

interface ParticleOptions {
  x?: number
  y?: number
  element?: HTMLElement | null
  color?: string
  count?: number
  radius?: number
}

export function triggerParticles({ 
  x, 
  y, 
  element, 
  color = 'hsl(var(--foreground))', 
  count = 12,
  radius = 80
}: ParticleOptions) {
  let startX = x ?? window.innerWidth / 2
  let startY = y ?? window.innerHeight / 2

  if (element) {
    const rect = element.getBoundingClientRect()
    startX = rect.left + rect.width / 2
    startY = rect.top + rect.height / 2
  }

  const container = document.createElement('div')
  container.style.position = 'fixed'
  container.style.left = '0'
  container.style.top = '0'
  container.style.width = '100%'
  container.style.height = '100%'
  container.style.pointerEvents = 'none'
  container.style.zIndex = '9999'
  document.body.appendChild(container)

  const particles: HTMLDivElement[] = []

  // 有序的环形分布
  for (let i = 0; i < count; i++) {
    const angle = (i / count) * Math.PI * 2
    const dot = document.createElement('div')
    dot.style.position = 'absolute'
    dot.style.left = `${startX}px`
    dot.style.top = `${startY}px`
    dot.style.width = '8px'
    dot.style.height = '8px'
    dot.style.backgroundColor = color
    dot.style.borderRadius = '50%'
    
    // 初始状态：聚拢在中心，缩放为0
    gsap.set(dot, { scale: 0, xPercent: -50, yPercent: -50 })
    
    container.appendChild(dot)
    particles.push(dot)

    // 目标位置
    const targetX = Math.cos(angle) * radius
    const targetY = Math.sin(angle) * radius

    // 优化的贝塞尔曲线动画 (极速爆发后缓慢消失)
    gsap.to(dot, {
      x: `+=${targetX}`,
      y: `+=${targetY}`,
      scale: 1.2,
      duration: 0.5,
      ease: 'expo.out', // 极速爆发
      delay: i * 0.015, // 有序的错落感
      onComplete: () => {
        gsap.to(dot, {
          scale: 0,
          opacity: 0,
          duration: 0.3,
          ease: 'power2.in',
        })
      }
    })
  }

  // 清理
  setTimeout(() => {
    container.remove()
  }, 1500)
}
