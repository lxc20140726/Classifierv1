export default function NotFoundPage() {
  return (
    <section className="flex min-h-[60vh] flex-col items-center justify-center gap-3 text-center">
      <p className="text-sm uppercase tracking-[0.24em] text-muted-foreground">404</p>
      <h2 className="text-4xl font-semibold tracking-tight">页面不存在</h2>
      <p className="max-w-md text-sm text-muted-foreground">
        当前地址没有对应页面，请从侧边栏返回有效功能页。
      </p>
    </section>
  )
}
