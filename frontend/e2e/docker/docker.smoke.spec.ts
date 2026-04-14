import { expect, test } from '@playwright/test'

test.describe.configure({ mode: 'serial' })

test('[docker,smoke] 容器黑盒全链路基础冒烟', async ({ page, request }) => {
  const healthResponse = await request.get('/health')
  expect(healthResponse.ok()).toBeTruthy()

  const scanResponse = await request.post('/api/folders/scan')
  expect(scanResponse.ok()).toBeTruthy()

  await expect
    .poll(async () => {
      const foldersResponse = await request.get('/api/folders?limit=50')
      if (!foldersResponse.ok()) return 0
      const body = (await foldersResponse.json()) as { data?: unknown[] }
      return Array.isArray(body.data) ? body.data.length : 0
    }, { timeout: 30000 })
    .toBeGreaterThan(0)

  await page.goto('/')
  await expect(page.getByRole('heading', { name: '媒体文件夹' })).toBeVisible()
})
