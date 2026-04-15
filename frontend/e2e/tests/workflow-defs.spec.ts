import { expect, test } from '@playwright/test'

import { json, method, mockEventStream, pathname } from '../support/api'

test('workflow definition CRUD works end-to-end in UI', async ({ page }) => {
  await mockEventStream(page)

  let defs = [
    {
      id: 'wf-1',
      name: '默认分类流程',
      graph_json: '{"nodes":[],"edges":[]}',
      is_active: true,
      version: 1,
      created_at: '2026-03-24T00:00:00Z',
      updated_at: '2026-03-24T00:00:00Z',
    },
  ]

  await page.route('**/api/workflow-defs**', async (route) => {
    const path = pathname(route)
    const reqMethod = method(route)

    if (path === '/api/workflow-defs' && reqMethod === 'GET') {
      await route.fulfill(json({ data: defs, total: defs.length, page: 1, limit: 100 }))
      return
    }

    if (path === '/api/workflow-defs' && reqMethod === 'POST') {
      const payload = JSON.parse(route.request().postData() ?? '{}') as { name: string; graph_json: string }
      const created = {
        id: 'wf-2',
        name: payload.name,
        graph_json: payload.graph_json,
        is_active: false,
        version: 1,
        created_at: '2026-03-25T00:00:00Z',
        updated_at: '2026-03-25T00:00:00Z',
      }
      defs = [...defs, created]
      await route.fulfill(json({ data: created }, 201))
      return
    }

    const id = path.split('/').pop() ?? ''
    const current = defs.find((item) => item.id === id)

    if (reqMethod === 'PUT' && current) {
      const payload = JSON.parse(route.request().postData() ?? '{}') as Partial<typeof current>
      defs = defs.map((item) =>
        item.id === id
          ? {
              ...item,
              ...payload,
              updated_at: '2026-03-26T00:00:00Z',
            }
          : item,
      )
      await route.fulfill(json({ data: defs.find((item) => item.id === id) }))
      return
    }

    if (reqMethod === 'DELETE' && current) {
      defs = defs.filter((item) => item.id !== id)
      await route.fulfill(json({ deleted: true }))
      return
    }

    await route.fulfill(json({ error: 'not found' }, 404))
  })

  await page.goto('/workflow-defs')

  await expect(page.getByRole('heading', { name: '工作流管理' })).toBeVisible()
  await expect(page.getByText('默认分类流程')).toBeVisible()

  await page.getByRole('button', { name: '新建' }).click()
  await expect(page.getByRole('heading', { name: '选择模板' })).toBeVisible()
  await page.getByRole('button', { name: '空白' }).click()
  await page.getByPlaceholder('工作流名称').fill('新建流程')
  await page.locator('textarea').fill('{"nodes":[{"id":"n1","type":"trigger"}],"edges":[]}')
  await page.getByRole('button', { name: '保存' }).click()
  await expect(page.getByText('新建流程')).toBeVisible()

  await page.getByRole('button', { name: '编辑' }).nth(1).click()
  await page.getByPlaceholder('工作流名称').fill('已更新流程')
  await page.getByRole('button', { name: '保存' }).click()
  await expect(page.getByText('已更新流程')).toBeVisible()

  await page.getByRole('button', { name: '设为激活' }).click()
  await expect(page.getByText('已激活')).toHaveCount(1)
  await expect(page.getByText('已更新流程')).toBeVisible()

  page.once('dialog', (dialog) => dialog.accept())
  await page.getByRole('button', { name: '删除' }).nth(1).click()
  await expect(page.getByText('已更新流程')).toHaveCount(0)
})
