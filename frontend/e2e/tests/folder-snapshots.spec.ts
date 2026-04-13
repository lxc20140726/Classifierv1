import { expect, test } from '@playwright/test'

import { json, method, mockEventStream, pathname } from '../support/api'

test('folder page opens snapshot drawer and reverts a committed snapshot', async ({ page }) => {
  await mockEventStream(page)

  const folder = {
    id: 'folder-1',
    path: '/source/album',
    source_dir: '/source',
    relative_path: 'album',
    name: 'album',
    category: 'photo',
    category_source: 'auto',
    status: 'pending',
    image_count: 12,
    video_count: 0,
    total_files: 12,
    total_size: 1200,
    marked_for_move: false,
    deleted_at: null,
    delete_staging_path: null,
    scanned_at: '2026-03-24T00:00:00Z',
    updated_at: '2026-03-24T00:00:00Z',
  }

  let snapshots = [
    {
      id: 'snap-1',
      job_id: 'job-1',
      folder_id: 'folder-1',
      operation_type: 'classify',
      before: [{ original_path: '/source/album', current_path: '/source/album' }],
      after: [{ original_path: '/source/album', current_path: '/target/photo/album' }],
      detail: { category: 'photo', source_path: '/source/album' },
      status: 'committed',
      created_at: '2026-03-24T00:00:00Z',
    },
  ]

  await page.route('**/api/**', async (route) => {
    const path = pathname(route)
    const reqMethod = method(route)

    if (path === '/api/folders' && reqMethod === 'GET') {
      await route.fulfill(json({ data: [folder], total: 1, page: 1, limit: 20 }))
      return
    }
    if (path === '/api/jobs' && reqMethod === 'GET') {
      await route.fulfill(json({ data: [], total: 0, page: 1, limit: 5 }))
      return
    }
    if (path === '/api/audit-logs' && reqMethod === 'GET') {
      await route.fulfill(json({ data: [], total: 0, page: 1, limit: 5 }))
      return
    }
    if (path === '/api/snapshots' && reqMethod === 'GET') {
      await route.fulfill(json({ data: snapshots }))
      return
    }
    if (path === '/api/snapshots/snap-1/revert' && reqMethod === 'POST') {
      snapshots = snapshots.map((item) => (item.id === 'snap-1' ? { ...item, status: 'reverted' } : item))
      await route.fulfill(json({ reverted: true, revert_result: { ok: true, current_state: [] } }))
      return
    }

    await route.fulfill(json({ error: 'unhandled' }, 404))
  })

  await page.goto('/')
  await expect(page.getByRole('heading', { name: '媒体文件夹' })).toBeVisible()
  await page.getByTitle('查看快照时间线').click()
  await expect(page.getByRole('heading', { name: '文件夹操作时间线' })).toBeVisible()
  await expect(page.getByText('分类记录')).toBeVisible()
  await page.getByRole('button', { name: '回退到此节点' }).click()
  await expect(page.getByText('已回退')).toBeVisible()
})
