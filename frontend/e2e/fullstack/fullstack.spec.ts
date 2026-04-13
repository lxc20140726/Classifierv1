import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { expect, test } from '@playwright/test'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const repoRoot = path.resolve(__dirname, '../../..')
const sourceDir = path.join(repoRoot, '.e2e-fullstack/source')
const targetDir = path.join(repoRoot, '.e2e-fullstack/target')

test.describe.configure({ mode: 'serial' })

async function saveConfig(request: import('@playwright/test').APIRequestContext) {
  const response = await request.put('/api/config', {
    data: {
      scan_input_dirs: JSON.stringify([sourceDir]),
      source_dir: sourceDir,
      target_dir: targetDir,
    },
  })
  expect(response.ok()).toBeTruthy()
}

async function scanUntilFoldersExist(request: import('@playwright/test').APIRequestContext) {
  await saveConfig(request)
  const scanResponse = await request.post('/api/folders/scan')
  expect(scanResponse.ok()).toBeTruthy()

  await expect
    .poll(async () => {
      const response = await request.get('/api/folders?limit=50')
      if (!response.ok()) return []
      const body = (await response.json()) as { data: Array<Record<string, unknown>> }
      return body.data.map((folder) => String(folder.name ?? folder.Name ?? '')).sort()
    }, { timeout: 15000 })
    .toEqual(['manga-comic', 'manual-only', 'photo-album'])
}

async function getFolders(request: import('@playwright/test').APIRequestContext) {
  const response = await request.get('/api/folders?limit=50')
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as { data: Array<Record<string, unknown>> }
  return {
    data: body.data.map((folder) => ({
      id: String(folder.id ?? folder.ID ?? ''),
      name: String(folder.name ?? folder.Name ?? ''),
      category: String(folder.category ?? folder.Category ?? ''),
    })),
  }
}

async function getTopLevelFolders(request: import('@playwright/test').APIRequestContext) {
  const response = await request.get('/api/folders?limit=100&top_level_only=true')
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as { data: Array<Record<string, unknown>> }
  return body.data.map((folder) => ({
    id: String(folder.id ?? folder.ID ?? ''),
    name: String(folder.name ?? folder.Name ?? ''),
    workflowSummary: (folder.workflow_summary ?? folder.WorkflowSummary ?? {}) as Record<string, unknown>,
  }))
}

async function getClassificationWorkflowDef(request: import('@playwright/test').APIRequestContext) {
  const response = await request.get('/api/workflow-defs?limit=100')
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as {
    data: Array<Record<string, unknown>>
  }
  const workflowDef = body.data.find((item) => {
    const graphJSON = String(item.graph_json ?? item.GraphJSON ?? '')
    return graphJSON.includes('"folder-picker"') && graphJSON.includes('"classification-writer"')
  })
  expect(workflowDef).toBeTruthy()
  return workflowDef!
}

test('workflow definition CRUD works against the real backend', async ({ page }) => {
  const uniqueName = `全链路工作流-${Date.now()}`
  const updatedName = `${uniqueName}-已更新`

  await page.goto('/')
  await page.getByRole('link', { name: '工作流' }).click()
  await expect(page.getByRole('heading', { name: '工作流定义' })).toBeVisible()
  await expect(page.getByText('默认分类流程')).toBeVisible()

  await page.getByRole('button', { name: '新建' }).click()
  await page.getByPlaceholder('工作流名称').fill(uniqueName)
  await page.locator('textarea').fill('{"nodes":[{"id":"n1","type":"trigger","config":{},"enabled":true}],"edges":[]}')
  await page.getByRole('button', { name: '保存' }).click()
  await expect(page.getByText(uniqueName)).toBeVisible()

  const row = page.locator('tr').filter({ hasText: uniqueName })
  await row.getByRole('button', { name: '编辑' }).click()
  await page.getByPlaceholder('工作流名称').fill(updatedName)
  await page.locator('textarea').fill('{"nodes":[{"id":"n1","type":"trigger","config":{},"enabled":true},{"id":"n2","type":"move","config":{},"enabled":true}],"edges":[{"id":"e1","source":"n1","source_port":0,"target":"n2","target_port":0}]}')
  await page.getByRole('button', { name: '保存' }).click()
  await expect(page.getByText(updatedName)).toBeVisible()

  const updatedRow = page.locator('tr').filter({ hasText: updatedName })
  await expect(updatedRow.getByText('已激活')).toBeVisible()

  page.once('dialog', (dialog) => dialog.accept())
  await updatedRow.getByRole('button', { name: '删除' }).click()
  await expect(page.getByText(updatedName)).toHaveCount(0)
})

test('scan flow creates folders and snapshot drawer can revert a real snapshot', async ({ page, request }) => {
  await scanUntilFoldersExist(request)

  await page.goto('/')
  await expect(page.getByRole('heading', { name: '媒体文件夹' })).toBeVisible()
  await expect(page.locator('[title="photo-album"]')).toBeVisible()

  const row = page.locator('tr').filter({ hasText: 'photo-album' })
  await row.locator('[title="查看快照时间线"]').click()

  await expect(page.getByRole('heading', { name: '文件夹操作时间线' })).toBeVisible()
  await expect(page.getByText('分类记录')).toBeVisible()
  await page.getByRole('button', { name: '回退到此节点' }).first().click()
  await expect(page.getByText('已回退')).toBeVisible()
})

test('legacy removed node type fails at runtime', async ({ request }) => {
  await scanUntilFoldersExist(request)
  const folders = await getFolders(request)
  const manualFolder = folders.data.find((folder) => folder.name === 'manual-only')
  expect(manualFolder).toBeTruthy()

  const createWorkflowResponse = await request.post('/api/workflow-defs', {
    data: {
      name: `人工确认-${Date.now()}`,
      graph_json: JSON.stringify({
        nodes: [
          { id: 'manual', type: 'manual-classifier', config: {}, enabled: true },
        ],
        edges: [],
      }),
    },
  })
  expect(createWorkflowResponse.ok()).toBeTruthy()
  const workflowDef = (await createWorkflowResponse.json()) as { data: { id: string } }

  const startJobResponse = await request.post('/api/jobs', {
    data: {
      workflow_def_id: workflowDef.data.id,
      folder_ids: [manualFolder!.id],
    },
  })
  expect(startJobResponse.ok()).toBeTruthy()
  const startJobBody = (await startJobResponse.json()) as { job_id: string }
  let workflowRunId = ''
  await expect
    .poll(async () => {
      const response = await request.get(`/api/jobs/${startJobBody.job_id}/workflow-runs?limit=20`)
      if (!response.ok()) return ''
      const body = (await response.json()) as {
        data: Array<{ id: string; status: string }>
      }
      workflowRunId = body.data[0]?.id ?? ''
      return body.data[0]?.status ?? ''
    }, { timeout: 15000 })
    .toBe('failed')

  const detailResponse = await request.get(`/api/workflow-runs/${workflowRunId}`)
  expect(detailResponse.ok()).toBeTruthy()
  const detail = (await detailResponse.json()) as {
    data: { status: string }
    node_runs: Array<{ node_type: string; error: string }>
  }
  expect(detail.data.status).toBe('failed')
  expect(detail.node_runs.length).toBeGreaterThan(0)
  expect(detail.node_runs[0].node_type).toBe('manual-classifier')
  expect(detail.node_runs[0].error).toContain('executor not found')

  const refreshedFolders = await getFolders(request)
  const refreshedManualFolder = refreshedFolders.data.find((folder) => folder.id === manualFolder!.id)
  expect(refreshedManualFolder?.category).toBe('pending')
})

test('classification workflow binds top-level folder summary and timeline', async ({ request }) => {
  await scanUntilFoldersExist(request)

  const topLevelFolders = await getTopLevelFolders(request)
  const rootFolder = topLevelFolders.find((folder) => folder.name === 'series-pack')
  expect(rootFolder).toBeTruthy()

  const workflowDef = await getClassificationWorkflowDef(request)
  const graph = JSON.parse(String(workflowDef.graph_json ?? workflowDef.GraphJSON ?? '')) as {
    nodes: Array<{ id: string; type: string; config?: Record<string, unknown> }>
    edges: Array<Record<string, unknown>>
  }
  const pickerNode = graph.nodes.find((node) => node.type === 'folder-picker')
  expect(pickerNode).toBeTruthy()
  pickerNode!.config = {
    ...(pickerNode!.config ?? {}),
    source_mode: 'folders',
    saved_folder_id: rootFolder!.id,
    saved_folder_ids: [rootFolder!.id],
    folder_ids: [rootFolder!.id],
  }

  const updateResponse = await request.put(`/api/workflow-defs/${String(workflowDef.id ?? workflowDef.ID ?? '')}`, {
    data: {
      name: String(workflowDef.name ?? workflowDef.Name ?? ''),
      description: String(workflowDef.description ?? workflowDef.Description ?? ''),
      graph_json: JSON.stringify(graph),
      is_active: Boolean(workflowDef.is_active ?? workflowDef.IsActive ?? true),
      version: Number(workflowDef.version ?? workflowDef.Version ?? 1),
    },
  })
  expect(updateResponse.ok()).toBeTruthy()

  const startJobResponse = await request.post('/api/jobs', {
    data: {
      workflow_def_id: String(workflowDef.id ?? workflowDef.ID ?? ''),
    },
  })
  expect(startJobResponse.ok()).toBeTruthy()
  const startJobBody = (await startJobResponse.json()) as { job_id: string }

  let workflowRunId = ''
  await expect
    .poll(async () => {
      const response = await request.get(`/api/jobs/${startJobBody.job_id}/workflow-runs?limit=20`)
      if (!response.ok()) return ''
      const body = (await response.json()) as {
        data: Array<{ id: string; status: string }>
      }
      workflowRunId = body.data[0]?.id ?? ''
      return body.data[0]?.status ?? ''
    }, { timeout: 20000 })
    .toBe('succeeded')

  await expect
    .poll(async () => {
      const folders = await getTopLevelFolders(request)
      const refreshedRoot = folders.find((folder) => folder.id === rootFolder!.id)
      const classification = (refreshedRoot?.workflowSummary.classification ?? {}) as Record<string, unknown>
      return {
        status: String(classification.status ?? ''),
        workflowRunID: String(classification.workflow_run_id ?? ''),
      }
    }, { timeout: 20000 })
    .toEqual({
      status: 'succeeded',
      workflowRunID: workflowRunId,
    })

  await expect
    .poll(async () => {
      const response = await request.get(`/api/audit-logs?folder_id=${rootFolder!.id}&action=workflow.run.complete&limit=20`)
      if (!response.ok()) return []
      const body = (await response.json()) as {
        data: Array<{ workflow_run_id: string }>
      }
      return body.data.map((item) => item.workflow_run_id)
    }, { timeout: 20000 })
    .toContain(workflowRunId)
})
