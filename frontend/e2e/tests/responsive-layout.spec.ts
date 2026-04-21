import { expect, test, type Page } from '@playwright/test'

import { json, method, mockEventStream, pathname } from '../support/api'

const LONG_PATH = '/mnt/source/projects/very-long-path/season-collection/ultra-hd-restored-edition'
const TARGET_PATH = '/mnt/target/video/Season-01/episode-01.mkv'

const folder = {
  id: 'folder-1',
  path: `${LONG_PATH}/Season-01`,
  source_dir: LONG_PATH,
  relative_path: 'Season-01',
  name: 'Season-01',
  category: 'video',
  category_source: 'workflow',
  status: 'done',
  image_count: 0,
  video_count: 2,
  other_file_count: 0,
  has_other_files: false,
  total_files: 2,
  total_size: 1024,
  marked_for_move: false,
  deleted_at: null,
  delete_staging_path: null,
  scanned_at: '2026-03-24T00:00:00Z',
  updated_at: '2026-03-24T01:00:00Z',
  workflow_summary: {
    classification: {
      status: 'failed',
      workflow_run_id: 'run-1',
      job_id: 'job-1',
      updated_at: '2026-03-24T01:00:00Z',
    },
    processing: {
      status: 'not_run',
    },
  },
}

const jobs = [
  {
    id: 'job-1',
    type: 'workflow',
    workflow_def_id: 'wf-1',
    status: 'failed',
    folder_ids: ['folder-1'],
    total: 4,
    done: 3,
    failed: 1,
    error: '流程执行失败：目标目录不存在',
    started_at: '2026-03-24T00:00:00Z',
    finished_at: '2026-03-24T00:03:00Z',
    created_at: '2026-03-24T00:00:00Z',
    updated_at: '2026-03-24T00:03:00Z',
  },
]

const workflowDefs = [
  {
    id: 'wf-1',
    name: '超长路径工作流任务',
    graph_json: '{"nodes":[],"edges":[]}',
    is_active: true,
    version: 3,
    created_at: '2026-03-24T00:00:00Z',
    updated_at: '2026-03-24T00:00:00Z',
  },
]

const workflowRuns = [
  {
    id: 'run-1',
    job_id: 'job-1',
    folder_id: 'folder-1',
    workflow_def_id: 'wf-1',
    status: 'failed',
    resume_node_id: null,
    last_node_id: 'pending-node',
    error: '工作流失败：移动节点写入失败',
    started_at: '2026-03-24T00:00:00Z',
    finished_at: '2026-03-24T00:03:00Z',
    created_at: '2026-03-24T00:00:00Z',
    updated_at: '2026-03-24T00:03:00Z',
  },
]

const nodeRuns = [
  {
    id: 'node-run-1',
    workflow_run_id: 'run-1',
    node_id: 'pending-node',
    node_type: 'move-node',
    sequence: 4,
    status: 'failed',
    input_json: '[]',
    output_json: '[]',
    error: '节点移动失败：目标目录不存在',
    progress_percent: 73,
    progress_stage: '移动文件',
    progress_source_path: `${LONG_PATH}/Season-01/episode-01.mkv`,
    started_at: '2026-03-24T00:00:00Z',
    finished_at: '2026-03-24T00:03:00Z',
    created_at: '2026-03-24T00:00:00Z',
  },
]

const auditLogs = [
  {
    id: 'audit-1',
    job_id: 'job-1',
    workflow_run_id: 'run-1',
    node_run_id: 'node-run-1',
    node_id: 'pending-node',
    node_type: 'move-node',
    folder_id: 'folder-1',
    folder_path: `${LONG_PATH}/Season-01/episode-01.mkv`,
    action: 'move',
    level: 'error',
    result: 'failed',
    error_msg: '移动失败：目标目录不存在',
    duration_ms: 12,
    detail: {
      before: { path: `${LONG_PATH}/Season-01/episode-01.mkv` },
      after: { path: TARGET_PATH },
      source_path: `${LONG_PATH}/Season-01/episode-01.mkv`,
      target_path: TARGET_PATH,
    },
    created_at: '2026-03-24T00:02:00Z',
  },
]

const scheduledWorkflows = [
  {
    id: 'scheduled-1',
    name: '超长路径处理流程',
    job_type: 'workflow',
    workflow_def_id: 'wf-1',
    folder_ids: ['folder-1'],
    source_dirs: [LONG_PATH],
    cron_spec: '0 */2 * * *',
    enabled: true,
    last_run_at: '2026-03-24T00:00:00Z',
    created_at: '2026-03-24T00:00:00Z',
    updated_at: '2026-03-24T00:00:00Z',
  },
]

const lineage = {
  folder,
  summary: {
    original_path: `${LONG_PATH}/Season-01`,
    current_path: '/mnt/target/video/Season-01',
    status: 'done',
    category: 'video',
    last_processed_at: '2026-03-24T00:03:00Z',
  },
  graph: {
    nodes: [],
    edges: [],
  },
  flow: {
    source_directory: {
      id: 'src-dir',
      path: `${LONG_PATH}/Season-01`,
      label: '源目录',
      artifact_type: 'source',
    },
    target_directories: [
      {
        id: 'dst-dir',
        path: '/mnt/target/video/Season-01',
        label: '目标目录',
        artifact_type: 'primary',
      },
    ],
    source_files: [
      {
        id: 'src-file-1',
        directory_id: 'src-dir',
        name: 'episode-01.mkv',
        path: `${LONG_PATH}/Season-01/episode-01.mkv`,
        relative_path: 'episode-01.mkv',
        size_bytes: 1024,
      },
    ],
    target_files: [
      {
        id: 'dst-file-1',
        directory_id: 'dst-dir',
        name: 'episode-01.mkv',
        path: TARGET_PATH,
        artifact_type: 'primary',
        node_type: 'move-node',
        workflow_run_id: 'run-1',
        job_id: 'job-1',
      },
    ],
    links: [
      {
        id: 'link-1',
        source_file_id: 'src-file-1',
        target_file_id: 'dst-file-1',
        workflow_run_id: 'run-1',
        job_id: 'job-1',
        node_type: 'move-node',
      },
    ],
  },
  timeline: [
    {
      id: 'timeline-1',
      type: 'move',
      occurred_at: '2026-03-24T00:02:00Z',
      title: '移动完成',
      description: '已移动到目标目录',
      path_from: `${LONG_PATH}/Season-01/episode-01.mkv`,
      path_to: TARGET_PATH,
      workflow_run_id: 'run-1',
      job_id: 'job-1',
      step_type: 'move-node',
    },
  ],
}

const classificationTree = {
  folder_id: 'folder-1',
  path: `${LONG_PATH}/Season-01`,
  name: 'Season-01',
  category: 'video',
  category_source: 'workflow',
  status: 'done',
  has_other_files: false,
  total_files: 2,
  image_count: 0,
  video_count: 2,
  other_file_count: 0,
  files: [
    {
      name: 'episode-01.mkv',
      ext: '.mkv',
      kind: 'video',
      size_bytes: 1024,
    },
  ],
  subtree: [],
}

const config = {
  version: 1,
  scan_cron: '0 */6 * * *',
  scan_input_dirs: [LONG_PATH],
  output_dirs: {
    video: ['/mnt/target/video'],
    manga: ['/mnt/target/manga'],
    photo: ['/mnt/target/photo'],
    other: ['/mnt/target/other'],
    mixed: ['/mnt/target/mixed'],
  },
}

const viewports = [
  { label: 'mobile', width: 390, height: 844 },
  { label: 'tablet', width: 768, height: 1024 },
  { label: 'desktop', width: 1366, height: 768 },
]

const routes = [
  '/',
  '/job-history',
  '/workflow-defs',
  '/audit-logs',
  '/jobs',
  '/settings',
  '/live-classification',
  '/folders/folder-1/lineage',
]

async function mockResponsiveApi(page: Page) {
  await mockEventStream(page)

  await page.route('**/api/**', async (route) => {
    const path = pathname(route)
    const reqMethod = method(route)

    if (!path.startsWith('/api/')) {
      await route.continue()
      return
    }

    if (path === '/api/folders' && reqMethod === 'GET') {
      await route.fulfill(json({ data: [folder], total: 1, page: 1, limit: 20 }))
      return
    }
    if (path === '/api/folders/folder-1' && reqMethod === 'GET') {
      await route.fulfill(json({ data: folder }))
      return
    }
    if (path === '/api/folders/folder-1/classification-tree' && reqMethod === 'GET') {
      await route.fulfill(json({ data: classificationTree }))
      return
    }
    if (path === '/api/folders/folder-1/lineage' && reqMethod === 'GET') {
      await route.fulfill(json(lineage))
      return
    }
    if (path === '/api/jobs' && reqMethod === 'GET') {
      await route.fulfill(json({ data: jobs, total: jobs.length, page: 1, limit: 20 }))
      return
    }
    if (path === '/api/jobs/job-1/progress' && reqMethod === 'GET') {
      await route.fulfill(
        json({
          job_id: 'job-1',
          status: 'failed',
          done: 3,
          total: 4,
          failed: 1,
          updated_at: '2026-03-24T00:03:00Z',
        }),
      )
      return
    }
    if (path === '/api/jobs/job-1/workflow-runs' && reqMethod === 'GET') {
      await route.fulfill(json({ data: workflowRuns, total: workflowRuns.length, page: 1, limit: 100 }))
      return
    }
    if (path === '/api/workflow-runs/run-1' && reqMethod === 'GET') {
      await route.fulfill(
        json({
          data: workflowRuns[0],
          node_runs: nodeRuns,
          review_summary: {
            total: 0,
            pending: 0,
            approved: 0,
            rolled_back: 0,
            rejected: 0,
            failed_step_runs: 1,
          },
        }),
      )
      return
    }
    if (path === '/api/workflow-runs/run-1/reviews' && reqMethod === 'GET') {
      await route.fulfill(
        json({
          data: [],
          summary: {
            total: 0,
            pending: 0,
            approved: 0,
            rolled_back: 0,
            rejected: 0,
            failed_step_runs: 1,
          },
        }),
      )
      return
    }
    if (path === '/api/workflow-defs' && reqMethod === 'GET') {
      await route.fulfill(
        json({
          data: workflowDefs,
          total: workflowDefs.length,
          page: 1,
          limit: 100,
        }),
      )
      return
    }
    if (path === '/api/audit-logs' && reqMethod === 'GET') {
      await route.fulfill(json({ data: auditLogs, total: auditLogs.length, page: 1, limit: 50 }))
      return
    }
    if (path === '/api/scheduled-workflows' && reqMethod === 'GET') {
      await route.fulfill(
        json({
          data: scheduledWorkflows,
          total: scheduledWorkflows.length,
          page: 1,
          limit: 100,
        }),
      )
      return
    }
    if (path === '/api/config' && reqMethod === 'GET') {
      await route.fulfill(json({ data: config }))
      return
    }

    await route.fulfill(json({ error: `unhandled ${reqMethod} ${path}` }, 404))
  })
}

async function expectNoHorizontalOverflow(page: Page, label: string) {
  const metrics = await page.evaluate(() => ({
    scrollWidth: document.documentElement.scrollWidth,
    clientWidth: document.documentElement.clientWidth,
  }))

  expect(metrics.scrollWidth, label).toBeLessThanOrEqual(metrics.clientWidth)
}

test('responsive routes avoid horizontal overflow', async ({ page }) => {
  await mockResponsiveApi(page)

  for (const viewport of viewports) {
    await page.setViewportSize({ width: viewport.width, height: viewport.height })
    for (const route of routes) {
      await page.goto(route)
      await page.waitForLoadState('domcontentloaded')
      await page.waitForTimeout(120)
      await expectNoHorizontalOverflow(page, `${viewport.label} ${route}`)
    }
  }
})

test('mobile sidebar opens, closes, and closes after navigation', async ({ page }) => {
  await mockResponsiveApi(page)
  await page.setViewportSize({ width: 390, height: 844 })

  await page.goto('/')
  const openSidebarButton = page.getByRole('button', { name: '打开侧边栏' })
  const closeSidebarButton = page.getByRole('button', { name: '关闭侧边栏' })

  await openSidebarButton.click()
  await expect(closeSidebarButton).toBeVisible()
  await page.getByRole('link', { name: '执行历史' }).click()

  await expect(page).toHaveURL(/\/job-history$/)
  await expect(closeSidebarButton).toBeHidden()

  await openSidebarButton.click()
  await closeSidebarButton.click()
  await expect(closeSidebarButton).toBeHidden()
})

test('desktop sidebar can collapse and expand while content remains visible', async ({ page }) => {
  await mockResponsiveApi(page)
  await page.setViewportSize({ width: 1366, height: 768 })

  await page.goto('/workflow-defs')
  await expect(page.getByRole('heading', { name: '工作流管理' })).toBeVisible()

  await page.getByRole('button', { name: '收起侧边栏' }).click()
  await expect(page.getByRole('button', { name: '展开侧边栏' })).toBeVisible()
  await expect(page.getByRole('heading', { name: '工作流管理' })).toBeVisible()

  await page.getByRole('button', { name: '展开侧边栏' }).click()
  await expect(page.getByRole('button', { name: '收起侧边栏' })).toBeVisible()
  await expectNoHorizontalOverflow(page, 'desktop workflow-defs after sidebar toggle')
})

test('mobile cards keep key fields visible', async ({ page }) => {
  await mockResponsiveApi(page)
  await page.setViewportSize({ width: 390, height: 844 })

  await page.goto('/jobs')
  await expect(page.getByText('0 */2 * * *')).toBeVisible()
  await expect(page.getByText('已启用')).toBeVisible()
  await expect(page.getByRole('button', { name: '立即执行' })).toBeVisible()

  await page.goto('/audit-logs')
  await expect(page.getByText('移动失败：目标目录不存在')).toBeVisible()
  await expect(page.getByText(TARGET_PATH)).toBeVisible()

  await page.goto('/job-history')
  await page.getByText('超长路径工作流任务').click()
  await page.getByText('目录ID：folder-1').click()
  await expect(page.getByText('move-node')).toBeVisible()
  await expect(page.getByText('节点移动失败：目标目录不存在')).toBeVisible()

  await page.goto('/folders/folder-1/lineage')
  await expect(page.getByText(TARGET_PATH).first()).toBeVisible()
  await expect(page.getByText(`${LONG_PATH}/Season-01/episode-01.mkv`).first()).toBeVisible()
})
