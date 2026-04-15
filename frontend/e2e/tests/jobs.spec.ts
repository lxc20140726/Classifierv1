import { expect, test } from '@playwright/test'

import { json, method, mockEventStream, pathname } from '../support/api'

test('jobs page expands workflow runs and submits waiting input', async ({ page }) => {
  await mockEventStream(page)

  const jobs = [
    {
      id: 'job-1',
      type: 'workflow',
      workflow_def_id: 'wf-1',
      status: 'running',
      folder_ids: ['folder-1'],
      total: 1,
      done: 0,
      failed: 0,
      error: '',
      started_at: '2026-03-24T00:00:00Z',
      finished_at: null,
      created_at: '2026-03-24T00:00:00Z',
      updated_at: '2026-03-24T00:00:00Z',
    },
  ]

  const workflowDefs = [
    {
      id: 'wf-1',
      name: '测试流程',
      graph_json: '{"nodes":[],"edges":[]}',
      is_active: true,
      version: 1,
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
      status: 'waiting_input',
      resume_node_id: 'manual',
      last_node_id: 'pending-node',
      error: '',
      started_at: '2026-03-24T00:00:00Z',
      finished_at: null,
      created_at: '2026-03-24T00:00:00Z',
      updated_at: '2026-03-24T00:00:00Z',
    },
  ]

  const nodeRuns = [
    {
      id: 'node-run-1',
      workflow_run_id: 'run-1',
      node_id: 'pending-node',
      node_type: 'move-node',
      sequence: 4,
      status: 'waiting_input',
      input_json: '[]',
      output_json: '[]',
      error: '',
      started_at: '2026-03-24T00:00:00Z',
      finished_at: '2026-03-24T00:00:10Z',
      created_at: '2026-03-24T00:00:00Z',
    },
  ]

  let reviews = [
    {
      id: 'review-1',
      workflow_run_id: 'run-1',
      folder_id: 'folder-1',
      status: 'pending',
      before: { path: '/source/album', name: 'album' },
      after: { path: '/target/photo/album', name: 'album' },
      diff: {
        path_changed: true,
        name_changed: false,
        new_artifacts: [],
        executed_steps: [],
      },
      reason: '',
      created_at: '2026-03-24T00:00:00Z',
      updated_at: '2026-03-24T00:00:00Z',
    },
  ]

  await page.route('**/api/**', async (route) => {
    const path = pathname(route)
    const reqMethod = method(route)
    if (!path.startsWith('/api/')) {
      await route.continue()
      return
    }

    if (path === '/api/jobs' && reqMethod === 'GET') {
      await route.fulfill(json({ data: jobs, total: jobs.length, page: 1, limit: 20 }))
      return
    }
    if (path === '/api/workflow-defs' && reqMethod === 'GET') {
      await route.fulfill(json({ data: workflowDefs, total: workflowDefs.length, page: 1, limit: 100 }))
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
            total: reviews.length,
            pending: reviews.filter((item) => item.status === 'pending').length,
            approved: reviews.filter((item) => item.status === 'approved').length,
            rolled_back: reviews.filter((item) => item.status === 'rolled_back').length,
          },
        }),
      )
      return
    }
    if (path === '/api/workflow-runs/run-1/reviews' && reqMethod === 'GET') {
      await route.fulfill(
        json({
          data: reviews,
          summary: {
            total: reviews.length,
            pending: reviews.filter((item) => item.status === 'pending').length,
            approved: reviews.filter((item) => item.status === 'approved').length,
            rolled_back: reviews.filter((item) => item.status === 'rolled_back').length,
          },
        }),
      )
      return
    }
    if (path === '/api/workflow-runs/run-1/reviews/review-1/approve' && reqMethod === 'POST') {
      reviews = reviews.map((item) =>
        item.id === 'review-1'
          ? { ...item, status: 'approved', updated_at: '2026-03-24T00:01:00Z' }
          : item,
      )
      await route.fulfill(json({ approved: true }))
      return
    }
    if (path === '/api/jobs/job-1/progress' && reqMethod === 'GET') {
      await route.fulfill(
        json({
          job_id: 'job-1',
          status: 'running',
          done: 0,
          total: 1,
          failed: 0,
          updated_at: '2026-03-24T00:00:00Z',
        }),
      )
      return
    }

    await route.fulfill(json({ error: 'unhandled' }, 404))
  })

  await page.goto('/job-history')
  await expect(page.getByRole('heading', { name: '执行历史' })).toBeVisible()
  await expect(page.getByRole('heading', { name: '任务历史' })).toBeVisible()
  await page.getByRole('row', { name: /测试流程/ }).click()
  await expect(page.getByText('WORKFLOW RUNS (1)')).toBeVisible()
  await expect(page.getByText('待确认')).toBeVisible()
  await page.getByRole('cell', { name: 'folder-1', exact: true }).click()
  await expect(page.getByText('目录确认面板')).toBeVisible()
  await expect(page.getByRole('cell', { name: 'move-node', exact: true })).toBeVisible()
  await page.getByRole('button', { name: '确认通过', exact: true }).click()
  await expect(page.getByText('确认已通过')).toBeVisible()
})
