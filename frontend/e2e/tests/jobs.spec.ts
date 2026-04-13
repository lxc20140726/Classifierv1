import { expect, test } from '@playwright/test'

import { json, method, mockEventStream, pathname } from '../support/api'

test('jobs page expands workflow runs and submits waiting input', async ({ page }) => {
  await mockEventStream(page)

  const jobs = [
    {
      id: 'job-1',
      type: 'workflow',
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

  let workflowRuns = [
    {
      id: 'run-1',
      job_id: 'job-1',
      folder_id: 'folder-1',
      workflow_def_id: 'wf-1',
      status: 'waiting_input',
      resume_node_id: 'manual',
      created_at: '2026-03-24T00:00:00Z',
      updated_at: '2026-03-24T00:00:00Z',
    },
  ]

  let nodeRuns = [
    {
      id: 'node-run-1',
      workflow_run_id: 'run-1',
      node_id: 'pending-node',
      node_type: 'audit-log',
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

  await page.route('**/api/**', async (route) => {
    const path = pathname(route)
    const reqMethod = method(route)

    if (path === '/api/jobs' && reqMethod === 'GET') {
      await route.fulfill(json({ data: jobs, total: jobs.length, page: 1, limit: 20 }))
      return
    }
    if (path === '/api/jobs/job-1/workflow-runs' && reqMethod === 'GET') {
      await route.fulfill(json({ data: workflowRuns, total: workflowRuns.length, page: 1, limit: 100 }))
      return
    }
    if (path === '/api/workflow-runs/run-1' && reqMethod === 'GET') {
      await route.fulfill(json({ data: workflowRuns[0], node_runs: nodeRuns }))
      return
    }
    if (path === '/api/workflow-runs/run-1/provide-input' && reqMethod === 'POST') {
      workflowRuns = workflowRuns.map((item) => ({ ...item, status: 'succeeded' }))
      nodeRuns = nodeRuns.map((item) => ({ ...item, status: 'succeeded' }))
      await route.fulfill({ status: 204, contentType: 'application/json', body: '' })
      return
    }

    await route.fulfill(json({ error: 'unhandled' }, 404))
  })

  await page.goto('/jobs')
  await expect(page.getByRole('heading', { name: '任务历史' })).toBeVisible()
  await page.getByRole('cell', { name: 'workflow' }).click()
  await expect(page.getByText('工作流运行（1）')).toBeVisible()
  await expect(page.getByText('待确认')).toBeVisible()
  await page.getByRole('row', { name: 'folder-1 待确认 2026/3/24 08:00:00 照片 确认', exact: true }).click()
  await expect(page.getByRole('cell', { name: 'audit-log', exact: true })).toBeVisible()
  await page.getByRole('combobox').selectOption('manga')
  await page.getByRole('button', { name: '确认' }).click()
  await expect(page.getByRole('row', { name: 'folder-1 已完成 2026/3/24 08:00:00', exact: true })).toBeVisible()
})
