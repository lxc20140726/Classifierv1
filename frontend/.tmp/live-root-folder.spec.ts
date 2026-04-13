import { expect, test } from '@playwright/test'

test('live classification workflow updates root folder summary and audit timeline', async ({ page, request }) => {
  const folderId = '16eee49e8d49f07857cb07c926b27c2289141d9e'

  const defsResponse = await request.get('/api/workflow-defs?limit=100')
  expect(defsResponse.ok()).toBeTruthy()
  const defsBody = (await defsResponse.json()) as { data: Array<Record<string, unknown>> }
  const workflowDef = defsBody.data.find((item) => {
    const graphJSON = String(item.graph_json ?? '')
    return graphJSON.includes('"folder-picker"') && graphJSON.includes('"classification-writer"')
  })
  expect(workflowDef).toBeTruthy()

  const workflowDefId = String(workflowDef!.id ?? '')
  const originalGraphJSON = String(workflowDef!.graph_json ?? '')
  const graph = JSON.parse(originalGraphJSON) as {
    nodes: Array<{ type: string; config?: Record<string, unknown> }>
    edges: Array<Record<string, unknown>>
  }
  const pickerNode = graph.nodes.find((node) => node.type === 'folder-picker')
  expect(pickerNode).toBeTruthy()
  pickerNode!.config = {
    ...(pickerNode!.config ?? {}),
    source_mode: 'folders',
    saved_folder_id: folderId,
    saved_folder_ids: [folderId],
    folder_ids: [folderId],
  }

  const restoreWorkflow = async () => {
    await request.put(`/api/workflow-defs/${workflowDefId}`, {
      data: {
        name: String(workflowDef!.name ?? ''),
        description: String(workflowDef!.description ?? ''),
        graph_json: originalGraphJSON,
        is_active: Boolean(workflowDef!.is_active ?? true),
        version: Number(workflowDef!.version ?? 1),
      },
    })
  }

  await page.goto('/')
  await expect(page.locator('body')).toBeVisible()

  try {
    const updateResponse = await request.put(`/api/workflow-defs/${workflowDefId}`, {
      data: {
        name: String(workflowDef!.name ?? ''),
        description: String(workflowDef!.description ?? ''),
        graph_json: JSON.stringify(graph),
        is_active: Boolean(workflowDef!.is_active ?? true),
        version: Number(workflowDef!.version ?? 1),
      },
    })
    expect(updateResponse.ok()).toBeTruthy()

    const startJobResponse = await request.post('/api/jobs', {
      data: { workflow_def_id: workflowDefId },
    })
    expect(startJobResponse.ok()).toBeTruthy()
    const startJobBody = (await startJobResponse.json()) as { job_id: string }

    let workflowRunId = ''
    await expect
      .poll(async () => {
        const response = await request.get(`/api/jobs/${startJobBody.job_id}/workflow-runs?limit=20`)
        if (!response.ok()) return ''
        const body = (await response.json()) as { data: Array<{ id: string; status: string }> }
        workflowRunId = body.data[0]?.id ?? ''
        return body.data[0]?.status ?? ''
      }, { timeout: 30000 })
      .toBe('succeeded')

    await expect
      .poll(async () => {
        const response = await request.get(`/api/folders?limit=100&top_level_only=true`)
        if (!response.ok()) return { status: '', workflowRunId: '' }
        const body = (await response.json()) as { data: Array<Record<string, unknown>> }
        const folder = body.data.find((item) => String(item.id ?? '') === folderId)
        const classification = (folder?.workflow_summary as Record<string, unknown> | undefined)?.classification as Record<string, unknown> | undefined
        return {
          status: String(classification?.status ?? ''),
          workflowRunId: String(classification?.workflow_run_id ?? ''),
        }
      }, { timeout: 30000 })
      .toEqual({ status: 'succeeded', workflowRunId })

    await expect
      .poll(async () => {
        const response = await request.get(`/api/audit-logs?folder_id=${folderId}&action=workflow.run.complete&limit=20`)
        if (!response.ok()) return [] as string[]
        const body = (await response.json()) as { data: Array<{ workflow_run_id: string }> }
        return body.data.map((item) => item.workflow_run_id)
      }, { timeout: 30000 })
      .toContain(workflowRunId)
  } finally {
    await restoreWorkflow()
  }
})
