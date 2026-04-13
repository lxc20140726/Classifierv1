import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { expect, test } from '@playwright/test'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const repoRoot = path.resolve(__dirname, '..', '..')
const sourceDir = path.join(repoRoot, '.local', 'source')
const targetDir = path.join(repoRoot, '.local', 'target')

async function saveConfig(request: import('@playwright/test').APIRequestContext) {
  const response = await request.put('/api/config', {
    data: {
      scan_input_dirs: [sourceDir],
      output_dirs: {
        video: [targetDir],
        manga: [targetDir],
        photo: [targetDir],
        other: [targetDir],
        mixed: [targetDir],
      },
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
      const response = await request.get('/api/folders?limit=100&top_level_only=true')
      if (!response.ok()) return [] as string[]
      const body = (await response.json()) as { data: Array<Record<string, unknown>> }
      return body.data.map((folder) => String(folder.name ?? folder.Name ?? '')).sort()
    }, { timeout: 30000 })
    .toContain('series-pack')
}

function buildClassificationGraph(folderId: string) {
  return {
    nodes: [
      {
        id: 'picker',
        type: 'folder-picker',
        enabled: true,
        config: {
          saved_folder_id: folderId,
          saved_folder_ids: [folderId],
          folder_ids: [folderId],
        },
      },
      {
        id: 'scanner',
        type: 'folder-tree-scanner',
        enabled: true,
        inputs: {
          source_dir: {
            link_source: {
              source_node_id: 'picker',
              source_port: 'path',
            },
          },
        },
      },
      {
        id: 'kw',
        type: 'name-keyword-classifier',
        enabled: true,
        inputs: {
          trees: {
            link_source: {
              source_node_id: 'scanner',
              source_port: 'tree',
            },
          },
        },
      },
      {
        id: 'ft',
        type: 'file-tree-classifier',
        enabled: true,
        inputs: {
          trees: {
            link_source: {
              source_node_id: 'scanner',
              source_port: 'tree',
            },
          },
        },
      },
      {
        id: 'ext',
        type: 'ext-ratio-classifier',
        enabled: true,
        inputs: {
          trees: {
            link_source: {
              source_node_id: 'scanner',
              source_port: 'tree',
            },
          },
        },
      },
      {
        id: 'cc',
        type: 'confidence-check',
        enabled: true,
        config: {
          threshold: 0.75,
        },
        inputs: {
          signals: {
            link_source: {
              source_node_id: 'ext',
              source_port: 'signal',
            },
          },
        },
      },
      {
        id: 'agg',
        type: 'signal-aggregator',
        enabled: true,
        inputs: {
          trees: {
            link_source: {
              source_node_id: 'scanner',
              source_port: 'tree',
            },
          },
          signal_kw: {
            link_source: {
              source_node_id: 'kw',
              source_port: 'signal',
            },
          },
          signal_ft: {
            link_source: {
              source_node_id: 'ft',
              source_port: 'signal',
            },
          },
          signal_high: {
            link_source: {
              source_node_id: 'cc',
              source_port: 'high',
            },
          },
        },
      },
      {
        id: 'writer',
        type: 'classification-writer',
        enabled: true,
        inputs: {
          entries: {
            link_source: {
              source_node_id: 'agg',
              source_port: 'entries',
            },
          },
        },
      },
    ],
    edges: [
      { id: 'e0', source: 'picker', source_port: 'path', target: 'scanner', target_port: 'source_dir' },
      { id: 'e1', source: 'scanner', source_port: 'tree', target: 'kw', target_port: 'trees' },
      { id: 'e2', source: 'scanner', source_port: 'tree', target: 'ft', target_port: 'trees' },
      { id: 'e3', source: 'scanner', source_port: 'tree', target: 'ext', target_port: 'trees' },
      { id: 'e4', source: 'ext', source_port: 'signal', target: 'cc', target_port: 'signals' },
      { id: 'e7', source: 'scanner', source_port: 'tree', target: 'agg', target_port: 'trees' },
      { id: 'e8', source: 'kw', source_port: 'signal', target: 'agg', target_port: 'signal_kw' },
      { id: 'e9', source: 'ft', source_port: 'signal', target: 'agg', target_port: 'signal_ft' },
      { id: 'e10', source: 'cc', source_port: 'high', target: 'agg', target_port: 'signal_high' },
      { id: 'e11', source: 'agg', source_port: 'entries', target: 'writer', target_port: 'entries' },
    ],
  }
}

test('live classification workflow updates folder status summary after run', async ({ page, request }) => {
  await scanUntilFoldersExist(request)

  const foldersResponse = await request.get('/api/folders?limit=100&top_level_only=true')
  expect(foldersResponse.ok()).toBeTruthy()
  const foldersBody = (await foldersResponse.json()) as { data: Array<Record<string, unknown>> }
  const rootFolder = foldersBody.data.find((item) => String(item.name ?? item.Name ?? '') === 'series-pack')
  expect(rootFolder).toBeTruthy()
  const rootFolderId = String(rootFolder!.id ?? rootFolder!.ID ?? '')
  const rootFolderPath = String(rootFolder!.path ?? rootFolder!.Path ?? '')

  const createWorkflowResponse = await request.post('/api/workflow-defs', {
    data: {
      name: `live-status-${Date.now()}`,
      graph_json: JSON.stringify(buildClassificationGraph(rootFolderId)),
    },
  })
  expect(createWorkflowResponse.ok()).toBeTruthy()
  const createWorkflowBody = (await createWorkflowResponse.json()) as { data: { id: string } }
  const workflowDefId = createWorkflowBody.data.id

  await page.goto('/')
  await expect(page.locator('body')).toBeVisible()

  try {
    const startJobResponse = await request.post('/api/jobs', {
      data: {
        workflow_def_id: workflowDefId,
        source_dir: rootFolderPath,
      },
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
        const response = await request.get('/api/folders?limit=100&top_level_only=true')
        if (!response.ok()) return { status: '', workflowRunId: '' }
        const body = (await response.json()) as { data: Array<Record<string, unknown>> }
        const folder = body.data.find((item) => String(item.id ?? item.ID ?? '') === rootFolderId)
        const classification = ((folder?.workflow_summary ?? folder?.WorkflowSummary) as Record<string, unknown> | undefined)
          ?.classification as Record<string, unknown> | undefined

        return {
          status: String(classification?.status ?? ''),
          workflowRunId: String(classification?.workflow_run_id ?? ''),
        }
      }, { timeout: 30000 })
      .toEqual({
        status: 'succeeded',
        workflowRunId,
      })
  } finally {
    await request.delete(`/api/workflow-defs/${workflowDefId}`)
  }
})
