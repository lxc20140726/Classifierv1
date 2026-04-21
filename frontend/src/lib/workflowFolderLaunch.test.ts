import { describe, expect, it, vi } from 'vitest'

import {
  getWorkflowFolderLaunchability,
  launchWorkflowForFolder,
} from '@/lib/workflowFolderLaunch'

const VALID_GRAPH = JSON.stringify({
  nodes: [
    {
      id: 'picker-1',
      type: 'folder-picker',
      enabled: true,
      config: {},
    },
  ],
  edges: [],
})

describe('workflowFolderLaunch', () => {
  it('启动任务失败时透传错误', async () => {
    await expect(
      launchWorkflowForFolder({
        workflowDef: { id: 'wf-1', graph_json: VALID_GRAPH },
        folderIds: ['folder-2'],
        startWorkflow: vi.fn(async () => {
          throw new Error('启动失败')
        }),
        bindLatestLaunch: vi.fn(async () => undefined),
      }),
    ).rejects.toThrow('启动失败')
  })

  it('graph 非法时返回不可启动', () => {
    const result = getWorkflowFolderLaunchability('{')
    expect(result.canLaunch).toBe(false)
    expect(result.error).toBeTruthy()
  })

  it('批量时会去重并传递 folder_ids', async () => {
    const startWorkflow = vi.fn(async () => 'job-1')
    await launchWorkflowForFolder({
      workflowDef: { id: 'wf-1', graph_json: VALID_GRAPH },
      folderIds: ['folder-1', 'folder-1', ' folder-2 '],
      startWorkflow,
      bindLatestLaunch: vi.fn(async () => undefined),
    })

    expect(startWorkflow).toHaveBeenCalledWith('wf-1', ['folder-1', 'folder-2'])
  })
})
