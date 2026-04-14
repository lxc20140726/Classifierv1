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
  it('后端更新图失败时抛错并中断启动', async () => {
    const updateError = new Error('更新失败')
    const startWorkflow = vi.fn(async () => 'job-1')

    await expect(
      launchWorkflowForFolder({
        workflowDef: { id: 'wf-1', graph_json: VALID_GRAPH },
        folderId: 'folder-1',
        updateWorkflowGraph: vi.fn(async () => {
          throw updateError
        }),
        startWorkflow,
        bindLatestLaunch: vi.fn(async () => undefined),
      }),
    ).rejects.toThrow('更新失败')

    expect(startWorkflow).not.toHaveBeenCalled()
  })

  it('后端启动任务失败时透传错误', async () => {
    await expect(
      launchWorkflowForFolder({
        workflowDef: { id: 'wf-1', graph_json: VALID_GRAPH },
        folderId: 'folder-2',
        updateWorkflowGraph: vi.fn(async () => undefined),
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
})
