import { expect, test } from '@playwright/test'

import {
  getWorkflowFolderLaunchability,
  launchWorkflowForFolder,
} from '../../src/lib/workflowFolderLaunch'

const VALID_GRAPH_WITH_PICKER = JSON.stringify({
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

const VALID_GRAPH_WITHOUT_PICKER = JSON.stringify({
  nodes: [
    {
      id: 'trigger-1',
      type: 'trigger',
      enabled: true,
      config: {},
    },
  ],
  edges: [],
})

test('可启动工作流：会补丁图并启动任务', async () => {
  let updatedGraphJson = ''
  let updatedWorkflowDefId = ''
  let startedWorkflowDefId = ''
  let boundWorkflowDefId = ''
  let boundJobId = ''

  const result = await launchWorkflowForFolder({
    workflowDef: { id: 'wf-1', graph_json: VALID_GRAPH_WITH_PICKER },
    folderId: 'folder-123',
    updateWorkflowGraph: async (workflowDefId, graphJson) => {
      updatedWorkflowDefId = workflowDefId
      updatedGraphJson = graphJson
    },
    startWorkflow: async (workflowDefId) => {
      startedWorkflowDefId = workflowDefId
      return 'job-456'
    },
    bindLatestLaunch: async (workflowDefId, jobId) => {
      boundWorkflowDefId = workflowDefId
      boundJobId = jobId
    },
  })

  expect(result.jobId).toBe('job-456')
  expect(updatedWorkflowDefId).toBe('wf-1')
  expect(startedWorkflowDefId).toBe('wf-1')
  expect(boundWorkflowDefId).toBe('wf-1')
  expect(boundJobId).toBe('job-456')

  const patched = JSON.parse(updatedGraphJson) as { nodes: Array<{ config?: Record<string, unknown> }> }
  expect(patched.nodes[0]?.config?.saved_folder_id).toBe('folder-123')
})

test('无 folder-picker：不可直启', async () => {
  const check = getWorkflowFolderLaunchability(VALID_GRAPH_WITHOUT_PICKER)
  expect(check.canLaunch).toBeFalsy()
  expect(check.enabledPickerCount).toBe(0)
  expect(check.error).toContain('folder-picker')
})

test('空 folder id：启动时报错', async () => {
  await expect(
    launchWorkflowForFolder({
      workflowDef: { id: 'wf-2', graph_json: VALID_GRAPH_WITH_PICKER },
      folderId: '   ',
      updateWorkflowGraph: async () => undefined,
      startWorkflow: async () => 'job-1',
      bindLatestLaunch: async () => undefined,
    }),
  ).rejects.toThrow('请选择一条文件夹记录')
})

test('graph JSON 非法：校验失败', () => {
  const check = getWorkflowFolderLaunchability('{')
  expect(check.canLaunch).toBeFalsy()
  expect(check.error).toBeTruthy()
})

test('更新工作流图失败：抛出后端异常并中断启动', async () => {
  let started = false

  await expect(
    launchWorkflowForFolder({
      workflowDef: { id: 'wf-3', graph_json: VALID_GRAPH_WITH_PICKER },
      folderId: 'folder-123',
      updateWorkflowGraph: async () => {
        throw new Error('后端更新失败')
      },
      startWorkflow: async () => {
        started = true
        return 'job-1'
      },
      bindLatestLaunch: async () => undefined,
    }),
  ).rejects.toThrow('后端更新失败')

  expect(started).toBeFalsy()
})

test('启动任务失败：抛出网络错误', async () => {
  await expect(
    launchWorkflowForFolder({
      workflowDef: { id: 'wf-4', graph_json: VALID_GRAPH_WITH_PICKER },
      folderId: 'folder-123',
      updateWorkflowGraph: async () => undefined,
      startWorkflow: async () => {
        throw new Error('网络错误')
      },
      bindLatestLaunch: async () => undefined,
    }),
  ).rejects.toThrow('网络错误')
})
