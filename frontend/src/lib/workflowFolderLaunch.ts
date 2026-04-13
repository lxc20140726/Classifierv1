import {
  applyFolderSelectionToEnabledPickers,
  checkLaunchableFolderPickers,
} from '@/lib/workflowGraphFolderPicker'
import type { WorkflowDefinition } from '@/types'

export interface WorkflowFolderLaunchability {
  enabledPickerCount: number
  initialSelectedFolderId: string
  canLaunch: boolean
  error: string | null
}

export interface LaunchWorkflowForFolderParams {
  workflowDef: Pick<WorkflowDefinition, 'id' | 'graph_json'>
  folderId: string
  updateWorkflowGraph: (workflowDefId: string, graphJson: string) => Promise<void>
  startWorkflow: (workflowDefId: string) => Promise<string>
  bindLatestLaunch: (workflowDefId: string, jobId: string, folderId?: string) => Promise<void>
}

export interface LaunchWorkflowForFolderResult {
  jobId: string
  patchedGraphJson: string
  enabledPickerCount: number
}

export function getWorkflowFolderLaunchability(graphJson: string): WorkflowFolderLaunchability {
  try {
    const check = checkLaunchableFolderPickers(graphJson)
    return {
      enabledPickerCount: check.enabledPickerCount,
      initialSelectedFolderId: check.initialSelectedFolderId,
      canLaunch: check.enabledPickerCount > 0,
      error: check.enabledPickerCount > 0 ? null : '该工作流缺少启用中的 folder-picker 节点，无法直接启动',
    }
  } catch (err) {
    return {
      enabledPickerCount: 0,
      initialSelectedFolderId: '',
      canLaunch: false,
      error: err instanceof Error ? err.message : '工作流图解析失败',
    }
  }
}

export async function launchWorkflowForFolder({
  workflowDef,
  folderId,
  updateWorkflowGraph,
  startWorkflow,
  bindLatestLaunch,
}: LaunchWorkflowForFolderParams): Promise<LaunchWorkflowForFolderResult> {
  const launchability = getWorkflowFolderLaunchability(workflowDef.graph_json)
  if (!launchability.canLaunch) {
    throw new Error(launchability.error ?? '该工作流暂不可快捷启动')
  }

  const normalizedFolderId = folderId.trim()
  if (normalizedFolderId === '') {
    throw new Error('请选择一条文件夹记录')
  }

  const patchedGraphJson = applyFolderSelectionToEnabledPickers(
    workflowDef.graph_json,
    normalizedFolderId,
  )

  await updateWorkflowGraph(workflowDef.id, patchedGraphJson)
  const jobId = await startWorkflow(workflowDef.id)
  void bindLatestLaunch(workflowDef.id, jobId, normalizedFolderId)

  return {
    jobId,
    patchedGraphJson,
    enabledPickerCount: launchability.enabledPickerCount,
  }
}
