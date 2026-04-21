import { checkLaunchableFolderPickers } from '@/lib/workflowGraphFolderPicker'
import type { WorkflowDefinition } from '@/types'

export interface WorkflowFolderLaunchability {
  enabledPickerCount: number
  initialSelectedFolderId: string
  canLaunch: boolean
  error: string | null
}

export interface LaunchWorkflowForFolderParams {
  workflowDef: Pick<WorkflowDefinition, 'id' | 'graph_json'>
  folderIds: string[]
  startWorkflow: (workflowDefId: string, folderIds: string[]) => Promise<string>
  bindLatestLaunch: (workflowDefId: string, jobId: string, folderId?: string) => Promise<void>
}

export interface LaunchWorkflowForFolderResult {
  jobId: string
  folderIds: string[]
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
  folderIds,
  startWorkflow,
  bindLatestLaunch,
}: LaunchWorkflowForFolderParams): Promise<LaunchWorkflowForFolderResult> {
  const launchability = getWorkflowFolderLaunchability(workflowDef.graph_json)
  if (!launchability.canLaunch) {
    throw new Error(launchability.error ?? '该工作流暂不可快捷启动')
  }

  const normalizedFolderIDs = [...new Set(folderIds.map((id) => id.trim()).filter((id) => id !== ''))]
  if (normalizedFolderIDs.length === 0) {
    throw new Error('请选择至少一个文件夹')
  }

  const jobId = await startWorkflow(workflowDef.id, normalizedFolderIDs)
  if (normalizedFolderIDs.length === 1) {
    void bindLatestLaunch(workflowDef.id, jobId, normalizedFolderIDs[0])
  } else {
    void bindLatestLaunch(workflowDef.id, jobId)
  }

  return {
    jobId,
    folderIds: normalizedFolderIDs,
    enabledPickerCount: launchability.enabledPickerCount,
  }
}
