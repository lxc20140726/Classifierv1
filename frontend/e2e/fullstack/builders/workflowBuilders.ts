import { findWorkflowDefByName, listWorkflowDefs } from '../framework/apiHelpers'

import type { APIRequestContext } from '@playwright/test'

const PROCESSING_WORKFLOW_CANDIDATES = ['处理', 'default-processing', '通用处理流程'] as const
const CLASSIFICATION_WORKFLOW_CANDIDATES = ['分类', '默认分类流程'] as const

async function findFirstWorkflowByNames(
  request: APIRequestContext,
  names: readonly string[],
) {
  for (const name of names) {
    const existing = await findWorkflowDefByName(request, name)
    if (existing != null) {
      return existing
    }
  }
  return null
}

export async function ensureProcessingWorkflow(request: APIRequestContext): Promise<string> {
  const existing = await findFirstWorkflowByNames(request, PROCESSING_WORKFLOW_CANDIDATES)
  if (existing == null) {
    const defs = await listWorkflowDefs(request)
    const known = defs.map((item) => item.name).join('、')
    throw new Error(`当前环境缺少真实处理工作流（支持名称：${PROCESSING_WORKFLOW_CANDIDATES.join(' / ')}），本套件不会自动创建，请先在环境中完成配置。当前可见工作流：${known || '（空）'}`)
  }
  return existing.id
}

export async function ensureClassificationWorkflow(request: APIRequestContext): Promise<string> {
  const existing = await findFirstWorkflowByNames(request, CLASSIFICATION_WORKFLOW_CANDIDATES)
  if (existing == null) {
    const defs = await listWorkflowDefs(request)
    const known = defs.map((item) => item.name).join('、')
    throw new Error(`当前环境缺少真实分类工作流（支持名称：${CLASSIFICATION_WORKFLOW_CANDIDATES.join(' / ')}），本套件不会自动创建，请先在环境中完成配置。当前可见工作流：${known || '（空）'}`)
  }
  return existing.id
}

export async function ensureRequiredRealWorkflows(
  request: APIRequestContext,
): Promise<{ classificationWorkflowID: string; processingWorkflowID: string }> {
  const classificationWorkflowID = await ensureClassificationWorkflow(request)
  const processingWorkflowID = await ensureProcessingWorkflow(request)
  return { classificationWorkflowID, processingWorkflowID }
}
