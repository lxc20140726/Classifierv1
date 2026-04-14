import { findWorkflowDefByName } from '../framework/apiHelpers'

import type { APIRequestContext } from '@playwright/test'

export async function ensureProcessingWorkflow(request: APIRequestContext): Promise<string> {
  const existing = await findWorkflowDefByName(request, '处理')
  if (existing == null) {
    throw new Error('当前环境缺少真实工作流“处理”，本套件不会自动创建，请先在环境中完成配置')
  }
  return existing.id
}

export async function ensureClassificationWorkflow(request: APIRequestContext): Promise<string> {
  const existing = await findWorkflowDefByName(request, '分类')
  if (existing == null) {
    throw new Error('当前环境缺少真实工作流“分类”，本套件不会自动创建，请先在环境中完成配置')
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
