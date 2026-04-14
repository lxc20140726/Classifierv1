import { findWorkflowDefByName, upsertWorkflowDef } from '../framework/apiHelpers'

import type { APIRequestContext } from '@playwright/test'

function stringifyGraph(graph: Record<string, unknown>): string {
  return JSON.stringify(graph)
}

export async function ensureDefaultProcessingWorkflow(request: APIRequestContext): Promise<string> {
  const existing = await findWorkflowDefByName(request, 'default-processing')
  if (existing == null) {
    throw new Error('未找到 default-processing，请确认后端 seed 已初始化')
  }
  return existing.id
}

export async function ensureDefaultClassificationWorkflow(request: APIRequestContext): Promise<string> {
  const existing = await findWorkflowDefByName(request, '默认分类流程')
  if (existing == null) {
    throw new Error('未找到 默认分类流程，请确认后端 seed 已初始化')
  }
  return existing.id
}

export async function ensureProcessingPassWorkflow(request: APIRequestContext): Promise<string> {
  const graph = {
    nodes: [
      { id: 'p-reader', type: 'classification-reader', config: {}, enabled: true },
      {
        id: 'p-move',
        type: 'move-node',
        config: {
          path_ref_type: 'output',
          path_ref_key: 'mixed',
          path_suffix: '.processed',
          move_unit: 'folder',
          conflict_policy: 'auto_rename',
        },
        enabled: true,
        inputs: {
          items: { link_source: { source_node_id: 'p-reader', source_port: 'entry' } },
        },
      },
      { id: 'p-preview', type: 'processing-result-preview', config: {}, enabled: true, inputs: { items: { link_source: { source_node_id: 'p-move', source_port: 'items' } } } },
    ],
    edges: [
      { id: 'e-reader-move', source: 'p-reader', source_port: 'entry', target: 'p-move', target_port: 'items' },
      { id: 'e-move-preview', source: 'p-move', source_port: 'items', target: 'p-preview', target_port: 'items' },
    ],
  }

  return upsertWorkflowDef(request, {
    name: 'e2e-processing-pass',
    description: 'E2E: 处理通过链路',
    graphJSON: stringifyGraph(graph),
    isActive: false,
  })
}

export async function ensureProcessingRollbackWorkflow(request: APIRequestContext): Promise<string> {
  const graph = {
    nodes: [
      { id: 'p-reader', type: 'classification-reader', config: {}, enabled: true },
      { id: 'p-preview', type: 'processing-result-preview', config: {}, enabled: true, inputs: { items: { link_source: { source_node_id: 'p-reader', source_port: 'entry' } } } },
    ],
    edges: [
      { id: 'e-reader-preview', source: 'p-reader', source_port: 'entry', target: 'p-preview', target_port: 'items' },
    ],
  }

  return upsertWorkflowDef(request, {
    name: 'e2e-processing-rollback',
    description: 'E2E: 处理回退链路',
    graphJSON: stringifyGraph(graph),
    isActive: false,
  })
}
