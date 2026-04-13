import type { NodeRun } from '@/types'

import type { ClassificationPreviewSummary, NodePreviewSummary, ProcessingPreviewSummary } from '@/components/workflow-preview/previewTypes'

export const CATEGORY_LABELS: Record<string, string> = {
  photo: '照片',
  video: '视频',
  manga: '漫画',
  mixed: '混合',
  other: '其他',
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function unwrapNodeOutputPort(
  parsed: Record<string, unknown>,
  portName: string,
): unknown {
  const direct = parsed[portName]
  if (direct !== undefined) {
    return direct
  }
  const outputs = parsed.outputs
  if (isRecord(outputs)) {
    return outputs[portName]
  }
  return undefined
}

function unwrapPortValue(port: unknown): unknown {
  if (!isRecord(port)) return port
  if ('value' in port) return port.value
  return port
}

export function parseNodePreviewSummary(nodeRun: NodeRun | null): NodePreviewSummary | null {
  if (!nodeRun?.output_json) return null
  try {
    const parsed = JSON.parse(nodeRun.output_json) as Record<string, unknown>
    if (!isRecord(parsed)) return null

    const summaryPort = unwrapNodeOutputPort(parsed, 'summary')
    if (summaryPort === undefined) return null
    const summaryValue = unwrapPortValue(summaryPort)
    if (!isRecord(summaryValue)) return null
    return summaryValue as unknown as NodePreviewSummary
  } catch {
    return null
  }
}

export function isClassificationSummary(
  summary: NodePreviewSummary | null,
): summary is ClassificationPreviewSummary {
  return Boolean(summary && 'entries' in summary && 'by_category' in summary)
}

export function isProcessingSummary(
  summary: NodePreviewSummary | null,
): summary is ProcessingPreviewSummary {
  return Boolean(summary && 'by_directory' in summary && 'total_dirs' in summary)
}

export function normalizeCategory(raw: string): string {
  const value = raw.trim().toLowerCase()
  if (value in CATEGORY_LABELS) return value
  return 'other'
}

export function inferCategoryFromPath(path: string): string {
  const normalized = path.toLowerCase()
  if (normalized.includes('/video') || normalized.includes('视频')) return 'video'
  if (normalized.includes('/manga') || normalized.includes('漫画')) return 'manga'
  if (normalized.includes('/photo') || normalized.includes('照片') || normalized.includes('写真')) return 'photo'
  if (normalized.includes('/mixed') || normalized.includes('混合')) return 'mixed'
  return 'other'
}
