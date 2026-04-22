import { renderToStaticMarkup } from 'react-dom/server'
import { describe, expect, it } from 'vitest'

import { WorkflowRunStatusCard } from '@/components/WorkflowRunStatusCard'
import type { WorkflowRunCardView } from '@/store/workflowRunStore'

function makeView(overrides: Partial<WorkflowRunCardView> = {}): WorkflowRunCardView {
  return {
    workflowDefId: 'wf-1',
    jobId: 'job-1',
    workflowRunId: 'run-1',
    status: 'running',
    currentNodeId: 'node-1',
    currentNodeType: 'compress-node',
    completedNodes: 1,
    totalNodes: 4,
    currentNodeProgressPercent: 37,
    currentNodeProgressDone: 37,
    currentNodeProgressTotal: 100,
    currentNodeProgressText: '写入中',
    currentNodeDurationText: '3 秒',
    progressSourcePath: '',
    progressTargetPath: '',
    failureSummary: '',
    reviewProgressText: '-',
    pendingReviewCount: 0,
    isBinding: false,
    ...overrides,
  }
}

describe('WorkflowRunStatusCard', () => {
  it('节点进度条使用 SVG 连续宽度渲染', () => {
    const html = renderToStaticMarkup(<WorkflowRunStatusCard view={makeView()} />)

    expect(html).toContain('viewBox="0 0 100 8"')
    expect(html).toContain('width="37"')
    expect(html).toContain('37%')
  })
})
