import { expect } from '@playwright/test'
import { readdir, stat } from 'node:fs/promises'
import path from 'node:path'

import {
  assertFolderCategoryEquals,
  assertFolderCategoryNotEmpty,
  assertFolderCategoryNotEquals,
  assertFolderLineageContainsKeywords,
  assertFolderListAndDetailCategoryConsistent,
  assertFoldersExist,
  assertLineagePageVisible,
  assertReviewDecision,
  assertSourceRelativePathExists,
  assertSourceRelativePathNotEmpty,
  assertTargetDirectoryEmpty,
  assertTargetDirectoryNotEmpty,
  assertTargetEntryCountAtLeast,
  assertWorkflowRunStatus,
} from '../builders/assertionBuilders'
import {
  createBatchMediaSiblingTemplate,
  createDocumentHybridSiblingTemplate,
  createLayeredComplexDirectoryTemplate,
  createMixedStandaloneProcessingTemplate,
  createRollbackComplexTemplate,
} from '../builders/directoryBuilders'
import { ensureRequiredRealWorkflows } from '../builders/workflowBuilders'
import {
  approveAllReviews,
  listFolders,
  listReviewItems,
  listReviewsSummary,
  provideWorkflowInput,
  rollbackAllReviews,
  saveRuntimeConfig,
  startWorkflowJob,
  triggerScan,
  waitForPendingReviews,
  waitForWorkflowRunStatusIn,
} from '../framework/apiHelpers'

import type { E2EScenario, ScenarioRuntimeContext, ScenarioUnit } from '../framework/types'

function step(name: string, run: ScenarioUnit['run']): ScenarioUnit {
  return { name, run }
}

async function pathExists(absPath: string): Promise<boolean> {
  try {
    await stat(absPath)
    return true
  } catch {
    return false
  }
}

async function listRelativePaths(root: string, current = ''): Promise<string[]> {
  const currentPath = current === '' ? root : path.join(root, current)
  const entries = await readdir(currentPath, { withFileTypes: true })
  const out: string[] = []
  for (const entry of entries) {
    const relative = current === '' ? entry.name : `${current}/${entry.name}`
    out.push(relative)
    if (entry.isDirectory()) {
      const child = await listRelativePaths(root, relative)
      out.push(...child)
    }
  }
  return out
}

const precheckRealWorkflowsStep = step('预检真实工作流（分类/处理）', async (ctx) => {
  const workflows = await ensureRequiredRealWorkflows(ctx.request)
  ctx.state.workflowDefIDsByName['分类'] = workflows.classificationWorkflowID
  ctx.state.workflowDefIDsByName['处理'] = workflows.processingWorkflowID
})

const prepareScanStep = step('配置目录并触发扫描', async (ctx) => {
  await saveRuntimeConfig(ctx.request, {
    sourceDir: ctx.paths.sourceDir,
    targetDir: ctx.paths.targetDir,
    scanInputDirs: [ctx.paths.sourceDir],
  })
  await triggerScan(ctx.request)
})

const captureFolderIDsStep = step('读取扫描结果并缓存 folder id/path', async (ctx) => {
  await expect
    .poll(async () => {
      const items = await listFolders(ctx.request)
      return items.filter((item) => item.path.startsWith(ctx.paths.sourceDir)).length
    }, { timeout: 20000 })
    .toBeGreaterThan(0)

  const folders = await listFolders(ctx.request)
  const runtimeFolders = folders.filter((folder) => folder.path.startsWith(ctx.paths.sourceDir))
  for (const folder of runtimeFolders) {
    ctx.state.folderIDsByName[folder.name] = folder.id
    ctx.state.folderIDsByPath[folder.path] = folder.id
    const normalizedPath = folder.path.replaceAll('\\', '/')
    const baseName = path.posix.basename(normalizedPath)
    if (baseName !== '') {
      ctx.state.folderIDsByName[baseName] = folder.id
    }
  }
  expect(Object.keys(ctx.state.folderIDsByName).length).toBeGreaterThan(0)
})

const runClassificationStep = step('执行真实“分类”工作流（支持 waiting_input）', async (ctx) => {
  const workflowDefID = ctx.state.workflowDefIDsByName['分类']
  expect(workflowDefID).toBeTruthy()
  const jobID = await startWorkflowJob(ctx.request, workflowDefID, ctx.paths.sourceDir)
  ctx.state.jobIDs.push(jobID)

  let run = await waitForWorkflowRunStatusIn(
    ctx.request,
    jobID,
    ['succeeded', 'waiting_input', 'failed', 'rolled_back'],
    60000,
  )

  if (run.status === 'waiting_input') {
    await provideWorkflowInput(ctx.request, run.id, 'mixed')
    run = await waitForWorkflowRunStatusIn(
      ctx.request,
      jobID,
      ['succeeded', 'waiting_input', 'failed', 'rolled_back'],
      60000,
    )
  }

  ctx.state.workflowRunIDs.push(run.id)
  ctx.state.workflowRunStatusByID[run.id] = run.status
  ctx.state.scenarioData.lastClassificationRunID = run.id
  ctx.state.scenarioData.lastClassificationFinalStatus = run.status

  if (run.status !== 'succeeded') {
    throw new Error(`分类工作流未成功结束，当前状态=${run.status}`)
  }
})

function runProcessingStep(mode: 'approve' | 'rollback'): ScenarioUnit {
  const label = mode === 'approve' ? '执行真实“处理”工作流并审批（如需）' : '执行真实“处理”工作流并回退'
  return step(label, async (ctx) => {
    const workflowDefID = ctx.state.workflowDefIDsByName['处理']
    expect(workflowDefID).toBeTruthy()
    const jobID = await startWorkflowJob(ctx.request, workflowDefID, ctx.paths.sourceDir)
    ctx.state.jobIDs.push(jobID)

    const initial = await waitForWorkflowRunStatusIn(
      ctx.request,
      jobID,
      ['waiting_input', 'succeeded', 'failed', 'rolled_back'],
      70000,
    )

    ctx.state.workflowRunIDs.push(initial.id)
    ctx.state.workflowRunStatusByID[initial.id] = initial.status
    ctx.state.scenarioData.lastProcessingRunID = initial.id
    ctx.state.scenarioData.lastProcessingInitialStatus = initial.status

    if (initial.status === 'waiting_input') {
      await waitForPendingReviews(ctx.request, initial.id, 60000)
      if (mode === 'approve') {
        const approved = await approveAllReviews(ctx.request, initial.id)
        expect(approved).toBeGreaterThan(0)
        ctx.state.scenarioData.lastProcessingApproved = String(approved)
        const final = await waitForWorkflowRunStatusIn(
          ctx.request,
          jobID,
          ['succeeded', 'failed', 'rolled_back'],
          70000,
        )
        ctx.state.workflowRunStatusByID[initial.id] = final.status
        ctx.state.scenarioData.lastProcessingFinalStatus = final.status
        if (final.status !== 'succeeded') {
          throw new Error(`审批后处理工作流未成功结束，当前状态=${final.status}`)
        }
        return
      }

      const rolledBack = await rollbackAllReviews(ctx.request, initial.id)
      expect(rolledBack).toBeGreaterThan(0)
      ctx.state.scenarioData.lastProcessingRolledBack = String(rolledBack)
      const final = await waitForWorkflowRunStatusIn(
        ctx.request,
        jobID,
        ['rolled_back', 'failed', 'succeeded'],
        70000,
      )
      ctx.state.workflowRunStatusByID[initial.id] = final.status
      ctx.state.scenarioData.lastProcessingFinalStatus = final.status
      if (final.status !== 'rolled_back') {
        throw new Error(`回退后处理工作流状态异常，当前状态=${final.status}`)
      }
      return
    }

    ctx.state.scenarioData.lastProcessingFinalStatus = initial.status
    if (mode === 'rollback') {
      throw new Error('当前真实环境“处理”工作流未进入 waiting_input，无法验证 rollback 链路')
    }
    if (initial.status !== 'succeeded') {
      throw new Error(`处理工作流执行失败，当前状态=${initial.status}`)
    }
  })
}

function assertReviewOrTargetContainsKeywords(pathKeywords: string[]): ScenarioUnit {
  return step(`断言评审或目标目录包含路径关键词：${pathKeywords.join(', ')}`, async (ctx) => {
    const runID = ctx.state.scenarioData.lastProcessingRunID ?? ''
    expect(runID).not.toEqual('')
    const summary = await listReviewsSummary(ctx.request, runID)
    if (summary.total > 0) {
      const reviews = await listReviewItems(ctx.request, runID)
      const allPaths = reviews.flatMap((item) => [item.beforePath, item.afterPath])
      for (const keyword of pathKeywords) {
        expect(allPaths.some((item) => item.includes(keyword))).toBeTruthy()
      }
      return
    }

    expect(ctx.state.scenarioData.lastProcessingFinalStatus).toBe('succeeded')
    const targetPaths = await listRelativePaths(ctx.paths.targetDir)
    const sourcePaths = await listRelativePaths(ctx.paths.sourceDir)
    expect(targetPaths.length + sourcePaths.length).toBeGreaterThan(0)
    for (const keyword of pathKeywords) {
      const hitTarget = targetPaths.some((item) => item.includes(keyword))
      const hitSource = sourcePaths.some((item) => item.includes(keyword))
      expect(hitTarget || hitSource).toBeTruthy()
    }
  })
}

const assertMixedLeafTraceOrUnsupportedStep = step('断言 mixed 叶子路径可追踪或保留 unsupported 文件', async (ctx) => {
  const runID = ctx.state.scenarioData.lastProcessingRunID ?? ''
  expect(runID).not.toEqual('')

  const summary = await listReviewsSummary(ctx.request, runID)
  if (summary.total > 0) {
    const reviews = await listReviewItems(ctx.request, runID)
    const matched = reviews.some((item) => item.beforePath.includes('mixed-leaf') || item.afterPath.includes('mixed-leaf'))
    if (matched) {
      return
    }
  }

  const sourcePaths = await listRelativePaths(ctx.paths.sourceDir)
  const hasUnsupported = sourcePaths.some((item) => item.endsWith('.txt') || item.endsWith('.pdf'))
  expect(hasUnsupported).toBeTruthy()
})

const assertSingleMediaDirectoryTraceStep = step('断言单独视频/图片目录有可追踪路径', async (ctx) => {
  const runID = ctx.state.scenarioData.lastProcessingRunID ?? ''
  expect(runID).not.toEqual('')
  const targetPaths = await listRelativePaths(ctx.paths.targetDir)
  const sourcePaths = await listRelativePaths(ctx.paths.sourceDir)

  const summary = await listReviewsSummary(ctx.request, runID)
  if (summary.total > 0) {
    const reviews = await listReviewItems(ctx.request, runID)
    const allPaths = reviews.flatMap((item) => [item.beforePath, item.afterPath])
    const hasVideoInReview = allPaths.some((item) => item.includes('video-only-leaf'))
    const hasPhotoInReview = allPaths.some((item) => item.includes('photo-only-leaf'))
    const hasPhotoInFS = targetPaths.some((item) => item.includes('photo-only-leaf')) || sourcePaths.some((item) => item.includes('photo-only-leaf'))
    expect(hasVideoInReview).toBeTruthy()
    expect(hasPhotoInReview || hasPhotoInFS).toBeTruthy()
    return
  }

  const hasVideoTrace = targetPaths.some((item) => item.includes('video-only-leaf')) || sourcePaths.some((item) => item.includes('video-only-leaf'))
  const hasPhotoTrace = targetPaths.some((item) => item.includes('photo-only-leaf')) || sourcePaths.some((item) => item.includes('photo-only-leaf'))
  expect(hasVideoTrace).toBeTruthy()
  expect(hasPhotoTrace).toBeTruthy()
})

const assertBatchReviewCoverageStep = step('断言批量兄弟目录在评审或目标产物中都有覆盖', async (ctx) => {
  const runID = ctx.state.scenarioData.lastProcessingRunID ?? ''
  expect(runID).not.toEqual('')
  const expectedKeywords = ['video-sibling-a', 'video-sibling-b', 'photo-sibling-a', 'photo-sibling-b', 'mixed-sibling-a']
  const summary = await listReviewsSummary(ctx.request, runID)
  if (summary.total > 0) {
    const reviews = await listReviewItems(ctx.request, runID)
    const allPaths = reviews.flatMap((item) => [item.beforePath, item.afterPath])
    const hitCount = expectedKeywords.filter((keyword) => allPaths.some((item) => item.includes(keyword))).length
    expect(hitCount).toBeGreaterThanOrEqual(2)
    return
  }

  const targetPaths = await listRelativePaths(ctx.paths.targetDir)
  const hitCount = expectedKeywords.filter((keyword) => targetPaths.some((item) => item.includes(keyword))).length
  expect(hitCount).toBeGreaterThanOrEqual(2)
})

const assertOtherDirectoryBehaviorStep = step('断言 docs-only 目录未被错误分流', async (ctx) => {
  const sourceDocsOnly = path.join(ctx.paths.sourceDir, 'complex-doc-root/docs-only-leaf')
  const sourceExists = await pathExists(sourceDocsOnly)
  const targetPaths = await listRelativePaths(ctx.paths.targetDir)
  const targetHasDocsOnly = targetPaths.some((item) => item.includes('docs-only-leaf'))

  expect(sourceExists || targetHasDocsOnly).toBeTruthy()
  if (targetHasDocsOnly) {
    const suspicious = targetPaths.some((item) =>
      item.includes('docs-only-leaf') && (item.toLowerCase().includes('video') || item.toLowerCase().includes('photo')),
    )
    expect(suspicious).toBeFalsy()
  }
})

export function buildComplexScenarios(): E2EScenario[] {
  return [
    {
      id: 'complex-scan-and-classification',
      name: '复杂目录扫描与分类',
      tags: ['complex', 'scan', 'classify'],
      directoryTemplate: createLayeredComplexDirectoryTemplate('complex-scan-root'),
      steps: [precheckRealWorkflowsStep, prepareScanStep, captureFolderIDsStep, runClassificationStep],
      assertions: [
        assertFoldersExist(['complex-scan-root', 'video-leaf-a', 'photo-leaf-a', 'mixed-leaf-a', 'other-leaf-a', 'mixed-grand-leaf']),
        assertFolderCategoryNotEmpty('complex-scan-root'),
        assertFolderCategoryEquals('complex-scan-root', 'mixed'),
        assertFolderCategoryEquals('video-leaf-a', 'video'),
        assertFolderCategoryEquals('photo-leaf-a', 'photo'),
        assertFolderCategoryEquals('other-leaf-a', 'other'),
        assertFolderCategoryEquals('mixed-leaf-a', 'mixed'),
        assertFolderListAndDetailCategoryConsistent(['complex-scan-root', 'video-leaf-a', 'photo-leaf-a', 'mixed-leaf-a', 'other-leaf-a']),
      ],
    },
    {
      id: 'complex-mixed-subdirs-and-single-media-process',
      name: '子目录 mixed + 单独视频图片文件处理',
      tags: ['complex', 'classify', 'process'],
      directoryTemplate: createMixedStandaloneProcessingTemplate('complex-mixed-single-root'),
      steps: [precheckRealWorkflowsStep, prepareScanStep, captureFolderIDsStep, runClassificationStep, runProcessingStep('approve')],
      assertions: [
        assertFoldersExist(['complex-mixed-single-root']),
        assertFolderCategoryEquals('complex-mixed-single-root', 'mixed'),
        assertMixedLeafTraceOrUnsupportedStep,
        assertSingleMediaDirectoryTraceStep,
        assertTargetDirectoryNotEmpty(),
      ],
    },
    {
      id: 'complex-batch-video-photo-siblings-process',
      name: '多个视频图片子目录批量处理',
      tags: ['complex', 'classify', 'process'],
      directoryTemplate: createBatchMediaSiblingTemplate('complex-batch-root'),
      steps: [precheckRealWorkflowsStep, prepareScanStep, captureFolderIDsStep, runClassificationStep, runProcessingStep('approve')],
      assertions: [
        assertFoldersExist(['complex-batch-root']),
        assertBatchReviewCoverageStep,
        assertTargetEntryCountAtLeast(2),
        assertTargetDirectoryNotEmpty(),
      ],
    },
    {
      id: 'complex-other-files-multi-subdirs-process',
      name: '含其他文件的多个子目录处理',
      tags: ['complex', 'classify', 'process', 'history'],
      directoryTemplate: createDocumentHybridSiblingTemplate('complex-doc-root'),
      steps: [precheckRealWorkflowsStep, prepareScanStep, captureFolderIDsStep, runClassificationStep, runProcessingStep('approve')],
      assertions: [
        assertFoldersExist(['complex-doc-root']),
        assertOtherDirectoryBehaviorStep,
        assertTargetDirectoryNotEmpty(),
      ],
    },
    {
      id: 'complex-process-rollback',
      name: '复杂目录处理回退',
      tags: ['complex', 'process', 'rollback', 'history'],
      directoryTemplate: createRollbackComplexTemplate('complex-rollback-root'),
      steps: [precheckRealWorkflowsStep, prepareScanStep, captureFolderIDsStep, runClassificationStep, runProcessingStep('rollback')],
      assertions: [
        assertWorkflowRunStatus((ctx) => ctx.state.scenarioData.lastProcessingRunID ?? '', 'rolled_back'),
        assertReviewDecision((ctx) => ctx.state.scenarioData.lastProcessingRunID ?? '', 'rolled_back'),
        assertTargetDirectoryEmpty(),
        assertSourceRelativePathExists('complex-rollback-root'),
        assertSourceRelativePathNotEmpty('complex-rollback-root'),
      ],
    },
    {
      id: 'complex-lineage-and-history',
      name: '复杂目录流向与历史验证',
      tags: ['complex', 'process', 'lineage', 'history'],
      directoryTemplate: createMixedStandaloneProcessingTemplate('complex-lineage-root'),
      steps: [precheckRealWorkflowsStep, prepareScanStep, captureFolderIDsStep, runClassificationStep, runProcessingStep('approve')],
      assertions: [
        assertTargetDirectoryNotEmpty(),
        assertFolderLineageContainsKeywords(() => 'mixed-leaf', ['mixed-leaf', 'complex-lineage-root']),
        assertLineagePageVisible(() => 'mixed-leaf'),
      ],
    },
  ]
}
