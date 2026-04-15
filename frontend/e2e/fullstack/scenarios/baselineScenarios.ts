import { expect } from '@playwright/test'
import path from 'node:path'

import {
  assertFolderCategoryNotEmpty,
  assertFoldersExist,
  assertLineagePageVisible,
  assertReviewDecision,
  assertTargetDirectoryNotEmpty,
} from '../builders/assertionBuilders'
import {
  createDirectoryTemplate,
  createEmptyDirectory,
  createMangaDirectory,
  createMixedDirectory,
  createNoiseDirectory,
  createPhotoDirectory,
  createVideoDirectory,
} from '../builders/directoryBuilders'
import {
  ensureClassificationWorkflow,
  ensureProcessingWorkflow,
} from '../builders/workflowBuilders'
import {
  approveAllReviews,
  getFolderLineage,
  listFolders,
  listReviewsSummary,
  rollbackAllReviews,
  saveRuntimeConfig,
  startWorkflowJob,
  triggerScan,
  waitForPendingReviews,
  waitForWorkflowRunStatus,
  waitForWorkflowRunStatusIn,
} from '../framework/apiHelpers'

import type { E2EScenario, ScenarioUnit } from '../framework/types'

function step(name: string, run: ScenarioUnit['run']): ScenarioUnit {
  return { name, run }
}

function buildDefaultTemplate() {
  return createDirectoryTemplate([
    createPhotoDirectory('photo-album'),
    createVideoDirectory('video-season'),
    createMangaDirectory('manga-comic'),
    createMixedDirectory('series-pack/leaf-mixed'),
    createNoiseDirectory('manual-only'),
    createEmptyDirectory('empty-dir'),
  ])
}

const prepareScanStep = step('配置目录并触发扫描', async (ctx) => {
  await saveRuntimeConfig(ctx.request, {
    sourceDir: ctx.paths.sourceDir,
    targetDir: ctx.paths.targetDir,
    scanInputDirs: [ctx.paths.sourceDir],
  })
  await triggerScan(ctx.request)
})

const captureFolderIDsStep = step('读取扫描结果并缓存 folder id', async (ctx) => {
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

const runClassificationStep = step('执行分类工作流', async (ctx) => {
  const classificationDefID = await ensureClassificationWorkflow(ctx.request)
  ctx.state.workflowDefIDsByName['分类'] = classificationDefID
  const classifyJobID = await startWorkflowJob(ctx.request, classificationDefID, ctx.paths.sourceDir)
  ctx.state.jobIDs.push(classifyJobID)
  const classifyRunID = await waitForWorkflowRunStatus(ctx.request, classifyJobID, 'succeeded')
  ctx.state.workflowRunIDs.push(classifyRunID)
  ctx.state.workflowRunStatusByID[classifyRunID] = 'succeeded'
})

const startProcessingStep = step('启动处理工作流', async (ctx) => {
  const workflowDefID = await ensureProcessingWorkflow(ctx.request)
  ctx.state.workflowDefIDsByName['处理'] = workflowDefID
  const jobID = await startWorkflowJob(ctx.request, workflowDefID, ctx.paths.sourceDir)
  ctx.state.jobIDs.push(jobID)
})

const approveProcessingStep = step('处理流批量审批通过', async (ctx) => {
  const jobID = ctx.state.jobIDs.at(-1) ?? ''
  expect(jobID).not.toEqual('')
  const initial = await waitForWorkflowRunStatusIn(
    ctx.request,
    jobID,
    ['waiting_input', 'succeeded', 'failed', 'rolled_back'],
    60000,
  )
  ctx.state.workflowRunIDs.push(initial.id)

  if (initial.status === 'waiting_input') {
    await waitForPendingReviews(ctx.request, initial.id)
    const approved = await approveAllReviews(ctx.request, initial.id)
    expect(approved).toBeGreaterThan(0)
    await waitForWorkflowRunStatus(ctx.request, jobID, 'succeeded')
    ctx.state.workflowRunStatusByID[initial.id] = 'succeeded'
    return
  }

  ctx.state.workflowRunStatusByID[initial.id] = initial.status
  if (initial.status !== 'succeeded') {
    throw new Error(`处理工作流审批阶段状态异常，当前状态=${initial.status}`)
  }
})

const rollbackProcessingStep = step('处理流批量回退', async (ctx) => {
  const jobID = ctx.state.jobIDs.at(-1) ?? ''
  expect(jobID).not.toEqual('')
  const initial = await waitForWorkflowRunStatusIn(
    ctx.request,
    jobID,
    ['waiting_input', 'succeeded', 'failed', 'rolled_back'],
    60000,
  )
  ctx.state.workflowRunIDs.push(initial.id)

  if (initial.status === 'waiting_input') {
    await waitForPendingReviews(ctx.request, initial.id)
    const rolledBack = await rollbackAllReviews(ctx.request, initial.id)
    expect(rolledBack).toBeGreaterThan(0)
    await waitForWorkflowRunStatus(ctx.request, jobID, 'rolled_back')
    ctx.state.workflowRunStatusByID[initial.id] = 'rolled_back'
    return
  }

  ctx.state.workflowRunStatusByID[initial.id] = initial.status
  throw new Error(`处理工作流未进入 waiting_input，无法执行回退，当前状态=${initial.status}`)
})

const rollbackOrAutoSucceedProcessingStep = step('处理流批量回退（若进入待确认）', async (ctx) => {
  const jobID = ctx.state.jobIDs.at(-1) ?? ''
  expect(jobID).not.toEqual('')
  const initial = await waitForWorkflowRunStatusIn(
    ctx.request,
    jobID,
    ['waiting_input', 'succeeded', 'failed', 'rolled_back'],
    60000,
  )
  ctx.state.workflowRunIDs.push(initial.id)

  if (initial.status === 'waiting_input') {
    await waitForPendingReviews(ctx.request, initial.id)
    const rolledBack = await rollbackAllReviews(ctx.request, initial.id)
    expect(rolledBack).toBeGreaterThan(0)
    await waitForWorkflowRunStatus(ctx.request, jobID, 'rolled_back')
    ctx.state.workflowRunStatusByID[initial.id] = 'rolled_back'
    return
  }

  ctx.state.workflowRunStatusByID[initial.id] = initial.status
  if (initial.status !== 'succeeded') {
    throw new Error(`处理工作流重检阶段状态异常，当前状态=${initial.status}`)
  }
})

const verifyLineageAPIStep = step('验证流向图 API', async (ctx) => {
  let folderID = ctx.state.folderIDsByName['photo-album']
  if (folderID == null || folderID.trim() === '') {
    const folders = await listFolders(ctx.request)
    const matched = folders.find(
      (folder) => folder.name === 'photo-album' && folder.path.startsWith(ctx.paths.sourceDir),
    )
    if (matched != null) {
      folderID = matched.id
      ctx.state.folderIDsByName['photo-album'] = matched.id
      ctx.state.folderIDsByPath[matched.path] = matched.id
    }
  }
  expect(folderID).toBeTruthy()
  const lineage = await getFolderLineage(ctx.request, folderID)
  expect(Array.isArray(lineage.timeline)).toBeTruthy()
  expect(Array.isArray(lineage.flow?.links ?? [])).toBeTruthy()
})

const verifyOutputCheckSummaryStep = step('验证处理结果摘要可读取', async (ctx) => {
  const runID = ctx.state.workflowRunIDs.at(-1) ?? ''
  expect(runID).not.toEqual('')
  const summary = await listReviewsSummary(ctx.request, runID)
  expect(summary.total).toBeGreaterThan(0)
})

const verifyApprovedOrAutoSucceededStep = step('验证首轮处理已审批或自动成功', async (ctx) => {
  const runID = ctx.state.workflowRunIDs.at(-2) ?? ''
  expect(runID).not.toEqual('')
  const summary = await listReviewsSummary(ctx.request, runID)
  if (summary.total > 0) {
    expect(summary.approved).toBeGreaterThan(0)
    return
  }
  expect(ctx.state.workflowRunStatusByID[runID]).toBe('succeeded')
})

const verifyRecheckRollbackOrAutoSucceededStep = step('验证重检结果为回退或自动成功', async (ctx) => {
  const runID = ctx.state.workflowRunIDs.at(-1) ?? ''
  expect(runID).not.toEqual('')
  const summary = await listReviewsSummary(ctx.request, runID)
  if (summary.total > 0) {
    expect(summary.rolled_back).toBeGreaterThan(0)
    return
  }
  expect(ctx.state.workflowRunStatusByID[runID]).toBe('succeeded')
})

export function buildBaselineScenarios(): E2EScenario[] {
  return [
    {
      id: 'scan-default-classify',
      name: '扫描与分类',
      tags: ['smoke', 'scan', 'classify'],
      directoryTemplate: buildDefaultTemplate(),
      steps: [prepareScanStep, captureFolderIDsStep, runClassificationStep],
      assertions: [
        assertFoldersExist(['photo-album', 'video-season', 'manga-comic']),
        assertFolderCategoryNotEmpty('photo-album'),
      ],
    },
    {
      id: 'process-approve-all',
      name: '处理工作流进入待确认并审批通过',
      tags: ['smoke', 'process', 'history', 'output-check'],
      directoryTemplate: buildDefaultTemplate(),
      steps: [
        prepareScanStep,
        captureFolderIDsStep,
        runClassificationStep,
        startProcessingStep,
        approveProcessingStep,
      ],
      assertions: [
        assertReviewDecision((ctx) => ctx.state.workflowRunIDs.at(-1) ?? '', 'approved'),
        assertTargetDirectoryNotEmpty(),
        verifyOutputCheckSummaryStep,
      ],
    },
    {
      id: 'process-rollback-all',
      name: '处理工作流回退',
      tags: ['process', 'rollback', 'history'],
      directoryTemplate: buildDefaultTemplate(),
      steps: [
        prepareScanStep,
        captureFolderIDsStep,
        runClassificationStep,
        startProcessingStep,
        rollbackProcessingStep,
      ],
      assertions: [
        assertReviewDecision((ctx) => ctx.state.workflowRunIDs.at(-1) ?? '', 'rolled_back'),
      ],
    },
    {
      id: 'lineage-api-and-page',
      name: '目录分组流向图 API + 页面',
      tags: ['lineage', 'history', 'smoke'],
      directoryTemplate: buildDefaultTemplate(),
      steps: [
        prepareScanStep,
        captureFolderIDsStep,
        runClassificationStep,
        startProcessingStep,
        approveProcessingStep,
        verifyLineageAPIStep,
      ],
      assertions: [
        assertLineagePageVisible(() => 'photo-album'),
      ],
    },
    {
      id: 'output-check-recheck',
      name: '输出校验通过与失败重检（基于处理工作流）',
      tags: ['process', 'output-check', 'rollback'],
      directoryTemplate: buildDefaultTemplate(),
      steps: [
        prepareScanStep,
        captureFolderIDsStep,
        runClassificationStep,
        startProcessingStep,
        approveProcessingStep,
        startProcessingStep,
        rollbackOrAutoSucceedProcessingStep,
      ],
      assertions: [
        verifyApprovedOrAutoSucceededStep,
        verifyRecheckRollbackOrAutoSucceededStep,
      ],
    },
  ]
}
