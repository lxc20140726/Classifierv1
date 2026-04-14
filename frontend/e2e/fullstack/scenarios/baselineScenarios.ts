import { expect } from '@playwright/test'

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
      return items.length
    }, { timeout: 20000 })
    .toBeGreaterThan(0)

  const folders = await listFolders(ctx.request)
  for (const folder of folders) {
    ctx.state.folderIDsByName[folder.name] = folder.id
    ctx.state.folderIDsByPath[folder.path] = folder.id
  }
  expect(Object.keys(ctx.state.folderIDsByName).length).toBeGreaterThan(0)
})

const runClassificationStep = step('执行分类工作流', async (ctx) => {
  const classificationDefID = await ensureClassificationWorkflow(ctx.request)
  ctx.state.workflowDefIDsByName['分类'] = classificationDefID
  const classifyJobID = await startWorkflowJob(ctx.request, classificationDefID)
  ctx.state.jobIDs.push(classifyJobID)
  const classifyRunID = await waitForWorkflowRunStatus(ctx.request, classifyJobID, 'succeeded')
  ctx.state.workflowRunIDs.push(classifyRunID)
  ctx.state.workflowRunStatusByID[classifyRunID] = 'succeeded'
})

const startProcessingStep = step('启动处理工作流', async (ctx) => {
  const workflowDefID = await ensureProcessingWorkflow(ctx.request)
  ctx.state.workflowDefIDsByName['处理'] = workflowDefID
  const jobID = await startWorkflowJob(ctx.request, workflowDefID)
  ctx.state.jobIDs.push(jobID)
})

const approveProcessingStep = step('处理流批量审批通过', async (ctx) => {
  const jobID = ctx.state.jobIDs.at(-1) ?? ''
  expect(jobID).not.toEqual('')
  const workflowRunID = await waitForWorkflowRunStatus(ctx.request, jobID, 'waiting_input')
  ctx.state.workflowRunIDs.push(workflowRunID)
  await waitForPendingReviews(ctx.request, workflowRunID)
  const approved = await approveAllReviews(ctx.request, workflowRunID)
  expect(approved).toBeGreaterThan(0)
  await waitForWorkflowRunStatus(ctx.request, jobID, 'succeeded')
  ctx.state.workflowRunStatusByID[workflowRunID] = 'succeeded'
})

const rollbackProcessingStep = step('处理流批量回退', async (ctx) => {
  const jobID = ctx.state.jobIDs.at(-1) ?? ''
  expect(jobID).not.toEqual('')
  const workflowRunID = await waitForWorkflowRunStatus(ctx.request, jobID, 'waiting_input')
  ctx.state.workflowRunIDs.push(workflowRunID)
  await waitForPendingReviews(ctx.request, workflowRunID)
  const rolledBack = await rollbackAllReviews(ctx.request, workflowRunID)
  expect(rolledBack).toBeGreaterThan(0)
  await waitForWorkflowRunStatus(ctx.request, jobID, 'rolled_back')
  ctx.state.workflowRunStatusByID[workflowRunID] = 'rolled_back'
})

const verifyLineageAPIStep = step('验证流向图 API', async (ctx) => {
  const folderID = ctx.state.folderIDsByName['photo-album']
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
        rollbackProcessingStep,
      ],
      assertions: [
        assertReviewDecision((ctx) => ctx.state.workflowRunIDs.at(-2) ?? '', 'approved'),
        assertReviewDecision((ctx) => ctx.state.workflowRunIDs.at(-1) ?? '', 'rolled_back'),
      ],
    },
  ]
}
