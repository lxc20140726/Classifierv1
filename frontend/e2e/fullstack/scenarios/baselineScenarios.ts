import { expect } from '@playwright/test'

import {
  assertFolderCategoryEquals,
  assertFolderCategoryNotEmpty,
  assertFoldersExist,
  assertLineagePageVisible,
  assertReviewContainsPaths,
  assertReviewDecision,
  assertTargetDirectoryNotEmpty,
} from '../builders/assertionBuilders'
import {
  createCustomDirectory,
  createDirectoryTemplate,
  createEmptyDirectory,
  createMangaDirectory,
  createMixedDirectory,
  createNoiseDirectory,
  createPhotoDirectory,
  createVideoDirectory,
} from '../builders/directoryBuilders'
import {
  ensureDefaultClassificationWorkflow,
  ensureDefaultProcessingWorkflow,
  ensureProcessingPassWorkflow,
  ensureProcessingRollbackWorkflow,
} from '../builders/workflowBuilders'
import {
  approveAllReviews,
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

function buildComplexTemplate() {
  return createDirectoryTemplate([
    createPhotoDirectory('complex-root/photos/2024/travel'),
    createPhotoDirectory('complex-root/photos/2025/family'),
    createVideoDirectory('complex-root/videos/season-1'),
    createVideoDirectory('complex-root/videos/season-2/extras'),
    createMangaDirectory('complex-root/manga/series-a/vol-01'),
    createMangaDirectory('complex-root/manga/series-a/vol-02'),
    createMixedDirectory('complex-root/mixed/album-a'),
    createMixedDirectory('complex-root/mixed/album-b/subset'),
    createNoiseDirectory('complex-root/manual/docs'),
    createNoiseDirectory('complex-root/manual/contracts'),
    createCustomDirectory('complex-root/stat-buckets/batch-1/stat-video-heavy-a', ['mp4', 'mkv', 'mp4', 'jpg']),
    createCustomDirectory('complex-root/stat-buckets/batch-1/stat-video-heavy-b', ['mkv', 'mp4', 'png', 'mp4']),
    createCustomDirectory('complex-root/stat-buckets/batch-2/stat-video-heavy-c', ['mp4', 'mkv', 'webp', 'mp4', 'txt']),
    createEmptyDirectory('complex-root/empty/placeholder'),
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
  }
  expect(Object.keys(ctx.state.folderIDsByName).length).toBeGreaterThan(0)
})

const runDefaultClassificationStep = step('执行默认分类流程', async (ctx) => {
  const classificationDefID = await ensureDefaultClassificationWorkflow(ctx.request)
  ctx.state.workflowDefIDsByName['默认分类流程'] = classificationDefID
  const classifyJobID = await startWorkflowJob(ctx.request, classificationDefID)
  ctx.state.jobIDs.push(classifyJobID)
  const classifyRunID = await waitForWorkflowRunStatus(ctx.request, classifyJobID, 'succeeded')
  ctx.state.workflowRunIDs.push(classifyRunID)
})

const runDefaultClassificationForComplexStep = step('执行默认分类流程（复杂目录容错）', async (ctx) => {
  const classificationDefID = await ensureDefaultClassificationWorkflow(ctx.request)
  ctx.state.workflowDefIDsByName['默认分类流程'] = classificationDefID
  const classifyJobID = await startWorkflowJob(ctx.request, classificationDefID)
  ctx.state.jobIDs.push(classifyJobID)

  let classifyRunID = ''
  await expect
    .poll(async () => {
      const response = await ctx.request.get(`/api/jobs/${classifyJobID}/workflow-runs?limit=20`)
      expect(response.ok()).toBeTruthy()
      const body = (await response.json()) as { data?: Array<{ id?: string; status?: string }> }
      const first = body.data?.[0]
      classifyRunID = first?.id ?? ''
      const status = first?.status ?? ''
      return ['succeeded', 'failed', 'rolled_back', 'waiting_input'].includes(status)
    }, { timeout: 45000 })
    .toBeTruthy()

  expect(classifyRunID).not.toEqual('')
  ctx.state.workflowRunIDs.push(classifyRunID)
})

const startDefaultProcessingStep = step('启动 default-processing', async (ctx) => {
  const workflowDefID = await ensureDefaultProcessingWorkflow(ctx.request)
  ctx.state.workflowDefIDsByName['default-processing'] = workflowDefID
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
})

const verifyLineageAPIStep = step('验证流向图 API', async (ctx) => {
  const folderID = ctx.state.folderIDsByName['series-pack'] || ctx.state.folderIDsByName['series-pack/leaf-mixed'] || ctx.state.folderIDsByName['series-pack']
  const candidateID = folderID || ctx.state.folderIDsByName['photo-album']
  expect(candidateID).toBeTruthy()

  const response = await ctx.request.get(`/api/folders/${candidateID}/lineage`)
  expect(response.ok()).toBeTruthy()
  const body = (await response.json()) as {
    flow?: { links?: unknown[] }
    timeline?: unknown[]
  }
  expect(Array.isArray(body.timeline)).toBeTruthy()
  expect(Array.isArray(body.flow?.links ?? [])).toBeTruthy()
})

const verifyOutputCheckSummaryStep = step('验证处理结果摘要可读取', async (ctx) => {
  const runID = ctx.state.workflowRunIDs.at(-1) ?? ''
  expect(runID).not.toEqual('')
  const summary = await listReviewsSummary(ctx.request, runID)
  expect(summary.total).toBeGreaterThan(0)
})

const startDefaultProcessingAndAssertRunStep = step('启动 default-processing 并断言处理流已执行', async (ctx) => {
  const workflowDefID = await ensureDefaultProcessingWorkflow(ctx.request)
  const jobID = await startWorkflowJob(ctx.request, workflowDefID)
  ctx.state.jobIDs.push(jobID)

  let workflowRunID = ''
  let finalStatus = ''
  await expect
    .poll(async () => {
      const runsResponse = await ctx.request.get(`/api/jobs/${jobID}/workflow-runs?limit=20`)
      expect(runsResponse.ok()).toBeTruthy()
      const body = (await runsResponse.json()) as { data?: Array<{ id?: string; status?: string }> }
      const first = body.data?.[0]
      workflowRunID = first?.id ?? ''
      finalStatus = first?.status ?? ''
      return ['waiting_input', 'succeeded'].includes(finalStatus)
    }, { timeout: 45000 })
    .toBeTruthy()

  ctx.state.workflowRunIDs.push(workflowRunID)
  expect(workflowRunID).not.toEqual('')

  if (finalStatus === 'waiting_input') {
    const summary = await listReviewsSummary(ctx.request, workflowRunID)
    expect(summary.pending).toBeGreaterThan(0)
  }
})

export function buildBaselineScenarios(): E2EScenario[] {
  return [
    {
      id: 'complex-directory-classify-process',
      name: '复杂目录 + 默认分类与处理流程',
      tags: ['complex', 'classify', 'process'],
      directoryTemplate: buildComplexTemplate(),
      steps: [
        prepareScanStep,
        captureFolderIDsStep,
        runDefaultClassificationForComplexStep,
        startDefaultProcessingAndAssertRunStep,
      ],
      assertions: [
        assertFoldersExist(['complex-root']),
        assertFolderCategoryNotEmpty('complex-root'),
        assertFolderCategoryEquals('stat-video-heavy-a', 'video'),
        assertFolderCategoryEquals('stat-video-heavy-b', 'video'),
        assertFolderCategoryEquals('stat-video-heavy-c', 'video'),
        assertReviewContainsPaths(
          (ctx) => ctx.state.workflowRunIDs.at(-1) ?? '',
          ['stat-video-heavy-a', 'stat-video-heavy-b', 'stat-video-heavy-c'],
        ),
      ],
    },
    {
      id: 'scan-default-classify',
      name: '扫描与默认分类',
      tags: ['smoke', 'scan', 'classify'],
      directoryTemplate: buildDefaultTemplate(),
      steps: [prepareScanStep, captureFolderIDsStep],
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
        runDefaultClassificationStep,
        startDefaultProcessingStep,
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
        runDefaultClassificationStep,
        startDefaultProcessingStep,
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
        runDefaultClassificationStep,
        step('创建处理通过测试工作流', async (ctx) => {
          const id = await ensureProcessingPassWorkflow(ctx.request)
          ctx.state.workflowDefIDsByName['e2e-processing-pass'] = id
        }),
        step('运行处理通过测试工作流并审批', async (ctx) => {
          const workflowDefID = ctx.state.workflowDefIDsByName['e2e-processing-pass']
          expect(workflowDefID).toBeTruthy()
          const jobID = await startWorkflowJob(ctx.request, workflowDefID)
          ctx.state.jobIDs.push(jobID)
          const runID = await waitForWorkflowRunStatus(ctx.request, jobID, 'waiting_input')
          ctx.state.workflowRunIDs.push(runID)
          await waitForPendingReviews(ctx.request, runID)
          await approveAllReviews(ctx.request, runID)
          await waitForWorkflowRunStatus(ctx.request, jobID, 'succeeded')
        }),
        verifyLineageAPIStep,
      ],
      assertions: [
        assertLineagePageVisible(() => 'photo-album'),
      ],
    },
    {
      id: 'output-check-recheck',
      name: '输出校验通过与失败重检（流程模板可切换）',
      tags: ['process', 'output-check', 'rollback'],
      directoryTemplate: buildDefaultTemplate(),
      steps: [
        prepareScanStep,
        captureFolderIDsStep,
        runDefaultClassificationStep,
        step('创建处理通过/回退两套测试工作流', async (ctx) => {
          ctx.state.workflowDefIDsByName['e2e-processing-pass'] = await ensureProcessingPassWorkflow(ctx.request)
          ctx.state.workflowDefIDsByName['e2e-processing-rollback'] = await ensureProcessingRollbackWorkflow(ctx.request)
        }),
        step('先执行处理通过链路', async (ctx) => {
          const passID = ctx.state.workflowDefIDsByName['e2e-processing-pass']
          const passJobID = await startWorkflowJob(ctx.request, passID)
          ctx.state.jobIDs.push(passJobID)
          const passRunID = await waitForWorkflowRunStatus(ctx.request, passJobID, 'waiting_input')
          ctx.state.workflowRunIDs.push(passRunID)
          await approveAllReviews(ctx.request, passRunID)
          await waitForWorkflowRunStatus(ctx.request, passJobID, 'succeeded')
        }),
        step('再执行处理回退链路并触发重检语义', async (ctx) => {
          const rollbackID = ctx.state.workflowDefIDsByName['e2e-processing-rollback']
          const rollbackJobID = await startWorkflowJob(ctx.request, rollbackID)
          ctx.state.jobIDs.push(rollbackJobID)
          const rollbackRunID = await waitForWorkflowRunStatus(ctx.request, rollbackJobID, 'waiting_input')
          ctx.state.workflowRunIDs.push(rollbackRunID)
          await rollbackAllReviews(ctx.request, rollbackRunID)
          await waitForWorkflowRunStatus(ctx.request, rollbackJobID, 'rolled_back')
        }),
      ],
      assertions: [
        assertReviewDecision((ctx) => ctx.state.workflowRunIDs[0] ?? '', 'approved'),
        assertReviewDecision((ctx) => ctx.state.workflowRunIDs.at(-1) ?? '', 'rolled_back'),
      ],
    },
  ]
}
