import { expect } from '@playwright/test'
import { readdir } from 'node:fs/promises'
import path from 'node:path'

import { listFolders, listReviewItems, listReviewsSummary } from '../framework/apiHelpers'

import type { ScenarioRuntimeContext, ScenarioUnit } from '../framework/types'

export function assertFoldersExist(names: string[]): ScenarioUnit {
  return {
    name: `断言扫描后存在目录：${names.join(', ')}`,
    async run(ctx: ScenarioRuntimeContext) {
      await expect
        .poll(async () => {
          const folders = await listFolders(ctx.request)
          const foundNames = new Set(folders.map((item) => item.name))
          return names.every((name) => foundNames.has(name))
        }, { timeout: 20000 })
        .toBeTruthy()
    },
  }
}

export function assertFolderCategoryNotEmpty(name: string): ScenarioUnit {
  return {
    name: `断言目录 ${name} 已分类`,
    async run(ctx: ScenarioRuntimeContext) {
      await expect
        .poll(async () => {
          const folders = await listFolders(ctx.request)
          const folder = folders.find((item) => item.name === name)
          if (folder == null) return ''
          ctx.state.folderIDsByName[name] = folder.id
          return (folder.category ?? '').trim()
        }, { timeout: 20000 })
        .not.toEqual('')
    },
  }
}

export function assertFolderCategoryEquals(name: string, expectedCategory: string): ScenarioUnit {
  return {
    name: `断言目录 ${name} 分类为 ${expectedCategory}`,
    async run(ctx: ScenarioRuntimeContext) {
      await expect
        .poll(async () => {
          const folders = await listFolders(ctx.request)
          const folder = folders.find((item) => item.name === name)
          if (folder == null) return ''
          ctx.state.folderIDsByName[name] = folder.id
          return folder.category
        }, { timeout: 20000 })
        .toBe(expectedCategory)
    },
  }
}

export function assertTargetDirectoryNotEmpty(): ScenarioUnit {
  return {
    name: '断言目标目录产生输出',
    async run(ctx: ScenarioRuntimeContext) {
      const entries = await readdir(ctx.paths.targetDir)
      expect(entries.length).toBeGreaterThan(0)
    },
  }
}

export function assertReviewDecision(
  workflowRunIDGetter: (ctx: ScenarioRuntimeContext) => string,
  expected: 'approved' | 'rolled_back',
): ScenarioUnit {
  return {
    name: `断言评审结果：${expected}`,
    async run(ctx: ScenarioRuntimeContext) {
      const workflowRunID = workflowRunIDGetter(ctx)
      expect(workflowRunID).not.toEqual('')
      const summary = await listReviewsSummary(ctx.request, workflowRunID)
      if (expected === 'approved') {
        expect(summary.approved).toBeGreaterThan(0)
      } else {
        expect(summary.rolled_back).toBeGreaterThan(0)
      }
    },
  }
}

export function assertReviewContainsPaths(
  workflowRunIDGetter: (ctx: ScenarioRuntimeContext) => string,
  pathKeywords: string[],
): ScenarioUnit {
  return {
    name: `断言处理评审覆盖路径：${pathKeywords.join(', ')}`,
    async run(ctx: ScenarioRuntimeContext) {
      const workflowRunID = workflowRunIDGetter(ctx)
      expect(workflowRunID).not.toEqual('')
      const reviews = await listReviewItems(ctx.request, workflowRunID)
      const allPaths = reviews.flatMap((item) => [item.beforePath, item.afterPath]).filter((item) => item.trim() !== '')
      for (const keyword of pathKeywords) {
        expect(allPaths.some((item) => item.includes(keyword))).toBeTruthy()
      }
    },
  }
}

export function assertLineagePageVisible(folderNameGetter: (ctx: ScenarioRuntimeContext) => string): ScenarioUnit {
  return {
    name: '断言文件流向页面可视化可见',
    async run(ctx: ScenarioRuntimeContext) {
      const folderName = folderNameGetter(ctx)
      const folderID = ctx.state.folderIDsByName[folderName]
      expect(folderID).toBeTruthy()

      await ctx.page.goto(`${ctx.baseURL}/folders/${folderID}/lineage`)
      await expect(ctx.page.getByRole('heading', { name: '文件夹文件流向' })).toBeVisible()
      await expect(ctx.page.getByRole('heading', { name: '目录分组文件流向图' })).toBeVisible()
    },
  }
}

export function assertPathExistsInTarget(relativePath: string): ScenarioUnit {
  return {
    name: `断言目标目录存在路径：${relativePath}`,
    async run(ctx: ScenarioRuntimeContext) {
      const fullPath = path.join(ctx.paths.targetDir, relativePath)
      const entries = await readdir(path.dirname(fullPath))
      expect(entries.length).toBeGreaterThan(0)
    },
  }
}
