import { expect } from '@playwright/test'
import { readdir, stat } from 'node:fs/promises'
import path from 'node:path'

import {
  getFolderDetail,
  getFolderLineage,
  getWorkflowRun,
  listFolders,
  listReviewItems,
  listReviewsSummary,
} from '../framework/apiHelpers'

import type { ScenarioRuntimeContext, ScenarioUnit } from '../framework/types'

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
          ctx.state.folderIDsByPath[folder.path] = folder.id
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
          ctx.state.folderIDsByPath[folder.path] = folder.id
          return folder.category
        }, { timeout: 20000 })
        .toBe(expectedCategory)
    },
  }
}

export function assertFolderCategoryNotEquals(name: string, unexpectedCategory: string): ScenarioUnit {
  return {
    name: `断言目录 ${name} 分类不为 ${unexpectedCategory}`,
    async run(ctx: ScenarioRuntimeContext) {
      await expect
        .poll(async () => {
          const folders = await listFolders(ctx.request)
          const folder = folders.find((item) => item.name === name)
          if (folder == null) return ''
          ctx.state.folderIDsByName[name] = folder.id
          ctx.state.folderIDsByPath[folder.path] = folder.id
          return folder.category
        }, { timeout: 20000 })
        .not.toBe(unexpectedCategory)
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

export function assertTargetDirectoryEmpty(): ScenarioUnit {
  return {
    name: '断言目标目录为空',
    async run(ctx: ScenarioRuntimeContext) {
      const entries = await readdir(ctx.paths.targetDir)
      expect(entries.length).toBe(0)
    },
  }
}

export function assertTargetEntryCountAtLeast(minCount: number): ScenarioUnit {
  return {
    name: `断言目标目录顶层条目不少于 ${minCount}`,
    async run(ctx: ScenarioRuntimeContext) {
      const entries = await readdir(ctx.paths.targetDir)
      expect(entries.length).toBeGreaterThanOrEqual(minCount)
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

export function assertWorkflowRunStatus(
  workflowRunIDGetter: (ctx: ScenarioRuntimeContext) => string,
  expectedStatus: string,
): ScenarioUnit {
  return {
    name: `断言 workflow run 状态为 ${expectedStatus}`,
    async run(ctx: ScenarioRuntimeContext) {
      const workflowRunID = workflowRunIDGetter(ctx)
      expect(workflowRunID).not.toEqual('')
      const detail = await getWorkflowRun(ctx.request, workflowRunID)
      expect(detail.status).toBe(expectedStatus)
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

export function assertReviewContainsBeforeAfterPairs(
  workflowRunIDGetter: (ctx: ScenarioRuntimeContext) => string,
  pairs: Array<{ beforeKeyword: string; afterKeyword?: string }>,
): ScenarioUnit {
  return {
    name: '断言评审包含关键 before/after 路径',
    async run(ctx: ScenarioRuntimeContext) {
      const workflowRunID = workflowRunIDGetter(ctx)
      expect(workflowRunID).not.toEqual('')
      const reviews = await listReviewItems(ctx.request, workflowRunID)
      for (const pair of pairs) {
        const matched = reviews.find((item) => {
          if (!item.beforePath.includes(pair.beforeKeyword)) {
            return false
          }
          if (pair.afterKeyword == null) {
            return item.afterPath.trim() !== ''
          }
          return item.afterPath.includes(pair.afterKeyword)
        })
        expect(matched, `before=${pair.beforeKeyword}, after=${pair.afterKeyword ?? '*'}`).toBeTruthy()
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

export function assertFolderLineageContainsKeywords(
  folderNameGetter: (ctx: ScenarioRuntimeContext) => string,
  pathKeywords: string[],
): ScenarioUnit {
  return {
    name: `断言 lineage 数据包含关键路径：${pathKeywords.join(', ')}`,
    async run(ctx: ScenarioRuntimeContext) {
      const folderName = folderNameGetter(ctx)
      const folderID = ctx.state.folderIDsByName[folderName]
      expect(folderID).toBeTruthy()

      const lineage = await getFolderLineage(ctx.request, folderID)
      expect(Array.isArray(lineage.timeline)).toBeTruthy()
      expect(Array.isArray(lineage.flow?.links ?? [])).toBeTruthy()

      const serialized = JSON.stringify(lineage)
      for (const keyword of pathKeywords) {
        expect(serialized.includes(keyword)).toBeTruthy()
      }
    },
  }
}

export function assertFolderListAndDetailCategoryConsistent(folderNames: string[]): ScenarioUnit {
  return {
    name: `断言 folder 列表与详情分类一致：${folderNames.join(', ')}`,
    async run(ctx: ScenarioRuntimeContext) {
      const folders = await listFolders(ctx.request)
      for (const folderName of folderNames) {
        const listRow = folders.find((item) => item.name === folderName)
        expect(listRow, `列表中缺少目录 ${folderName}`).toBeTruthy()
        if (listRow == null) continue
        const detail = await getFolderDetail(ctx.request, listRow.id)
        expect(detail.id).toBe(listRow.id)
        expect(detail.name).toBe(listRow.name)
        expect(detail.path).toBe(listRow.path)
        expect(detail.category).toBe(listRow.category)
      }
    },
  }
}

export function assertPathExistsInTarget(relativePath: string): ScenarioUnit {
  return {
    name: `断言目标目录存在路径：${relativePath}`,
    async run(ctx: ScenarioRuntimeContext) {
      const fullPath = path.join(ctx.paths.targetDir, relativePath)
      expect(await pathExists(fullPath)).toBeTruthy()
    },
  }
}

export function assertTargetContainsPathKeywords(pathKeywords: string[]): ScenarioUnit {
  return {
    name: `断言目标目录包含关键词路径：${pathKeywords.join(', ')}`,
    async run(ctx: ScenarioRuntimeContext) {
      const allPaths = await listRelativePaths(ctx.paths.targetDir)
      for (const keyword of pathKeywords) {
        expect(allPaths.some((item) => item.includes(keyword))).toBeTruthy()
      }
    },
  }
}

export function assertSourceRelativePathExists(relativePath: string): ScenarioUnit {
  return {
    name: `断言源目录存在路径：${relativePath}`,
    async run(ctx: ScenarioRuntimeContext) {
      const fullPath = path.join(ctx.paths.sourceDir, relativePath)
      expect(await pathExists(fullPath)).toBeTruthy()
    },
  }
}

export function assertSourceRelativePathNotEmpty(relativePath: string): ScenarioUnit {
  return {
    name: `断言源目录路径非空：${relativePath}`,
    async run(ctx: ScenarioRuntimeContext) {
      const fullPath = path.join(ctx.paths.sourceDir, relativePath)
      const entries = await readdir(fullPath)
      expect(entries.length).toBeGreaterThan(0)
    },
  }
}

export function assertSourceContainsUnsupportedExtensions(extensions: string[]): ScenarioUnit {
  return {
    name: `断言源目录仍可见 unsupported 文件：${extensions.join(', ')}`,
    async run(ctx: ScenarioRuntimeContext) {
      const allPaths = await listRelativePaths(ctx.paths.sourceDir)
      const normalizedExts = extensions.map((item) => item.toLowerCase())
      const hasMatch = allPaths.some((item) => {
        const ext = path.extname(item).replace('.', '').toLowerCase()
        return normalizedExts.includes(ext)
      })
      expect(hasMatch).toBeTruthy()
    },
  }
}
