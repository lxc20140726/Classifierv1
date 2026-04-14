import type { APIRequestContext, Browser, Page, TestInfo } from '@playwright/test'

export type ScenarioTag =
  | 'complex'
  | 'smoke'
  | 'scan'
  | 'classify'
  | 'process'
  | 'rollback'
  | 'history'
  | 'output-check'
  | 'lineage'
  | 'docker'

export interface DirectorySeed {
  relativePath: string
  assets: AssetKind[]
}

export type AssetKind =
  | 'jpg'
  | 'png'
  | 'webp'
  | 'mp4'
  | 'mkv'
  | 'cbz'
  | 'cbr'
  | 'txt'
  | 'pdf'

export interface DirectoryTemplate {
  seeds: DirectorySeed[]
}

export interface RuntimePaths {
  root: string
  sourceDir: string
  targetDir: string
  configDir: string
  deleteStagingDir: string
}

export interface ScenarioState {
  folderIDsByName: Record<string, string>
  workflowDefIDsByName: Record<string, string>
  jobIDs: string[]
  workflowRunIDs: string[]
}

export interface ScenarioRuntimeContext {
  scenario: E2EScenario
  browser: Browser
  page: Page
  request: APIRequestContext
  baseURL: string
  paths: RuntimePaths
  state: ScenarioState
  testInfo: TestInfo
}

export interface ScenarioUnit {
  name: string
  run: (ctx: ScenarioRuntimeContext) => Promise<void>
}

export interface E2EScenario {
  id: string
  name: string
  tags: ScenarioTag[]
  directoryTemplate: DirectoryTemplate
  steps: ScenarioUnit[]
  assertions: ScenarioUnit[]
}
