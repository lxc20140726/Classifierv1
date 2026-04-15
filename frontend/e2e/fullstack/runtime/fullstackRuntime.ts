import { spawn } from 'node:child_process'
import { copyFile, cp, mkdir, readdir, rm, stat } from 'node:fs/promises'
import net from 'node:net'
import { homedir } from 'node:os'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { materializeTemplate, prepareRuntimePaths, printRuntimeTree } from './fixtureRuntime'

import type { Browser, Playwright, TestInfo } from '@playwright/test'
import type { E2EScenario, RuntimePaths, ScenarioRuntimeContext, ScenarioState } from '../framework/types'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const frontendDir = path.resolve(__dirname, '../../..')
const repoRoot = path.resolve(frontendDir, '..')
const backendDir = path.join(repoRoot, 'backend')
const frontendDistDir = path.join(frontendDir, 'dist')
const embeddedDistDir = path.join(backendDir, 'cmd/server/web/dist')
const defaultWorkflowSeedConfigDir = path.join(repoRoot, '.e2e-fullstack', 'config')
const homeWorkflowSeedConfigDir = path.join(homedir(), '.classifier', 'config')

let prepareFrontendPromise: Promise<void> | null = null

async function run(command: string, args: string[], cwd: string) {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      stdio: 'inherit',
      env: process.env,
      shell: process.platform === 'win32',
    })
    child.on('exit', (code) => {
      if (code === 0) {
        resolve()
        return
      }
      reject(new Error(`${command} ${args.join(' ')} exited with code ${code ?? 'null'}`))
    })
  })
}

async function ensureFrontendPrepared() {
  if (prepareFrontendPromise != null) {
    return prepareFrontendPromise
  }

  prepareFrontendPromise = (async () => {
    const shouldBuildFrontend = process.env.E2E_BUILD_FRONTEND === '1'
    if (shouldBuildFrontend) {
      const npmCommand = process.platform === 'win32' ? 'npm.cmd' : 'npm'
      await run(npmCommand, ['run', 'build'], frontendDir)
      await rm(embeddedDistDir, { recursive: true, force: true })
      await mkdir(embeddedDistDir, { recursive: true })
      await cp(frontendDistDir, embeddedDistDir, { recursive: true })
      return
    }

    try {
      await stat(path.join(embeddedDistDir, 'index.html'))
    } catch {
      await mkdir(embeddedDistDir, { recursive: true })
      await cp(frontendDistDir, embeddedDistDir, { recursive: true })
    }
  })()

  return prepareFrontendPromise
}

async function pickFreePort(): Promise<number> {
  return new Promise((resolve, reject) => {
    const server = net.createServer()
    server.listen(0, '127.0.0.1', () => {
      const address = server.address()
      if (address == null || typeof address === 'string') {
        reject(new Error('failed to allocate free port'))
        return
      }
      const port = address.port
      server.close((error) => {
        if (error != null) {
          reject(error)
          return
        }
        resolve(port)
      })
    })
    server.on('error', reject)
  })
}

async function waitForHealth(baseURL: string, timeoutMs = 45000) {
  const startedAt = Date.now()
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(`${baseURL}/health`)
      if (response.ok) return
    } catch {
      // ignore and retry
    }
    await new Promise((resolve) => setTimeout(resolve, 500))
  }
  throw new Error(`等待后端健康检查超时: ${baseURL}/health`)
}

async function resolveWorkflowSeedConfigDir(): Promise<string | null> {
  const raw = process.env.E2E_WORKFLOW_CONFIG_DIR?.trim()
  if (raw != null) {
    if (raw === '' || raw.toLowerCase() === 'none') {
      return null
    }
    const explicit = path.isAbsolute(raw) ? raw : path.resolve(repoRoot, raw)
    try {
      await stat(path.join(explicit, 'classifier.db'))
      return explicit
    } catch (error) {
      const nodeError = error as NodeJS.ErrnoException
      if (nodeError.code === 'ENOENT') {
        return null
      }
      throw error
    }
  }

  for (const candidate of [defaultWorkflowSeedConfigDir, homeWorkflowSeedConfigDir]) {
    try {
      await stat(path.join(candidate, 'classifier.db'))
      return candidate
    } catch (error) {
      const nodeError = error as NodeJS.ErrnoException
      if (nodeError.code === 'ENOENT') {
        continue
      }
      throw error
    }
  }

  return null
}

async function seedWorkflowConfig(configDir: string) {
  const seedDir = await resolveWorkflowSeedConfigDir()
  if (seedDir == null) {
    return
  }

  let entries: Awaited<ReturnType<typeof readdir>>
  try {
    entries = await readdir(seedDir, { withFileTypes: true })
  } catch (error) {
    const nodeError = error as NodeJS.ErrnoException
    if (nodeError.code === 'ENOENT') {
      return
    }
    throw error
  }

  const dbFiles = entries.filter((entry) => entry.isFile() && entry.name.startsWith('classifier.db'))
  for (const file of dbFiles) {
    await copyFile(path.join(seedDir, file.name), path.join(configDir, file.name))
  }
}

function createState(): ScenarioState {
  return {
    folderIDsByName: {},
    folderIDsByPath: {},
    workflowDefIDsByName: {},
    workflowRunStatusByID: {},
    scenarioData: {},
    jobIDs: [],
    workflowRunIDs: [],
  }
}

async function startBackend(paths: RuntimePaths, port: number) {
  const goCommand = process.platform === 'win32' ? 'go.exe' : 'go'
  const server = spawn(goCommand, ['run', './cmd/server'], {
    cwd: backendDir,
    stdio: 'inherit',
    env: {
      ...process.env,
      CONFIG_DIR: paths.configDir,
      SOURCE_DIR: paths.sourceDir,
      TARGET_DIR: paths.targetDir,
      DELETE_STAGING_DIR: paths.deleteStagingDir,
      PORT: String(port),
      CGO_ENABLED: '0',
    },
  })
  return server
}

async function executeUnits(ctx: ScenarioRuntimeContext) {
  for (const step of ctx.scenario.steps) {
    await step.run(ctx)
  }
  for (const assertion of ctx.scenario.assertions) {
    await assertion.run(ctx)
  }
}

export async function runFullstackScenario(args: {
  scenario: E2EScenario
  browser: Browser
  playwright: Playwright
  testInfo: TestInfo
}) {
  await ensureFrontendPrepared()

  const runtimeRoot = path.join(
    repoRoot,
    '.e2e-runtime',
    `${args.scenario.id}-${args.testInfo.workerIndex}-${Date.now()}`,
  )

  const paths = await prepareRuntimePaths(runtimeRoot)
  await seedWorkflowConfig(paths.configDir)
  await materializeTemplate(args.scenario.directoryTemplate, paths)
  if (process.env.E2E_PRINT_TREE === '1') {
    await printRuntimeTree(paths.sourceDir, `scenario=${args.scenario.id} source`)
  }
  const port = await pickFreePort()
  const baseURL = `http://127.0.0.1:${port}`

  const backend = await startBackend(paths, port)
  let terminated = false
  const terminateBackend = async () => {
    if (terminated) return
    terminated = true
    backend.kill('SIGTERM')
    const exitedGracefully = await new Promise<boolean>((resolve) => {
      const timer = setTimeout(() => resolve(false), 3000)
      backend.once('exit', () => {
        clearTimeout(timer)
        resolve(true)
      })
    })
    if (!exitedGracefully) {
      backend.kill('SIGKILL')
    }
  }

  try {
    await waitForHealth(baseURL)
    const request = await args.playwright.request.newContext({ baseURL })
    const page = await args.browser.newPage()
    const ctx: ScenarioRuntimeContext = {
      scenario: args.scenario,
      browser: args.browser,
      page,
      request,
      baseURL,
      paths,
      state: createState(),
      testInfo: args.testInfo,
    }

    try {
      await executeUnits(ctx)
    } finally {
      await page.close()
      await request.dispose()
    }
  } finally {
    await terminateBackend()
    await rm(runtimeRoot, { recursive: true, force: true })
  }
}
