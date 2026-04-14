import { spawn } from 'node:child_process'
import { mkdir, rm, writeFile } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const frontendDir = path.resolve(__dirname, '../..')
const repoRoot = path.resolve(frontendDir, '..')
const runtimeRoot = path.join(repoRoot, '.e2e-docker')
const sourceDir = path.join(runtimeRoot, 'source')
const targetDir = path.join(runtimeRoot, 'target')
const envFile = path.join(runtimeRoot, '.env')
const appPort = '28080'

function run(command, args, cwd) {
  return new Promise((resolve, reject) => {
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

async function waitForHealth(timeoutMs = 180000) {
  const startedAt = Date.now()
  while (Date.now() - startedAt < timeoutMs) {
    try {
      const response = await fetch(`http://127.0.0.1:${appPort}/health`)
      if (response.ok) return
    } catch {
      // retry
    }
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
  throw new Error('Docker 服务健康检查超时')
}

async function prepareRuntime() {
  await rm(runtimeRoot, { recursive: true, force: true })
  await mkdir(sourceDir, { recursive: true })
  await mkdir(targetDir, { recursive: true })

  await mkdir(path.join(sourceDir, 'docker-photo'), { recursive: true })
  await writeFile(path.join(sourceDir, 'docker-photo', 'sample.jpg'), 'jpg-sample')
  await mkdir(path.join(sourceDir, 'docker-video'), { recursive: true })
  await writeFile(path.join(sourceDir, 'docker-video', 'sample.mp4'), 'mp4-sample')

  const envContent = [
    `NAS_ROOT=${runtimeRoot}`,
    `SOURCE_DIR=${sourceDir}`,
    `TARGET_DIR=${targetDir}`,
    `APP_PORT=${appPort}`,
    'TZ=Asia/Shanghai',
  ].join('\n')
  await writeFile(envFile, `${envContent}\n`)
}

async function dockerUp() {
  await run('docker', ['compose', '--env-file', envFile, 'up', '-d', '--build'], repoRoot)
}

async function dockerDown() {
  try {
    await run('docker', ['compose', '--env-file', envFile, 'down', '-v'], repoRoot)
  } catch {
    // ignore cleanup error
  }
}

await prepareRuntime()
await dockerUp()
await waitForHealth()

const shutdown = async () => {
  await dockerDown()
  process.exit(0)
}

process.on('SIGINT', () => {
  void shutdown()
})
process.on('SIGTERM', () => {
  void shutdown()
})

setInterval(() => {
  // keep process alive for Playwright webServer lifecycle
}, 1000)
