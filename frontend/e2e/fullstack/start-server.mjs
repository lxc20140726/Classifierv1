import { spawn } from 'node:child_process'
import { mkdir, rm, writeFile, cp } from 'node:fs/promises'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const __filename = fileURLToPath(import.meta.url)
const __dirname = path.dirname(__filename)
const frontendDir = path.resolve(__dirname, '../..')
const repoRoot = path.resolve(frontendDir, '..')
const backendDir = path.join(repoRoot, 'backend')
const runtimeRoot = path.join(repoRoot, '.e2e-fullstack')
const sourceDir = path.join(runtimeRoot, 'source')
const targetDir = path.join(runtimeRoot, 'target')
const configDir = path.join(runtimeRoot, 'config')
const deleteStagingDir = path.join(runtimeRoot, 'delete-staging')
const distDir = path.join(frontendDir, 'dist')
const embeddedDistDir = path.join(backendDir, 'cmd/server/web/dist')
const port = '18080'
const npmCommand = process.platform === 'win32' ? 'npm.cmd' : 'npm'
const goCommand = process.platform === 'win32' ? 'go.exe' : 'go'

async function createFixtureTree() {
  await rm(runtimeRoot, { recursive: true, force: true })
  await mkdir(sourceDir, { recursive: true })
  await mkdir(targetDir, { recursive: true })
  await mkdir(configDir, { recursive: true })
  await mkdir(deleteStagingDir, { recursive: true })

  await mkdir(path.join(sourceDir, 'photo-album'), { recursive: true })
  await writeFile(path.join(sourceDir, 'photo-album', '001.jpg'), 'jpg')
  await writeFile(path.join(sourceDir, 'photo-album', '002.png'), 'png')
  await writeFile(path.join(sourceDir, 'photo-album', '003.webp'), 'webp')

  await mkdir(path.join(sourceDir, 'manual-only'), { recursive: true })
  await writeFile(path.join(sourceDir, 'manual-only', 'notes.txt'), 'notes')

  await mkdir(path.join(sourceDir, 'manga-comic'), { recursive: true })
  await writeFile(path.join(sourceDir, 'manga-comic', 'chapter1.cbz'), 'cbz')

  await mkdir(path.join(sourceDir, 'series-pack', 'leaf-photo'), { recursive: true })
  await writeFile(path.join(sourceDir, 'series-pack', 'leaf-photo', '001.jpg'), 'jpg')
  await mkdir(path.join(sourceDir, 'series-pack', 'leaf-video'), { recursive: true })
  await writeFile(path.join(sourceDir, 'series-pack', 'ep1.mp4'), 'mp4')
  await writeFile(path.join(sourceDir, 'series-pack', 'leaf-video', 'ep2.mkv'), 'mkv')
}

async function syncBuiltFrontend() {
  await rm(embeddedDistDir, { recursive: true, force: true })
  await mkdir(embeddedDistDir, { recursive: true })
  await cp(distDir, embeddedDistDir, { recursive: true })
}

function run(command, args, cwd, extraEnv = {}) {
  return new Promise((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      stdio: 'inherit',
      env: { ...process.env, ...extraEnv },
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

await createFixtureTree()
await run(npmCommand, ['run', 'build'], frontendDir)
await syncBuiltFrontend()

const server = spawn(goCommand, ['run', './cmd/server'], {
  cwd: backendDir,
  stdio: 'inherit',
  env: {
    ...process.env,
    CONFIG_DIR: configDir,
    SOURCE_DIR: sourceDir,
    TARGET_DIR: targetDir,
    DELETE_STAGING_DIR: deleteStagingDir,
    PORT: port,
    CGO_ENABLED: '0',
  },
})

for (const signal of ['SIGINT', 'SIGTERM']) {
  process.on(signal, () => {
    server.kill(signal)
  })
}

server.on('exit', (code) => {
  process.exit(code ?? 1)
})
