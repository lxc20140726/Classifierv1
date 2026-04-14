import { spawn } from 'node:child_process'
import { mkdir, rm, writeFile, copyFile, readdir } from 'node:fs/promises'
import path from 'node:path'

import type { AssetKind, DirectoryTemplate, RuntimePaths } from '../framework/types'

interface RuntimeSamplePaths {
  jpg: string
  png: string
  webp: string
  mp4: string
  mkv: string
  txt: string
  pdf: string
}

const MINIMAL_PNG_BASE64 = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO7Z6XQAAAAASUVORK5CYII='
const MINIMAL_JPG_BASE64 = '/9j/4AAQSkZJRgABAQAAAQABAAD/2wCEAAkGBxAQEBUQEBAVFRUVFRUVFRUVFRUVFRUWFxUVFRUYHSggGBolGxUVITEhJSkrLi4uFx8zODMsNygtLisBCgoKDg0OGhAQGi0lHyUtLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLf/AABEIAAEAAQMBIgACEQEDEQH/xAAXAAEBAQEAAAAAAAAAAAAAAAAAAQID/8QAFhEBAQEAAAAAAAAAAAAAAAAAABEh/9oADAMBAAIQAxAAAAHeAP/EABQQAQAAAAAAAAAAAAAAAAAAACD/2gAIAQEAAQUCt//EABQRAQAAAAAAAAAAAAAAAAAAACD/2gAIAQMBAT8BX//EABQRAQAAAAAAAAAAAAAAAAAAACD/2gAIAQIBAT8BX//EABQQAQAAAAAAAAAAAAAAAAAAACD/2gAIAQEABj8Cf//Z'
const MINIMAL_PDF = '%PDF-1.1\n1 0 obj\n<< /Type /Catalog /Pages 2 0 R >>\nendobj\n2 0 obj\n<< /Type /Pages /Count 1 /Kids [3 0 R] >>\nendobj\n3 0 obj\n<< /Type /Page /Parent 2 0 R /MediaBox [0 0 100 100] /Contents 4 0 R >>\nendobj\n4 0 obj\n<< /Length 37 >>\nstream\nBT\n/F1 12 Tf\n10 50 Td\n(hi) Tj\nET\nendstream\nendobj\ntrailer\n<< /Root 1 0 R >>\n%%EOF\n'

async function run(command: string, args: string[], cwd: string) {
  await new Promise<void>((resolve, reject) => {
    const child = spawn(command, args, {
      cwd,
      stdio: 'ignore',
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

async function createMediaFixtures(dir: string): Promise<RuntimeSamplePaths> {
  await mkdir(dir, { recursive: true })
  const jpg = path.join(dir, 'tiny.jpg')
  const png = path.join(dir, 'tiny.png')
  const webp = path.join(dir, 'tiny.webp')
  const mp4 = path.join(dir, 'tiny.mp4')
  const mkv = path.join(dir, 'tiny.mkv')
  const txt = path.join(dir, 'tiny.txt')
  const pdf = path.join(dir, 'tiny.pdf')

  await writeFile(jpg, Buffer.from(MINIMAL_JPG_BASE64, 'base64'))
  await writeFile(png, Buffer.from(MINIMAL_PNG_BASE64, 'base64'))
  await writeFile(txt, 'sample-text\n')
  await writeFile(pdf, MINIMAL_PDF)

  try {
    await run('ffmpeg', ['-y', '-f', 'lavfi', '-i', 'color=c=blue:s=32x32:d=1', '-frames:v', '1', webp], dir)
  } catch {
    await copyFile(png, webp)
  }

  try {
    await run('ffmpeg', ['-y', '-f', 'lavfi', '-i', 'testsrc=size=64x64:rate=1:duration=1', '-pix_fmt', 'yuv420p', mp4], dir)
  } catch {
    await writeFile(mp4, 'dummy-mp4')
  }
  try {
    await run('ffmpeg', ['-y', '-f', 'lavfi', '-i', 'testsrc=size=64x64:rate=1:duration=1', mkv], dir)
  } catch {
    await writeFile(mkv, 'dummy-mkv')
  }

  return { jpg, png, webp, mp4, mkv, txt, pdf }
}

function resolveSamplePath(samples: RuntimeSamplePaths, asset: AssetKind): string {
  if (asset === 'jpg') return samples.jpg
  if (asset === 'png') return samples.png
  if (asset === 'webp') return samples.webp
  if (asset === 'mp4') return samples.mp4
  if (asset === 'mkv') return samples.mkv
  if (asset === 'txt') return samples.txt
  if (asset === 'pdf') return samples.pdf
  if (asset === 'cbz') return samples.jpg
  return samples.jpg
}

function resolveTargetName(asset: AssetKind, index: number): string {
  if (asset === 'cbz') return `sample-${index + 1}.cbz`
  if (asset === 'cbr') return `sample-${index + 1}.cbr`
  return `sample-${index + 1}.${asset}`
}

export async function prepareRuntimePaths(root: string): Promise<RuntimePaths> {
  const sourceDir = path.join(root, 'source')
  const targetDir = path.join(root, 'target')
  const configDir = path.join(root, 'config')
  const deleteStagingDir = path.join(root, 'delete-staging')

  await rm(root, { recursive: true, force: true })
  await mkdir(sourceDir, { recursive: true })
  await mkdir(targetDir, { recursive: true })
  await mkdir(configDir, { recursive: true })
  await mkdir(deleteStagingDir, { recursive: true })

  return { root, sourceDir, targetDir, configDir, deleteStagingDir }
}

export async function materializeTemplate(
  template: DirectoryTemplate,
  paths: RuntimePaths,
): Promise<void> {
  const fixtureRoot = path.join(paths.root, '__fixture-library__')
  const samples = await createMediaFixtures(fixtureRoot)

  for (const seed of template.seeds) {
    const targetDir = path.join(paths.sourceDir, seed.relativePath)
    await mkdir(targetDir, { recursive: true })
    for (const [index, asset] of seed.assets.entries()) {
      const samplePath = resolveSamplePath(samples, asset)
      const targetName = resolveTargetName(asset, index)
      await copyFile(samplePath, path.join(targetDir, targetName))
    }
  }
}

async function buildTreeLines(current: string, prefix: string): Promise<string[]> {
  const entries = await readdir(current, { withFileTypes: true })
  entries.sort((a, b) => a.name.localeCompare(b.name))

  const lines: string[] = []
  for (const [index, entry] of entries.entries()) {
    const isLast = index === entries.length - 1
    const connector = isLast ? '└─ ' : '├─ '
    const nextPrefix = `${prefix}${isLast ? '   ' : '│  '}`
    lines.push(`${prefix}${connector}${entry.name}`)
    if (entry.isDirectory()) {
      const nextPath = path.join(current, entry.name)
      const childLines = await buildTreeLines(nextPath, nextPrefix)
      lines.push(...childLines)
    }
  }
  return lines
}

export async function printRuntimeTree(root: string, title = 'runtime tree') {
  const lines = await buildTreeLines(root, '')
  const body = [`[E2E] ${title}: ${root}`, `.`]
  body.push(...lines)
  // eslint-disable-next-line no-console
  console.log(body.join('\n'))
}
