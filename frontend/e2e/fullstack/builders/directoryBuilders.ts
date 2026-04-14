import type { AssetKind, DirectorySeed, DirectoryTemplate } from '../framework/types'

function seed(relativePath: string, assets: AssetKind[]): DirectorySeed {
  return { relativePath, assets }
}

export function createCustomDirectory(relativePath: string, assets: AssetKind[]): DirectorySeed {
  return seed(relativePath, assets)
}

export function createPhotoDirectory(relativePath: string): DirectorySeed {
  return seed(relativePath, ['jpg', 'png', 'webp'])
}

export function createVideoDirectory(relativePath: string): DirectorySeed {
  return seed(relativePath, ['mp4', 'mkv'])
}

export function createMangaDirectory(relativePath: string): DirectorySeed {
  return seed(relativePath, ['cbz', 'cbr'])
}

export function createMixedDirectory(relativePath: string): DirectorySeed {
  return seed(relativePath, ['jpg', 'mp4', 'txt'])
}

export function createNoiseDirectory(relativePath: string): DirectorySeed {
  return seed(relativePath, ['txt', 'pdf'])
}

export function createEmptyDirectory(relativePath: string): DirectorySeed {
  return seed(relativePath, [])
}

export function createDirectoryTemplate(seeds: DirectorySeed[]): DirectoryTemplate {
  return { seeds }
}

function atRoot(root: string, relativePath: string): string {
  if (relativePath.trim() === '') return root
  return `${root}/${relativePath}`
}

export function createLayeredComplexDirectoryTemplate(root: string): DirectoryTemplate {
  return createDirectoryTemplate([
    createCustomDirectory(atRoot(root, ''), ['jpg', 'mp4', 'txt']),
    createVideoDirectory(atRoot(root, 'layer-a/video-leaf-a')),
    createPhotoDirectory(atRoot(root, 'layer-a/photo-leaf-a')),
    createMixedDirectory(atRoot(root, 'layer-a/mixed-leaf-a')),
    createNoiseDirectory(atRoot(root, 'layer-a/other-leaf-a')),
    createMixedDirectory(atRoot(root, 'layer-b/nested-parent/mixed-grand-leaf')),
  ])
}

export function createMixedStandaloneProcessingTemplate(root: string): DirectoryTemplate {
  return createDirectoryTemplate([
    createVideoDirectory(atRoot(root, 'video-only-leaf')),
    createPhotoDirectory(atRoot(root, 'photo-only-leaf')),
    createCustomDirectory(atRoot(root, 'mixed-leaf'), ['mp4', 'jpg', 'txt', 'pdf']),
  ])
}

export function createBatchMediaSiblingTemplate(root: string): DirectoryTemplate {
  return createDirectoryTemplate([
    createCustomDirectory(atRoot(root, 'video-sibling-a'), ['mp4', 'mkv', 'mp4']),
    createCustomDirectory(atRoot(root, 'video-sibling-b'), ['mkv', 'mp4', 'mp4']),
    createCustomDirectory(atRoot(root, 'photo-sibling-a'), ['jpg', 'png', 'webp']),
    createCustomDirectory(atRoot(root, 'photo-sibling-b'), ['jpg', 'png', 'jpg']),
    createCustomDirectory(atRoot(root, 'mixed-sibling-a'), ['mp4', 'jpg', 'txt']),
  ])
}

export function createDocumentHybridSiblingTemplate(root: string): DirectoryTemplate {
  return createDirectoryTemplate([
    createCustomDirectory(atRoot(root, 'docs-only-leaf'), ['txt', 'pdf', 'txt']),
    createCustomDirectory(atRoot(root, 'docs-photo-leaf'), ['txt', 'pdf', 'jpg', 'png', 'jpg']),
    createCustomDirectory(atRoot(root, 'docs-video-leaf'), ['txt', 'pdf', 'mp4', 'mkv', 'mp4']),
    createCustomDirectory(atRoot(root, 'docs-mixed-leaf'), ['txt', 'pdf', 'jpg', 'mp4', 'png']),
  ])
}

export function createRollbackComplexTemplate(root: string): DirectoryTemplate {
  return createDirectoryTemplate([
    createCustomDirectory(atRoot(root, 'rollback-video-leaf'), ['mp4', 'mkv', 'mp4']),
    createCustomDirectory(atRoot(root, 'rollback-photo-leaf'), ['jpg', 'png', 'webp']),
    createCustomDirectory(atRoot(root, 'rollback-mixed-leaf'), ['jpg', 'mp4', 'txt', 'pdf']),
    createNoiseDirectory(atRoot(root, 'rollback-other-leaf')),
  ])
}
