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
