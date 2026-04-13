import { request } from '@/api/client'

export interface FsDirEntry {
  name: string
  path: string
}

export interface FsDirsResponse {
  path: string
  parent: string
  entries: FsDirEntry[]
}

export function listDirs(path: string): Promise<FsDirsResponse> {
  const search = new URLSearchParams({ path })
  return request<FsDirsResponse>(`/fs/dirs?${search.toString()}`)
}
