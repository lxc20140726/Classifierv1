import { request } from '@/api/client'
import type { AppConfig } from '@/types'

export async function getConfig(): Promise<{ data: AppConfig }> {
  return request<{ data: AppConfig }>('/config')
}

export function updateConfig(values: AppConfig) {
  return request<{ saved: boolean; data: AppConfig }>('/config', {
    method: 'PUT',
    body: JSON.stringify(values),
  })
}
