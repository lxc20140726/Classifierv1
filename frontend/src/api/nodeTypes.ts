import { request } from '@/api/client'
import type { NodeSchema } from '@/types'

export function listNodeTypes() {
  return request<{ data: NodeSchema[] }>('/node-types')
}
