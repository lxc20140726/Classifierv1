import type { ApiError } from '@/types'

const API_BASE = '/api'

export class ApiRequestError extends Error {
  readonly status: number
  readonly body: Record<string, unknown> | null

  constructor(message: string, status: number, body: Record<string, unknown> | null = null) {
    super(message)
    this.name = 'ApiRequestError'
    this.status = status
    this.body = body
  }
}

export async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      'Content-Type': 'application/json',
      ...(init?.headers ?? {}),
    },
    ...init,
  })

  if (!response.ok) {
    let message = `Request failed: ${response.status}`
    let body: Record<string, unknown> | null = null

    try {
      body = (await response.json()) as Record<string, unknown>
      const payload = body as ApiError & Record<string, unknown>
      if (payload.error && typeof payload.error === 'string') {
        message = payload.error
      }
    } catch {
      message = response.statusText || message
    }

    throw new ApiRequestError(message, response.status, body)
  }

  if (response.status === 204) {
    return undefined as T
  }

  return (await response.json()) as T
}
