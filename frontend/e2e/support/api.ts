import type { Page, Route } from '@playwright/test'

export async function mockEventStream(page: Page) {
  await page.route('**/api/events', async (route) => {
    await route.fulfill({
      status: 200,
      contentType: 'text/event-stream',
      body: '',
    })
  })
}

export function json(body: unknown, status = 200) {
  return {
    status,
    contentType: 'application/json',
    body: JSON.stringify(body),
  }
}

export function pathname(route: Route) {
  return new URL(route.request().url()).pathname
}

export function method(route: Route) {
  return route.request().method()
}
