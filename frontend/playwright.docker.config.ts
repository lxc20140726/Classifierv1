import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: './e2e/docker',
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: 'http://127.0.0.1:28080',
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: 'node ./e2e/docker/start-docker-stack.mjs',
    url: 'http://127.0.0.1:28080/health',
    reuseExistingServer: false,
    timeout: 600000,
  },
})
