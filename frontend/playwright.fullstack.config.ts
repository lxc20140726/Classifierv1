import { defineConfig, devices } from '@playwright/test'

export default defineConfig({
  testDir: './e2e/fullstack',
  fullyParallel: false,
  workers: 1,
  retries: process.env.CI ? 1 : 0,
  reporter: [['list'], ['html', { open: 'never' }]],
  use: {
    baseURL: 'http://127.0.0.1:18080',
    trace: 'on-first-retry',
  },
  projects: [
    {
      name: 'chromium',
      use: { ...devices['Desktop Chrome'] },
    },
  ],
  webServer: {
    command: 'node ./e2e/fullstack/start-server.mjs',
    url: 'http://127.0.0.1:18080/health',
    reuseExistingServer: false,
    timeout: 180000,
  },
})
