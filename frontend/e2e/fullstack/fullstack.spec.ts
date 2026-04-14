import { test } from '@playwright/test'

import { readTagFilterFromEnv } from './framework/tagFilter'
import { loadFilteredScenarios } from './scenarios'
import { runFullstackScenario } from './runtime/fullstackRuntime'

test.describe.configure({ mode: 'serial' })

const selectedScenarios = loadFilteredScenarios()
const selectedTags = readTagFilterFromEnv()

if (selectedScenarios.length === 0) {
  test('没有匹配标签的 fullstack 场景', async () => {
    test.skip(true, `当前 E2E_TAGS=${selectedTags.join(',')} 未匹配到任何场景`)
  })
} else {
  for (const scenario of selectedScenarios) {
    test(`[${scenario.tags.join(',')}] ${scenario.name}`, async ({ browser, playwright }, testInfo) => {
      test.setTimeout(180000)
      await runFullstackScenario({
        scenario,
        browser,
        playwright,
        testInfo,
      })
    })
  }
}
