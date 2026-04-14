import { buildBaselineScenarios } from './baselineScenarios'
import { filterScenariosByTags, readTagFilterFromEnv } from '../framework/tagFilter'

import type { E2EScenario } from '../framework/types'

export function loadScenarios(): E2EScenario[] {
  return buildBaselineScenarios()
}

export function loadFilteredScenarios(): E2EScenario[] {
  const all = loadScenarios()
  const tags = readTagFilterFromEnv()
  return filterScenariosByTags(all, tags)
}
