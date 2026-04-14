import type { E2EScenario, ScenarioTag } from './types'

function normalizeTag(tag: string): ScenarioTag | null {
  const normalized = tag.trim().toLowerCase()
  if (normalized === '') return null
  const allTags: ScenarioTag[] = ['complex', 'smoke', 'scan', 'classify', 'process', 'rollback', 'history', 'output-check', 'lineage', 'docker']
  return allTags.includes(normalized as ScenarioTag) ? (normalized as ScenarioTag) : null
}

export function readTagFilterFromEnv(): ScenarioTag[] {
  const raw = process.env.E2E_TAGS ?? ''
  if (raw.trim() === '') return []
  return raw
    .split(',')
    .map(normalizeTag)
    .filter((tag): tag is ScenarioTag => tag != null)
}

export function filterScenariosByTags(
  scenarios: E2EScenario[],
  tags: ScenarioTag[],
): E2EScenario[] {
  if (tags.length === 0) return scenarios
  return scenarios.filter((scenario) => scenario.tags.some((tag) => tags.includes(tag)))
}
