export interface EntryPreviewItem {
  path: string
  name: string
  category: string
  confidence: number
  subdirs?: EntryPreviewItem[]
}

export interface ClassificationPreviewSummary {
  total: number
  top_level_count: number
  by_category: Record<string, number>
  avg_confidence: number
  classifier_sources: string[]
  entries: EntryPreviewItem[]
}

export interface DirStepResult {
  node_type: string
  node_label: string
  status: string
  error?: string
}

export interface DirProcessingSummary {
  source_path: string
  steps: DirStepResult[]
  succeeded: number
  failed: number
}

export interface ProcessingPreviewSummary {
  total_dirs: number
  total_steps: number
  succeeded: number
  failed: number
  by_directory: DirProcessingSummary[]
}

export type NodePreviewSummary = ClassificationPreviewSummary | ProcessingPreviewSummary

