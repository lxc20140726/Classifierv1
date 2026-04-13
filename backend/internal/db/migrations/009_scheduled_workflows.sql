CREATE TABLE IF NOT EXISTS scheduled_workflows (
    id              TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    job_type        TEXT NOT NULL DEFAULT 'workflow',
    workflow_def_id TEXT NOT NULL,
    folder_ids      TEXT NOT NULL DEFAULT '[]',
    source_dirs     TEXT NOT NULL DEFAULT '[]',
    cron_spec       TEXT NOT NULL,
    enabled         INTEGER NOT NULL DEFAULT 1,
    last_run_at     DATETIME,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_scheduled_workflows_enabled ON scheduled_workflows(enabled);
CREATE INDEX IF NOT EXISTS idx_scheduled_workflows_workflow_def_id ON scheduled_workflows(workflow_def_id);
CREATE INDEX IF NOT EXISTS idx_audit_logs_job_id ON audit_logs(job_id);
