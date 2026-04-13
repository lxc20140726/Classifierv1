ALTER TABLE jobs ADD COLUMN source_dir TEXT NOT NULL DEFAULT '';

ALTER TABLE node_runs ADD COLUMN resume_data TEXT NOT NULL DEFAULT '';

ALTER TABLE workflow_runs ADD COLUMN source_dir TEXT NOT NULL DEFAULT '';

CREATE TABLE workflow_runs_v2 (
    id              TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL,
    folder_id       TEXT,
    source_dir      TEXT NOT NULL DEFAULT '',
    workflow_def_id TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    resume_node_id  TEXT,
    last_node_id    TEXT,
    external_blocks INTEGER NOT NULL DEFAULT 0,
    error           TEXT,
    started_at      DATETIME,
    finished_at     DATETIME,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO workflow_runs_v2 (
    id,
    job_id,
    folder_id,
    source_dir,
    workflow_def_id,
    status,
    resume_node_id,
    last_node_id,
    external_blocks,
    error,
    started_at,
    finished_at,
    created_at,
    updated_at
)
SELECT
    id,
    job_id,
    folder_id,
    source_dir,
    workflow_def_id,
    status,
    resume_node_id,
    last_node_id,
    external_blocks,
    error,
    started_at,
    finished_at,
    created_at,
    updated_at
FROM workflow_runs;

DROP TABLE workflow_runs;
ALTER TABLE workflow_runs_v2 RENAME TO workflow_runs;

CREATE INDEX IF NOT EXISTS idx_workflow_runs_job_id ON workflow_runs(job_id);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_folder_id ON workflow_runs(folder_id);
