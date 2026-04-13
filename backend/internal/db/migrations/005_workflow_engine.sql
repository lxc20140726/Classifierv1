ALTER TABLE jobs ADD COLUMN workflow_def_id TEXT;

CREATE TABLE IF NOT EXISTS workflow_definitions (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    graph_json TEXT NOT NULL,
    is_active  INTEGER NOT NULL DEFAULT 1,
    version    INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS workflow_runs (
    id              TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL,
    folder_id       TEXT NOT NULL,
    workflow_def_id TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    resume_node_id  TEXT,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS node_runs (
    id              TEXT PRIMARY KEY,
    workflow_run_id TEXT NOT NULL,
    node_id         TEXT NOT NULL,
    node_type       TEXT NOT NULL,
    sequence        INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    input_json      TEXT,
    output_json     TEXT,
    error           TEXT,
    started_at      DATETIME,
    finished_at     DATETIME,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS node_snapshots (
    id              TEXT PRIMARY KEY,
    node_run_id     TEXT NOT NULL,
    workflow_run_id TEXT NOT NULL,
    kind            TEXT NOT NULL,
    fs_manifest     TEXT,
    output_json     TEXT,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_workflow_runs_job_id ON workflow_runs(job_id);
CREATE INDEX IF NOT EXISTS idx_workflow_runs_folder_id ON workflow_runs(folder_id);
CREATE INDEX IF NOT EXISTS idx_node_runs_workflow_run_id ON node_runs(workflow_run_id);
CREATE INDEX IF NOT EXISTS idx_node_snapshots_node_run_id ON node_snapshots(node_run_id);
