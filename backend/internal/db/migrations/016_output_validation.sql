CREATE TABLE IF NOT EXISTS folder_source_manifests (
    id            TEXT PRIMARY KEY,
    folder_id     TEXT NOT NULL,
    batch_id      TEXT NOT NULL,
    source_path   TEXT NOT NULL,
    relative_path TEXT NOT NULL,
    file_name     TEXT NOT NULL,
    size_bytes    INTEGER NOT NULL DEFAULT 0,
    created_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_folder_source_manifests_folder_id ON folder_source_manifests(folder_id);
CREATE INDEX IF NOT EXISTS idx_folder_source_manifests_folder_batch ON folder_source_manifests(folder_id, batch_id);
CREATE UNIQUE INDEX IF NOT EXISTS ux_folder_source_manifests_batch_relative ON folder_source_manifests(folder_id, batch_id, relative_path);

CREATE TABLE IF NOT EXISTS folder_output_mappings (
    id                  TEXT PRIMARY KEY,
    workflow_run_id     TEXT NOT NULL,
    folder_id           TEXT NOT NULL,
    source_path         TEXT NOT NULL,
    source_relative_path TEXT NOT NULL DEFAULT '',
    output_path         TEXT NOT NULL,
    output_container    TEXT NOT NULL DEFAULT '',
    node_type           TEXT NOT NULL,
    artifact_type       TEXT NOT NULL DEFAULT 'primary',
    required_artifact   INTEGER NOT NULL DEFAULT 0,
    created_at          DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_folder_output_mappings_workflow_run_id ON folder_output_mappings(workflow_run_id);
CREATE INDEX IF NOT EXISTS idx_folder_output_mappings_folder_id ON folder_output_mappings(folder_id);
CREATE INDEX IF NOT EXISTS idx_folder_output_mappings_folder_workflow ON folder_output_mappings(folder_id, workflow_run_id);

CREATE TABLE IF NOT EXISTS folder_output_checks (
    id             TEXT PRIMARY KEY,
    folder_id      TEXT NOT NULL,
    workflow_run_id TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL,
    mismatch_count INTEGER NOT NULL DEFAULT 0,
    failed_files   TEXT NOT NULL DEFAULT '[]',
    errors         TEXT NOT NULL DEFAULT '[]',
    checked_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_at     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_folder_output_checks_folder_id ON folder_output_checks(folder_id);
CREATE INDEX IF NOT EXISTS idx_folder_output_checks_workflow_run_id ON folder_output_checks(workflow_run_id);

ALTER TABLE folders ADD COLUMN output_check_summary TEXT NOT NULL DEFAULT '{"status":"pending","workflow_run_id":"","checked_at":null,"mismatch_count":0,"failed_files":[]}';
