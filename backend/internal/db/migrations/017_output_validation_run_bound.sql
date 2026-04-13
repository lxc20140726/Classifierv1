ALTER TABLE folder_source_manifests ADD COLUMN workflow_run_id TEXT;
CREATE INDEX IF NOT EXISTS idx_folder_source_manifests_run_folder ON folder_source_manifests(workflow_run_id, folder_id);
CREATE INDEX IF NOT EXISTS idx_folder_source_manifests_run_folder_relative ON folder_source_manifests(workflow_run_id, folder_id, relative_path);

ALTER TABLE folders ADD COLUMN identity_fingerprint TEXT NOT NULL DEFAULT '';
