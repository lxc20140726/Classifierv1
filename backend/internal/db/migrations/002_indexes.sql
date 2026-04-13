CREATE INDEX IF NOT EXISTS idx_folders_status    ON folders(status);
CREATE INDEX IF NOT EXISTS idx_folders_category  ON folders(category);
CREATE INDEX IF NOT EXISTS idx_snapshots_job     ON snapshots(job_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_folder  ON snapshots(folder_id);
CREATE INDEX IF NOT EXISTS idx_audit_folder      ON audit_logs(folder_id);
CREATE INDEX IF NOT EXISTS idx_audit_action      ON audit_logs(action);
CREATE INDEX IF NOT EXISTS idx_audit_created     ON audit_logs(created_at);
