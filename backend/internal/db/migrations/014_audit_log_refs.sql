ALTER TABLE audit_logs ADD COLUMN workflow_run_id TEXT;
ALTER TABLE audit_logs ADD COLUMN node_run_id TEXT;
ALTER TABLE audit_logs ADD COLUMN node_id TEXT;
ALTER TABLE audit_logs ADD COLUMN node_type TEXT;

CREATE INDEX IF NOT EXISTS idx_audit_workflow_run_id ON audit_logs(workflow_run_id);
CREATE INDEX IF NOT EXISTS idx_audit_node_run_id ON audit_logs(node_run_id);
CREATE INDEX IF NOT EXISTS idx_audit_node_id ON audit_logs(node_id);
CREATE INDEX IF NOT EXISTS idx_audit_node_type ON audit_logs(node_type);
