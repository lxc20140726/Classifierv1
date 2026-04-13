ALTER TABLE workflow_definitions ADD COLUMN description TEXT NOT NULL DEFAULT '';

ALTER TABLE workflow_runs ADD COLUMN last_node_id TEXT;
ALTER TABLE workflow_runs ADD COLUMN external_blocks INTEGER NOT NULL DEFAULT 0;
ALTER TABLE workflow_runs ADD COLUMN error TEXT;
ALTER TABLE workflow_runs ADD COLUMN started_at DATETIME;
ALTER TABLE workflow_runs ADD COLUMN finished_at DATETIME;

ALTER TABLE node_runs ADD COLUMN input_signature TEXT;
ALTER TABLE node_runs ADD COLUMN resume_token TEXT;

ALTER TABLE node_snapshots ADD COLUMN compensation TEXT;
