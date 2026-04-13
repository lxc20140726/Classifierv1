ALTER TABLE scheduled_workflows ADD COLUMN job_type TEXT NOT NULL DEFAULT 'workflow';
ALTER TABLE scheduled_workflows ADD COLUMN source_dirs TEXT NOT NULL DEFAULT '[]';
