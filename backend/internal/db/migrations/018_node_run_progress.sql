ALTER TABLE node_runs ADD COLUMN progress_percent INTEGER;
ALTER TABLE node_runs ADD COLUMN progress_done INTEGER;
ALTER TABLE node_runs ADD COLUMN progress_total INTEGER;
ALTER TABLE node_runs ADD COLUMN progress_stage TEXT;
ALTER TABLE node_runs ADD COLUMN progress_message TEXT;
ALTER TABLE node_runs ADD COLUMN progress_source_path TEXT;
ALTER TABLE node_runs ADD COLUMN progress_target_path TEXT;
ALTER TABLE node_runs ADD COLUMN progress_updated_at DATETIME;
