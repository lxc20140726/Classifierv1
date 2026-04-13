ALTER TABLE folders ADD COLUMN source_dir TEXT NOT NULL DEFAULT '';
ALTER TABLE folders ADD COLUMN relative_path TEXT NOT NULL DEFAULT '';
ALTER TABLE snapshots ADD COLUMN detail TEXT;

CREATE INDEX IF NOT EXISTS idx_folders_source_dir ON folders(source_dir);
