CREATE TABLE IF NOT EXISTS jobs (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',
    folder_ids  TEXT NOT NULL DEFAULT '[]',
    total       INTEGER NOT NULL DEFAULT 0,
    done        INTEGER NOT NULL DEFAULT 0,
    failed      INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    started_at  DATETIME,
    finished_at DATETIME,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

ALTER TABLE folders ADD COLUMN deleted_at DATETIME;
ALTER TABLE folders ADD COLUMN delete_staging_path TEXT;

CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at);
CREATE INDEX IF NOT EXISTS idx_folders_deleted_at ON folders(deleted_at);
CREATE INDEX IF NOT EXISTS idx_folders_path_active ON folders(path) WHERE deleted_at IS NULL;
