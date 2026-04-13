PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS folders (
    id              TEXT PRIMARY KEY,
    path            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL DEFAULT 'other',
    category_source TEXT NOT NULL DEFAULT 'auto',
    status          TEXT NOT NULL DEFAULT 'pending',
    image_count     INTEGER NOT NULL DEFAULT 0,
    video_count     INTEGER NOT NULL DEFAULT 0,
    total_files     INTEGER NOT NULL DEFAULT 0,
    total_size      INTEGER NOT NULL DEFAULT 0,
    marked_for_move INTEGER NOT NULL DEFAULT 0,
    scanned_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS snapshots (
    id             TEXT PRIMARY KEY,
    job_id         TEXT NOT NULL,
    folder_id      TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    before_state   TEXT NOT NULL,
    after_state    TEXT,
    status         TEXT NOT NULL DEFAULT 'pending',
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id          TEXT PRIMARY KEY,
    job_id      TEXT,
    folder_id   TEXT,
    folder_path TEXT NOT NULL,
    action      TEXT NOT NULL,
    level       TEXT NOT NULL DEFAULT 'info',
    detail      TEXT,
    result      TEXT NOT NULL,
    error_msg   TEXT,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
