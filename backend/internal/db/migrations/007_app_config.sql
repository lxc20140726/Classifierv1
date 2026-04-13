CREATE TABLE IF NOT EXISTS app_config (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    version    INTEGER NOT NULL,
    value      TEXT NOT NULL,
    checksum   TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
