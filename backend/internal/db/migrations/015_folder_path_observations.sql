DROP INDEX IF EXISTS ux_folders_path_active;
DROP TABLE IF EXISTS folders_legacy_identity;

CREATE TABLE IF NOT EXISTS folder_path_observations (
    id            TEXT PRIMARY KEY,
    folder_id     TEXT NOT NULL,
    path          TEXT NOT NULL,
    source_dir    TEXT NOT NULL DEFAULT '',
    relative_path TEXT NOT NULL DEFAULT '',
    is_current    INTEGER NOT NULL DEFAULT 0,
    first_seen_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO folder_path_observations (
    id, folder_id, path, source_dir, relative_path, is_current, first_seen_at, last_seen_at
)
WITH ranked_folders AS (
    SELECT
        id,
        path,
        source_dir,
        relative_path,
        scanned_at,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY path
            ORDER BY COALESCE(updated_at, scanned_at, CURRENT_TIMESTAMP) DESC, id
        ) AS path_rank,
        CASE
            WHEN TRIM(COALESCE(source_dir, '')) <> '' AND TRIM(COALESCE(relative_path, '')) <> '' THEN
                ROW_NUMBER() OVER (
                    PARTITION BY source_dir, relative_path
                    ORDER BY COALESCE(updated_at, scanned_at, CURRENT_TIMESTAMP) DESC, id
                )
            ELSE 1
        END AS source_relative_rank
    FROM folders
)
SELECT
    'obs-' || id,
    id,
    path,
    source_dir,
    relative_path,
    CASE
        WHEN path_rank = 1 AND source_relative_rank = 1 THEN 1
        ELSE 0
    END,
    COALESCE(scanned_at, CURRENT_TIMESTAMP),
    COALESCE(updated_at, scanned_at, CURRENT_TIMESTAMP)
FROM ranked_folders
WHERE path IS NOT NULL
  AND TRIM(path) <> ''
  AND NOT EXISTS (
      SELECT 1
      FROM folder_path_observations o
      WHERE o.folder_id = ranked_folders.id
        AND o.path = ranked_folders.path
  );

CREATE INDEX IF NOT EXISTS idx_folder_path_observations_path ON folder_path_observations(path);
CREATE INDEX IF NOT EXISTS idx_folder_path_observations_folder_current ON folder_path_observations(folder_id, is_current);
CREATE INDEX IF NOT EXISTS idx_folder_path_observations_source_relative_current ON folder_path_observations(source_dir, relative_path, is_current);
CREATE UNIQUE INDEX IF NOT EXISTS ux_folder_path_observations_current_path ON folder_path_observations(path) WHERE is_current = 1;
CREATE UNIQUE INDEX IF NOT EXISTS ux_folder_path_observations_folder_current ON folder_path_observations(folder_id) WHERE is_current = 1;
CREATE UNIQUE INDEX IF NOT EXISTS ux_folder_path_observations_source_relative_current ON folder_path_observations(source_dir, relative_path) WHERE is_current = 1 AND TRIM(source_dir) <> '' AND TRIM(relative_path) <> '';

ALTER TABLE folders RENAME TO folders_legacy_identity;

CREATE TABLE IF NOT EXISTS folders (
    id                  TEXT PRIMARY KEY,
    path                TEXT NOT NULL,
    name                TEXT NOT NULL,
    category            TEXT NOT NULL DEFAULT 'other',
    category_source     TEXT NOT NULL DEFAULT 'auto',
    status              TEXT NOT NULL DEFAULT 'pending',
    image_count         INTEGER NOT NULL DEFAULT 0,
    video_count         INTEGER NOT NULL DEFAULT 0,
    total_files         INTEGER NOT NULL DEFAULT 0,
    total_size          INTEGER NOT NULL DEFAULT 0,
    marked_for_move     INTEGER NOT NULL DEFAULT 0,
    scanned_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at          DATETIME DEFAULT CURRENT_TIMESTAMP,
    deleted_at          DATETIME,
    delete_staging_path TEXT,
    source_dir          TEXT NOT NULL DEFAULT '',
    relative_path       TEXT NOT NULL DEFAULT '',
    cover_image_path    TEXT NOT NULL DEFAULT '',
    other_file_count    INTEGER NOT NULL DEFAULT 0,
    has_other_files     INTEGER NOT NULL DEFAULT 0
);

INSERT INTO folders (
    id, path, name, category, category_source, status,
    image_count, video_count, total_files, total_size, marked_for_move,
    scanned_at, updated_at, deleted_at, delete_staging_path,
    source_dir, relative_path, cover_image_path, other_file_count, has_other_files
)
SELECT
    id, path, name, category, category_source, status,
    image_count, video_count, total_files, total_size, marked_for_move,
    scanned_at, updated_at, deleted_at, delete_staging_path,
    source_dir, relative_path, COALESCE(cover_image_path, ''), COALESCE(other_file_count, 0), COALESCE(has_other_files, 0)
FROM folders_legacy_identity;

DROP TABLE folders_legacy_identity;

CREATE INDEX IF NOT EXISTS idx_folders_status ON folders(status);
CREATE INDEX IF NOT EXISTS idx_folders_category ON folders(category);
CREATE INDEX IF NOT EXISTS idx_folders_deleted_at ON folders(deleted_at);
CREATE INDEX IF NOT EXISTS idx_folders_source_dir ON folders(source_dir);
