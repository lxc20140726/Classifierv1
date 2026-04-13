package db

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenRunsMigrations(t *testing.T) {
	t.Parallel()

	dsn := filepath.Join(t.TempDir(), "classifier.db")
	database, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer database.Close()

	tables := []string{"folders", "snapshots", "audit_logs", "config", "app_config"}
	for _, table := range tables {
		t.Run(table, func(t *testing.T) {
			if !sqliteObjectExists(t, database, "table", table) {
				t.Fatalf("expected table %q to exist", table)
			}
		})
	}

	indexes := []string{
		"idx_folders_status",
		"idx_folders_category",
		"idx_snapshots_job",
		"idx_snapshots_folder",
		"idx_audit_folder",
		"idx_audit_action",
		"idx_audit_created",
		"idx_audit_workflow_run_id",
		"idx_audit_node_run_id",
		"idx_audit_node_id",
		"idx_audit_node_type",
	}

	for _, index := range indexes {
		t.Run(index, func(t *testing.T) {
			if !sqliteObjectExists(t, database, "index", index) {
				t.Fatalf("expected index %q to exist", index)
			}
		})
	}
}

func TestOpenEnablesPragmas(t *testing.T) {
	t.Parallel()

	dsn := filepath.Join(t.TempDir(), "classifier_pragmas.db")
	database, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer database.Close()

	var foreignKeys int
	if err := database.QueryRow("PRAGMA foreign_keys;").Scan(&foreignKeys); err != nil {
		t.Fatalf("query PRAGMA foreign_keys error = %v", err)
	}

	if foreignKeys != 1 {
		t.Fatalf("foreign_keys = %d, want 1", foreignKeys)
	}

	var journalMode string
	if err := database.QueryRow("PRAGMA journal_mode;").Scan(&journalMode); err != nil {
		t.Fatalf("query PRAGMA journal_mode error = %v", err)
	}

	if journalMode != "wal" {
		t.Fatalf("journal_mode = %q, want %q", journalMode, "wal")
	}
}

func TestOpenAddsAuditReferenceColumns(t *testing.T) {
	t.Parallel()

	dsn := filepath.Join(t.TempDir(), "classifier_audit_columns.db")
	database, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	defer database.Close()

	rows, err := database.Query("PRAGMA table_info(audit_logs)")
	if err != nil {
		t.Fatalf("PRAGMA table_info(audit_logs) error = %v", err)
	}
	defer rows.Close()

	columns := make(map[string]struct{})
	for rows.Next() {
		var (
			cid        int
			name       string
			colType    string
			notNull    int
			defaultVal sql.NullString
			primaryKey int
		)
		if err := rows.Scan(&cid, &name, &colType, &notNull, &defaultVal, &primaryKey); err != nil {
			t.Fatalf("scan table_info row error = %v", err)
		}
		columns[strings.TrimSpace(name)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("table_info rows error = %v", err)
	}

	required := []string{"workflow_run_id", "node_run_id", "node_id", "node_type"}
	for _, column := range required {
		if _, ok := columns[column]; !ok {
			t.Fatalf("missing audit_logs column %q", column)
		}
	}
}

func TestOpenRemainsIdempotentWhenFoldersContainDuplicatePaths(t *testing.T) {
	t.Parallel()

	dsn := filepath.Join(t.TempDir(), "classifier_duplicate_paths.db")
	database, err := Open(dsn)
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}

	_, err = database.Exec(`
INSERT INTO folders (
	id, path, name, category, category_source, status,
	image_count, video_count, total_files, total_size, marked_for_move,
	scanned_at, updated_at, deleted_at, delete_staging_path,
	source_dir, relative_path, cover_image_path, other_file_count, has_other_files
) VALUES
	('folder-1', '/same/path', 'folder-1', 'other', 'auto', 'pending', 0, 0, 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, NULL, '', '', '', 0, 0),
	('folder-2', '/same/path', 'folder-2', 'other', 'auto', 'pending', 0, 0, 0, 0, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, NULL, NULL, '', '', '', 0, 0)
`)
	if err != nil {
		database.Close()
		t.Fatalf("insert duplicate folders error = %v", err)
	}

	if err := database.Close(); err != nil {
		t.Fatalf("database.Close() error = %v", err)
	}

	database, err = Open(dsn)
	if err != nil {
		t.Fatalf("Open() on migrated db with duplicate folder paths error = %v", err)
	}
	defer database.Close()
}

func sqliteObjectExists(t *testing.T, db *sql.DB, objectType, name string) bool {
	t.Helper()

	var count int
	err := db.QueryRow(
		"SELECT COUNT(*) FROM sqlite_master WHERE type = ? AND name = ?",
		objectType,
		name,
	).Scan(&count)
	if err != nil {
		t.Fatalf("query sqlite_master for %s %s failed: %v", objectType, name, err)
	}

	return count > 0
}

func TestOpenInvalidPath(t *testing.T) {
	t.Parallel()

	_, err := Open("/path/that/does/not/exist/classifier.db")
	if err == nil {
		t.Fatalf("expected Open() to fail for invalid path")
	}

	if got := err.Error(); got == "" {
		t.Fatalf("expected error text, got empty string")
	}

	_ = fmt.Sprintf("%v", err)
}
