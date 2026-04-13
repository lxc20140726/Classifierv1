package db

import (
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	_ "modernc.org/sqlite"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func Open(path string) (*sql.DB, error) {
	database, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("db.Open sql.Open: %w", err)
	}
	// SQLite 在并发写入时容易触发 SQLITE_BUSY；限制连接池并启用 busy_timeout
	// 可以让并发节点写库时排队等待，而不是立即失败。
	database.SetMaxOpenConns(1)
	database.SetMaxIdleConns(1)

	if _, err := database.Exec("PRAGMA journal_mode=WAL;"); err != nil {
		database.Close()
		return nil, fmt.Errorf("db.Open enable WAL: %w", err)
	}
	if _, err := database.Exec("PRAGMA busy_timeout=5000;"); err != nil {
		database.Close()
		return nil, fmt.Errorf("db.Open set busy timeout: %w", err)
	}

	if _, err := database.Exec("PRAGMA foreign_keys=ON;"); err != nil {
		database.Close()
		return nil, fmt.Errorf("db.Open enable foreign keys: %w", err)
	}

	if err := runMigrations(database); err != nil {
		database.Close()
		return nil, err
	}

	return database, nil
}

func runMigrations(database *sql.DB) error {
	paths, err := fs.Glob(migrationFiles, "migrations/*.sql")
	if err != nil {
		return fmt.Errorf("runMigrations glob: %w", err)
	}

	sort.Strings(paths)

	for _, path := range paths {
		contents, err := fs.ReadFile(migrationFiles, path)
		if err != nil {
			return fmt.Errorf("runMigrations read %s: %w", path, err)
		}

		sqlText := strings.TrimSpace(string(contents))
		if sqlText == "" {
			continue
		}

		for _, stmt := range strings.Split(sqlText, ";") {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}

			if _, err := database.Exec(stmt); err != nil {
				if strings.Contains(err.Error(), "duplicate column name") {
					continue
				}
				return fmt.Errorf("runMigrations exec %s: %w", path, err)
			}
		}
	}

	return nil
}
