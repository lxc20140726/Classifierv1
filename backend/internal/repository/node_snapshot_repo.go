package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type SQLiteNodeSnapshotRepository struct {
	db *sql.DB
}

func NewNodeSnapshotRepository(db *sql.DB) NodeSnapshotRepository {
	return &SQLiteNodeSnapshotRepository{db: db}
}

func (r *SQLiteNodeSnapshotRepository) Create(ctx context.Context, item *NodeSnapshot) error {
	_, err := r.db.ExecContext(
		ctx,
		`INSERT INTO node_snapshots (
	id, node_run_id, workflow_run_id, kind, fs_manifest, output_json, compensation, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		item.ID,
		item.NodeRunID,
		item.WorkflowRunID,
		item.Kind,
		nullableString(item.FSManifest),
		nullableString(item.OutputJSON),
		nullableString(item.Compensation),
	)
	if err != nil {
		return fmt.Errorf("nodeSnapshotRepo.Create: %w", err)
	}

	return nil
}

func (r *SQLiteNodeSnapshotRepository) ListByNodeRunID(ctx context.Context, nodeRunID string) ([]*NodeSnapshot, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT id, node_run_id, workflow_run_id, kind, fs_manifest, output_json, compensation, created_at
FROM node_snapshots
WHERE node_run_id = ?
ORDER BY created_at ASC`, nodeRunID)
	if err != nil {
		return nil, fmt.Errorf("nodeSnapshotRepo.ListByNodeRunID query: %w", err)
	}
	defer rows.Close()

	items := make([]*NodeSnapshot, 0)
	for rows.Next() {
		item, scanErr := scanNodeSnapshot(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("nodeSnapshotRepo.ListByNodeRunID scan: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("nodeSnapshotRepo.ListByNodeRunID rows: %w", err)
	}

	return items, nil
}

func scanNodeSnapshot(scanner interface{ Scan(dest ...any) error }) (*NodeSnapshot, error) {
	item := &NodeSnapshot{}
	var fsManifest sql.NullString
	var outputJSON sql.NullString
	var compensation sql.NullString
	var createdAt any

	err := scanner.Scan(
		&item.ID,
		&item.NodeRunID,
		&item.WorkflowRunID,
		&item.Kind,
		&fsManifest,
		&outputJSON,
		&compensation,
		&createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if fsManifest.Valid {
		item.FSManifest = fsManifest.String
	}
	if outputJSON.Valid {
		item.OutputJSON = outputJSON.String
	}
	if compensation.Valid {
		item.Compensation = compensation.String
	}
	item.CreatedAt, err = parseDBTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("scanNodeSnapshot parse created_at: %w", err)
	}

	return item, nil
}
