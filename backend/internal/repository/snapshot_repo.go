package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
)

type SQLiteSnapshotRepository struct {
	db *sql.DB
}

func NewSnapshotRepository(db *sql.DB) SnapshotRepository {
	return &SQLiteSnapshotRepository{db: db}
}

func (r *SQLiteSnapshotRepository) Create(ctx context.Context, s *Snapshot) error {
	_, err := r.db.ExecContext(
		ctx,
		`INSERT INTO snapshots (id, job_id, folder_id, operation_type, before_state, after_state, detail, status, created_at)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		s.ID,
		s.JobID,
		s.FolderID,
		s.OperationType,
		string(s.Before),
		nullableJSON(s.After),
		nullableJSON(s.Detail),
		s.Status,
	)
	if err != nil {
		return fmt.Errorf("snapshotRepo.Create: %w", err)
	}

	return nil
}

func (r *SQLiteSnapshotRepository) GetByID(ctx context.Context, id string) (*Snapshot, error) {
	snapshot, err := scanSnapshot(
		r.db.QueryRowContext(ctx, `
SELECT id, job_id, folder_id, operation_type, before_state, after_state, detail, status, created_at
FROM snapshots
WHERE id = ?`, id),
	)
	if err != nil {
		return nil, fmt.Errorf("snapshotRepo.GetByID: %w", err)
	}

	return snapshot, nil
}

func (r *SQLiteSnapshotRepository) ListByFolderID(ctx context.Context, folderID string) ([]*Snapshot, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT id, job_id, folder_id, operation_type, before_state, after_state, detail, status, created_at
FROM snapshots
WHERE folder_id = ?
ORDER BY created_at DESC`, folderID)
	if err != nil {
		return nil, fmt.Errorf("snapshotRepo.ListByFolderID: %w", err)
	}
	defer rows.Close()

	return collectSnapshots(rows)
}

func (r *SQLiteSnapshotRepository) ListByJobID(ctx context.Context, jobID string) ([]*Snapshot, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT id, job_id, folder_id, operation_type, before_state, after_state, detail, status, created_at
FROM snapshots
WHERE job_id = ?
ORDER BY created_at DESC`, jobID)
	if err != nil {
		return nil, fmt.Errorf("snapshotRepo.ListByJobID: %w", err)
	}
	defer rows.Close()

	return collectSnapshots(rows)
}

func (r *SQLiteSnapshotRepository) CommitAfter(ctx context.Context, id string, after json.RawMessage) error {
	res, err := r.db.ExecContext(
		ctx,
		"UPDATE snapshots SET after_state = ? WHERE id = ?",
		string(after),
		id,
	)
	if err != nil {
		return fmt.Errorf("snapshotRepo.CommitAfter: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("snapshotRepo.CommitAfter: %w", err)
	}

	return nil
}

func (r *SQLiteSnapshotRepository) UpdateDetail(ctx context.Context, id string, detail json.RawMessage) error {
	res, err := r.db.ExecContext(
		ctx,
		"UPDATE snapshots SET detail = ? WHERE id = ?",
		nullableJSON(detail),
		id,
	)
	if err != nil {
		return fmt.Errorf("snapshotRepo.UpdateDetail: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("snapshotRepo.UpdateDetail: %w", err)
	}

	return nil
}

func (r *SQLiteSnapshotRepository) UpdateStatus(ctx context.Context, id, status string) error {
	res, err := r.db.ExecContext(
		ctx,
		"UPDATE snapshots SET status = ? WHERE id = ?",
		status,
		id,
	)
	if err != nil {
		return fmt.Errorf("snapshotRepo.UpdateStatus: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("snapshotRepo.UpdateStatus: %w", err)
	}

	return nil
}

func collectSnapshots(rows *sql.Rows) ([]*Snapshot, error) {
	items := make([]*Snapshot, 0)
	for rows.Next() {
		item, err := scanSnapshot(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return items, nil
}

func scanSnapshot(scanner interface{ Scan(dest ...any) error }) (*Snapshot, error) {
	snapshot := &Snapshot{}
	var before string
	var after sql.NullString
	var detail sql.NullString
	var createdAt any

	err := scanner.Scan(
		&snapshot.ID,
		&snapshot.JobID,
		&snapshot.FolderID,
		&snapshot.OperationType,
		&before,
		&after,
		&detail,
		&snapshot.Status,
		&createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	snapshot.Before = json.RawMessage(before)

	if after.Valid {
		snapshot.After = json.RawMessage(after.String)
	}

	if detail.Valid {
		snapshot.Detail = json.RawMessage(detail.String)
	}

	snapshot.CreatedAt, err = parseDBTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("scanSnapshot parse created_at: %w", err)
	}

	return snapshot, nil
}
