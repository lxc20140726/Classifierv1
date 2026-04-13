package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type SQLiteSourceManifestRepository struct {
	db *sql.DB
}

func NewSourceManifestRepository(db *sql.DB) SourceManifestRepository {
	return &SQLiteSourceManifestRepository{db: db}
}

func (r *SQLiteSourceManifestRepository) CreateBatch(ctx context.Context, folderID, batchID string, manifests []*FolderSourceManifest) error {
	return r.createBatch(ctx, "", folderID, batchID, manifests)
}

func (r *SQLiteSourceManifestRepository) CreateBatchForWorkflowRun(ctx context.Context, workflowRunID, folderID, batchID string, manifests []*FolderSourceManifest) error {
	return r.createBatch(ctx, workflowRunID, folderID, batchID, manifests)
}

func (r *SQLiteSourceManifestRepository) createBatch(ctx context.Context, workflowRunID, folderID, batchID string, manifests []*FolderSourceManifest) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("sourceManifestRepo.createBatch begin tx: %w", err)
	}
	defer rollbackTx(tx)

	if _, err := tx.ExecContext(
		ctx,
		"DELETE FROM folder_source_manifests WHERE folder_id = ? AND batch_id = ?",
		folderID,
		batchID,
	); err != nil {
		return fmt.Errorf("sourceManifestRepo.createBatch clear existing batch: %w", err)
	}

	for _, item := range manifests {
		if item == nil {
			continue
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO folder_source_manifests (
	id, workflow_run_id, folder_id, batch_id, source_path, relative_path, file_name, size_bytes, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
			item.ID,
			nullIfEmpty(item.WorkflowRunID, workflowRunID),
			item.FolderID,
			item.BatchID,
			item.SourcePath,
			item.RelativePath,
			item.FileName,
			item.SizeBytes,
		); err != nil {
			return fmt.Errorf("sourceManifestRepo.createBatch insert manifest: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sourceManifestRepo.createBatch commit: %w", err)
	}

	return nil
}

func (r *SQLiteSourceManifestRepository) ListLatestByFolderID(ctx context.Context, folderID string) ([]*FolderSourceManifest, error) {
	var batchID string
	err := r.db.QueryRowContext(
		ctx,
		`SELECT batch_id
FROM folder_source_manifests
WHERE folder_id = ?
ORDER BY created_at DESC, batch_id DESC
LIMIT 1`,
		folderID,
	).Scan(&batchID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return []*FolderSourceManifest{}, nil
		}
		return nil, fmt.Errorf("sourceManifestRepo.ListLatestByFolderID query batch: %w", err)
	}

	rows, err := r.db.QueryContext(
		ctx,
		`SELECT id, workflow_run_id, folder_id, batch_id, source_path, relative_path, file_name, size_bytes, created_at
FROM folder_source_manifests
WHERE folder_id = ? AND batch_id = ?
ORDER BY relative_path ASC`,
		folderID,
		batchID,
	)
	if err != nil {
		return nil, fmt.Errorf("sourceManifestRepo.ListLatestByFolderID query manifests: %w", err)
	}
	defer rows.Close()

	items := make([]*FolderSourceManifest, 0)
	for rows.Next() {
		item, scanErr := scanFolderSourceManifest(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("sourceManifestRepo.ListLatestByFolderID scan: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sourceManifestRepo.ListLatestByFolderID rows: %w", err)
	}

	return items, nil
}

func (r *SQLiteSourceManifestRepository) ListByFolderID(ctx context.Context, folderID string) ([]*FolderSourceManifest, error) {
	rows, err := r.db.QueryContext(
		ctx,
		`SELECT id, workflow_run_id, folder_id, batch_id, source_path, relative_path, file_name, size_bytes, created_at
FROM folder_source_manifests
WHERE folder_id = ?
ORDER BY created_at DESC, batch_id DESC, relative_path ASC, id ASC`,
		folderID,
	)
	if err != nil {
		return nil, fmt.Errorf("sourceManifestRepo.ListByFolderID query manifests: %w", err)
	}
	defer rows.Close()

	items := make([]*FolderSourceManifest, 0)
	for rows.Next() {
		item, scanErr := scanFolderSourceManifest(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("sourceManifestRepo.ListByFolderID scan: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sourceManifestRepo.ListByFolderID rows: %w", err)
	}

	return items, nil
}

func (r *SQLiteSourceManifestRepository) ListByWorkflowRunAndFolderID(ctx context.Context, workflowRunID, folderID string) ([]*FolderSourceManifest, error) {
	rows, err := r.db.QueryContext(
		ctx,
		`SELECT id, workflow_run_id, folder_id, batch_id, source_path, relative_path, file_name, size_bytes, created_at
FROM folder_source_manifests
WHERE workflow_run_id = ? AND folder_id = ?
ORDER BY relative_path ASC, id ASC`,
		workflowRunID,
		folderID,
	)
	if err != nil {
		return nil, fmt.Errorf("sourceManifestRepo.ListByWorkflowRunAndFolderID query manifests: %w", err)
	}
	defer rows.Close()

	items := make([]*FolderSourceManifest, 0)
	for rows.Next() {
		item, scanErr := scanFolderSourceManifest(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("sourceManifestRepo.ListByWorkflowRunAndFolderID scan: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sourceManifestRepo.ListByWorkflowRunAndFolderID rows: %w", err)
	}

	return items, nil
}

func (r *SQLiteSourceManifestRepository) ExistsByWorkflowRunAndFolderID(ctx context.Context, workflowRunID, folderID string) (bool, error) {
	var one int
	err := r.db.QueryRowContext(
		ctx,
		`SELECT 1
FROM folder_source_manifests
WHERE workflow_run_id = ? AND folder_id = ?
LIMIT 1`,
		workflowRunID,
		folderID,
	).Scan(&one)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("sourceManifestRepo.ExistsByWorkflowRunAndFolderID: %w", err)
	}
	return true, nil
}

func scanFolderSourceManifest(scanner interface{ Scan(dest ...any) error }) (*FolderSourceManifest, error) {
	item := &FolderSourceManifest{}
	var workflowRunID sql.NullString
	var createdAt any

	if err := scanner.Scan(
		&item.ID,
		&workflowRunID,
		&item.FolderID,
		&item.BatchID,
		&item.SourcePath,
		&item.RelativePath,
		&item.FileName,
		&item.SizeBytes,
		&createdAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if workflowRunID.Valid {
		item.WorkflowRunID = workflowRunID.String
	}

	parsedCreatedAt, err := parseDBTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("scanFolderSourceManifest parse created_at: %w", err)
	}
	item.CreatedAt = parsedCreatedAt

	return item, nil
}

func nullIfEmpty(primary, fallback string) any {
	value := primary
	if value == "" {
		value = fallback
	}
	if value == "" {
		return nil
	}
	return value
}
