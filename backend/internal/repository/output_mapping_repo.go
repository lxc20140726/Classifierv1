package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type SQLiteOutputMappingRepository struct {
	db *sql.DB
}

func NewOutputMappingRepository(db *sql.DB) OutputMappingRepository {
	return &SQLiteOutputMappingRepository{db: db}
}

func (r *SQLiteOutputMappingRepository) ReplaceByWorkflowRunID(ctx context.Context, workflowRunID string, mappings []*FolderOutputMapping) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("outputMappingRepo.ReplaceByWorkflowRunID begin tx: %w", err)
	}
	defer rollbackTx(tx)

	if _, err := tx.ExecContext(
		ctx,
		"DELETE FROM folder_output_mappings WHERE workflow_run_id = ?",
		workflowRunID,
	); err != nil {
		return fmt.Errorf("outputMappingRepo.ReplaceByWorkflowRunID clear existing: %w", err)
	}

	for _, item := range mappings {
		if item == nil {
			continue
		}
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO folder_output_mappings (
	id, workflow_run_id, folder_id, source_path, source_relative_path, output_path, output_container, node_type, artifact_type, required_artifact, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
			item.ID,
			item.WorkflowRunID,
			item.FolderID,
			item.SourcePath,
			item.SourceRelativePath,
			item.OutputPath,
			item.OutputContainer,
			item.NodeType,
			item.ArtifactType,
			boolToInt(item.RequiredArtifact),
		); err != nil {
			return fmt.Errorf("outputMappingRepo.ReplaceByWorkflowRunID insert mapping: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("outputMappingRepo.ReplaceByWorkflowRunID commit: %w", err)
	}

	return nil
}

func (r *SQLiteOutputMappingRepository) ListByWorkflowRunID(ctx context.Context, workflowRunID string) ([]*FolderOutputMapping, error) {
	rows, err := r.db.QueryContext(
		ctx,
		`SELECT id, workflow_run_id, folder_id, source_path, source_relative_path, output_path, output_container, node_type, artifact_type, required_artifact, created_at
FROM folder_output_mappings
WHERE workflow_run_id = ?
ORDER BY created_at ASC, id ASC`,
		workflowRunID,
	)
	if err != nil {
		return nil, fmt.Errorf("outputMappingRepo.ListByWorkflowRunID query: %w", err)
	}
	defer rows.Close()

	items := make([]*FolderOutputMapping, 0)
	for rows.Next() {
		item, scanErr := scanFolderOutputMapping(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("outputMappingRepo.ListByWorkflowRunID scan: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outputMappingRepo.ListByWorkflowRunID rows: %w", err)
	}

	return items, nil
}

func (r *SQLiteOutputMappingRepository) ListByWorkflowRunAndFolderID(ctx context.Context, workflowRunID, folderID string) ([]*FolderOutputMapping, error) {
	rows, err := r.db.QueryContext(
		ctx,
		`SELECT id, workflow_run_id, folder_id, source_path, source_relative_path, output_path, output_container, node_type, artifact_type, required_artifact, created_at
FROM folder_output_mappings
WHERE workflow_run_id = ? AND folder_id = ?
ORDER BY created_at ASC, id ASC`,
		workflowRunID,
		folderID,
	)
	if err != nil {
		return nil, fmt.Errorf("outputMappingRepo.ListByWorkflowRunAndFolderID query: %w", err)
	}
	defer rows.Close()

	items := make([]*FolderOutputMapping, 0)
	for rows.Next() {
		item, scanErr := scanFolderOutputMapping(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("outputMappingRepo.ListByWorkflowRunAndFolderID scan: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outputMappingRepo.ListByWorkflowRunAndFolderID rows: %w", err)
	}

	return items, nil
}

func (r *SQLiteOutputMappingRepository) GetLatestWorkflowRunIDByFolderID(ctx context.Context, folderID string) (string, error) {
	var workflowRunID string
	if err := r.db.QueryRowContext(
		ctx,
		`SELECT workflow_run_id
FROM folder_output_mappings
WHERE folder_id = ?
ORDER BY created_at DESC, id DESC
LIMIT 1`,
		folderID,
	).Scan(&workflowRunID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", ErrNotFound
		}
		return "", fmt.Errorf("outputMappingRepo.GetLatestWorkflowRunIDByFolderID: %w", err)
	}

	return workflowRunID, nil
}

func scanFolderOutputMapping(scanner interface{ Scan(dest ...any) error }) (*FolderOutputMapping, error) {
	item := &FolderOutputMapping{}
	var requiredArtifact int
	var createdAt any

	if err := scanner.Scan(
		&item.ID,
		&item.WorkflowRunID,
		&item.FolderID,
		&item.SourcePath,
		&item.SourceRelativePath,
		&item.OutputPath,
		&item.OutputContainer,
		&item.NodeType,
		&item.ArtifactType,
		&requiredArtifact,
		&createdAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	item.RequiredArtifact = intToBool(requiredArtifact)
	parsedCreatedAt, err := parseDBTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("scanFolderOutputMapping parse created_at: %w", err)
	}
	item.CreatedAt = parsedCreatedAt

	return item, nil
}
