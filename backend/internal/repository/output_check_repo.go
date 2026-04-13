package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

type SQLiteOutputCheckRepository struct {
	db *sql.DB
}

func NewOutputCheckRepository(db *sql.DB) OutputCheckRepository {
	return &SQLiteOutputCheckRepository{db: db}
}

func (r *SQLiteOutputCheckRepository) Create(ctx context.Context, check *FolderOutputCheck) error {
	failedFilesRaw, err := json.Marshal(check.FailedFiles)
	if err != nil {
		return fmt.Errorf("outputCheckRepo.Create marshal failed files: %w", err)
	}
	errorsRaw, err := json.Marshal(check.Errors)
	if err != nil {
		return fmt.Errorf("outputCheckRepo.Create marshal errors: %w", err)
	}

	if _, err := r.db.ExecContext(
		ctx,
		`INSERT INTO folder_output_checks (
	id, folder_id, workflow_run_id, status, mismatch_count, failed_files, errors, checked_at, created_at
) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		check.ID,
		check.FolderID,
		check.WorkflowRunID,
		check.Status,
		check.MismatchCount,
		string(failedFilesRaw),
		string(errorsRaw),
	); err != nil {
		return fmt.Errorf("outputCheckRepo.Create: %w", err)
	}

	check.FailedFilesRaw = failedFilesRaw
	check.ErrorsRaw = errorsRaw
	return nil
}

func (r *SQLiteOutputCheckRepository) GetLatestByFolderID(ctx context.Context, folderID string) (*FolderOutputCheck, error) {
	item, err := scanFolderOutputCheck(r.db.QueryRowContext(
		ctx,
		`SELECT id, folder_id, workflow_run_id, status, mismatch_count, failed_files, errors, checked_at, created_at
FROM folder_output_checks
WHERE folder_id = ?
ORDER BY checked_at DESC, created_at DESC, id DESC
LIMIT 1`,
		folderID,
	))
	if err != nil {
		return nil, fmt.Errorf("outputCheckRepo.GetLatestByFolderID: %w", err)
	}

	return item, nil
}

func (r *SQLiteOutputCheckRepository) UpdateFolderSummary(ctx context.Context, folderID string, summary FolderOutputCheckSummary) error {
	payload := defaultFolderOutputCheckSummary()
	if strings.TrimSpace(summary.Status) != "" {
		payload.Status = strings.TrimSpace(summary.Status)
	}
	payload.WorkflowRunID = strings.TrimSpace(summary.WorkflowRunID)
	payload.CheckedAt = summary.CheckedAt
	payload.MismatchCount = summary.MismatchCount
	payload.FailedFiles = append([]string(nil), summary.FailedFiles...)

	raw, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("outputCheckRepo.UpdateFolderSummary marshal: %w", err)
	}

	res, err := r.db.ExecContext(
		ctx,
		"UPDATE folders SET output_check_summary = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
		string(raw),
		folderID,
	)
	if err != nil {
		return fmt.Errorf("outputCheckRepo.UpdateFolderSummary: %w", err)
	}
	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("outputCheckRepo.UpdateFolderSummary: %w", err)
	}

	return nil
}

func (r *SQLiteOutputCheckRepository) MarkFolderPending(ctx context.Context, folderID string) error {
	return r.UpdateFolderSummary(ctx, folderID, defaultFolderOutputCheckSummary())
}

func scanFolderOutputCheck(scanner interface{ Scan(dest ...any) error }) (*FolderOutputCheck, error) {
	item := &FolderOutputCheck{}
	var failedFilesRaw string
	var errorsRaw string
	var checkedAt any
	var createdAt any

	if err := scanner.Scan(
		&item.ID,
		&item.FolderID,
		&item.WorkflowRunID,
		&item.Status,
		&item.MismatchCount,
		&failedFilesRaw,
		&errorsRaw,
		&checkedAt,
		&createdAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	item.FailedFilesRaw = json.RawMessage(failedFilesRaw)
	item.ErrorsRaw = json.RawMessage(errorsRaw)
	if len(item.FailedFilesRaw) > 0 {
		_ = json.Unmarshal(item.FailedFilesRaw, &item.FailedFiles)
	}
	if len(item.ErrorsRaw) > 0 {
		_ = json.Unmarshal(item.ErrorsRaw, &item.Errors)
	}

	parsedCheckedAt, err := parseDBTime(checkedAt)
	if err != nil {
		return nil, fmt.Errorf("scanFolderOutputCheck parse checked_at: %w", err)
	}
	item.CheckedAt = parsedCheckedAt

	parsedCreatedAt, err := parseDBTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("scanFolderOutputCheck parse created_at: %w", err)
	}
	item.CreatedAt = parsedCreatedAt

	return item, nil
}
