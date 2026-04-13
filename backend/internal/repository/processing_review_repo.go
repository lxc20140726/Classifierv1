package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type SQLiteProcessingReviewRepository struct {
	db *sql.DB
}

func NewProcessingReviewRepository(db *sql.DB) ProcessingReviewRepository {
	return &SQLiteProcessingReviewRepository{db: db}
}

func (r *SQLiteProcessingReviewRepository) Create(ctx context.Context, item *ProcessingReviewItem) error {
	_, err := r.db.ExecContext(
		ctx,
		`INSERT INTO processing_review_items (
id, workflow_run_id, job_id, folder_id, status, before_json, after_json, step_results_json, diff_json, error, reviewed_at, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		item.ID,
		item.WorkflowRunID,
		item.JobID,
		item.FolderID,
		item.Status,
		nullableString(string(item.BeforeJSON)),
		nullableString(string(item.AfterJSON)),
		nullableString(string(item.StepResultsJSON)),
		nullableString(string(item.DiffJSON)),
		nullableString(item.Error),
		nullableTime(item.ReviewedAt),
	)
	if err != nil {
		return fmt.Errorf("processingReviewRepo.Create: %w", err)
	}

	return nil
}

func (r *SQLiteProcessingReviewRepository) GetByID(ctx context.Context, id string) (*ProcessingReviewItem, error) {
	item, err := scanProcessingReviewItem(r.db.QueryRowContext(ctx, `
SELECT id, workflow_run_id, job_id, folder_id, status, before_json, after_json, step_results_json, diff_json, error, created_at, updated_at, reviewed_at
FROM processing_review_items
WHERE id = ?`, id))
	if err != nil {
		return nil, fmt.Errorf("processingReviewRepo.GetByID: %w", err)
	}

	return item, nil
}

func (r *SQLiteProcessingReviewRepository) ListByWorkflowRunID(ctx context.Context, workflowRunID string) ([]*ProcessingReviewItem, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT id, workflow_run_id, job_id, folder_id, status, before_json, after_json, step_results_json, diff_json, error, created_at, updated_at, reviewed_at
FROM processing_review_items
WHERE workflow_run_id = ?
ORDER BY created_at ASC`, workflowRunID)
	if err != nil {
		return nil, fmt.Errorf("processingReviewRepo.ListByWorkflowRunID query: %w", err)
	}
	defer rows.Close()

	items := make([]*ProcessingReviewItem, 0)
	for rows.Next() {
		item, scanErr := scanProcessingReviewItem(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("processingReviewRepo.ListByWorkflowRunID scan: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("processingReviewRepo.ListByWorkflowRunID rows: %w", err)
	}

	return items, nil
}

func (r *SQLiteProcessingReviewRepository) GetLatestByFolderID(ctx context.Context, folderID string) (*ProcessingReviewItem, error) {
	item, err := scanProcessingReviewItem(r.db.QueryRowContext(ctx, `
SELECT id, workflow_run_id, job_id, folder_id, status, before_json, after_json, step_results_json, diff_json, error, created_at, updated_at, reviewed_at
FROM processing_review_items
WHERE folder_id = ?
ORDER BY COALESCE(reviewed_at, updated_at, created_at) DESC, updated_at DESC, created_at DESC, id DESC
LIMIT 1`, folderID))
	if err != nil {
		return nil, fmt.Errorf("processingReviewRepo.GetLatestByFolderID: %w", err)
	}

	return item, nil
}

func (r *SQLiteProcessingReviewRepository) UpdateDecision(ctx context.Context, id, status, errMsg string, reviewedAt *time.Time) error {
	res, err := r.db.ExecContext(
		ctx,
		`UPDATE processing_review_items
SET status = ?, error = ?, reviewed_at = ?, updated_at = CURRENT_TIMESTAMP
WHERE id = ?`,
		status,
		nullableString(errMsg),
		nullableTime(reviewedAt),
		id,
	)
	if err != nil {
		return fmt.Errorf("processingReviewRepo.UpdateDecision: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("processingReviewRepo.UpdateDecision: %w", err)
	}

	return nil
}

func scanProcessingReviewItem(scanner interface{ Scan(dest ...any) error }) (*ProcessingReviewItem, error) {
	item := &ProcessingReviewItem{}
	var beforeJSON sql.NullString
	var afterJSON sql.NullString
	var stepResultsJSON sql.NullString
	var diffJSON sql.NullString
	var errMsg sql.NullString
	var createdAt any
	var updatedAt any
	var reviewedAt any

	err := scanner.Scan(
		&item.ID,
		&item.WorkflowRunID,
		&item.JobID,
		&item.FolderID,
		&item.Status,
		&beforeJSON,
		&afterJSON,
		&stepResultsJSON,
		&diffJSON,
		&errMsg,
		&createdAt,
		&updatedAt,
		&reviewedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if beforeJSON.Valid {
		item.BeforeJSON = jsonRawMessageOrNil(beforeJSON.String)
	}
	if afterJSON.Valid {
		item.AfterJSON = jsonRawMessageOrNil(afterJSON.String)
	}
	if stepResultsJSON.Valid {
		item.StepResultsJSON = jsonRawMessageOrNil(stepResultsJSON.String)
	}
	if diffJSON.Valid {
		item.DiffJSON = jsonRawMessageOrNil(diffJSON.String)
	}
	if errMsg.Valid {
		item.Error = errMsg.String
	}
	created, err := parseDBTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("scanProcessingReviewItem parse created_at: %w", err)
	}
	item.CreatedAt = created
	updated, err := parseDBTime(updatedAt)
	if err != nil {
		return nil, fmt.Errorf("scanProcessingReviewItem parse updated_at: %w", err)
	}
	item.UpdatedAt = updated
	item.ReviewedAt, err = parseNullableTime(reviewedAt)
	if err != nil {
		return nil, fmt.Errorf("scanProcessingReviewItem parse reviewed_at: %w", err)
	}

	return item, nil
}

func jsonRawMessageOrNil(raw string) []byte {
	if raw == "" {
		return nil
	}
	return []byte(raw)
}
