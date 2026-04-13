package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type SQLiteWorkflowRunRepository struct {
	db *sql.DB
}

func NewWorkflowRunRepository(db *sql.DB) WorkflowRunRepository {
	return &SQLiteWorkflowRunRepository{db: db}
}

func (r *SQLiteWorkflowRunRepository) Create(ctx context.Context, item *WorkflowRun) error {
	_, err := r.db.ExecContext(
		ctx,
		`INSERT INTO workflow_runs (
	id, job_id, folder_id, source_dir, workflow_def_id, status, resume_node_id, last_node_id, external_blocks, error, started_at, finished_at, created_at, updated_at
 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		item.ID,
		item.JobID,
		item.FolderID,
		item.SourceDir,
		item.WorkflowDefID,
		item.Status,
		nullableString(item.ResumeNodeID),
		nullableString(item.LastNodeID),
		item.ExternalBlocks,
		nullableString(item.Error),
		nullableTime(item.StartedAt),
		nullableTime(item.FinishedAt),
	)
	if err != nil {
		return fmt.Errorf("workflowRunRepo.Create: %w", err)
	}

	return nil
}

func (r *SQLiteWorkflowRunRepository) GetByID(ctx context.Context, id string) (*WorkflowRun, error) {
	item, err := scanWorkflowRun(r.db.QueryRowContext(ctx, `
SELECT id, job_id, folder_id, source_dir, workflow_def_id, status, resume_node_id, last_node_id, external_blocks, error, started_at, finished_at, created_at, updated_at
FROM workflow_runs
WHERE id = ?`, id))
	if err != nil {
		return nil, fmt.Errorf("workflowRunRepo.GetByID: %w", err)
	}

	return item, nil
}

func (r *SQLiteWorkflowRunRepository) List(ctx context.Context, filter WorkflowRunListFilter) ([]*WorkflowRun, int, error) {
	where := make([]string, 0, 3)
	args := make([]any, 0, 3)

	if filter.JobID != "" {
		where = append(where, "job_id = ?")
		args = append(args, filter.JobID)
	}
	if filter.FolderID != "" {
		where = append(where, "folder_id = ?")
		args = append(args, filter.FolderID)
	}
	if filter.Status != "" {
		where = append(where, "status = ?")
		args = append(args, filter.Status)
	}

	whereClause := ""
	if len(where) > 0 {
		whereClause = " WHERE " + strings.Join(where, " AND ")
	}

	var total int
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM workflow_runs"+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("workflowRunRepo.List count: %w", err)
	}

	page := filter.Page
	if page <= 0 {
		page = 1
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 20
	}
	offset := (page - 1) * limit
	listArgs := append(append([]any{}, args...), limit, offset)

	rows, err := r.db.QueryContext(ctx, `
SELECT id, job_id, folder_id, source_dir, workflow_def_id, status, resume_node_id, last_node_id, external_blocks, error, started_at, finished_at, created_at, updated_at
FROM workflow_runs`+whereClause+`
ORDER BY created_at DESC
LIMIT ? OFFSET ?`, listArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("workflowRunRepo.List query: %w", err)
	}
	defer rows.Close()

	items := make([]*WorkflowRun, 0)
	for rows.Next() {
		item, scanErr := scanWorkflowRun(rows)
		if scanErr != nil {
			return nil, 0, fmt.Errorf("workflowRunRepo.List scan: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("workflowRunRepo.List rows: %w", err)
	}

	return items, total, nil
}

func (r *SQLiteWorkflowRunRepository) UpdateStatus(ctx context.Context, id, status, resumeNodeID string) error {
	res, err := r.db.ExecContext(
		ctx,
		`UPDATE workflow_runs
SET status = ?,
	resume_node_id = ?,
	last_node_id = ?,
	error = CASE WHEN ? = 'failed' THEN error ELSE '' END,
	started_at = CASE WHEN ? = 'running' THEN COALESCE(started_at, CURRENT_TIMESTAMP) ELSE started_at END,
	finished_at = CASE WHEN ? IN ('succeeded', 'failed', 'rolled_back') THEN CURRENT_TIMESTAMP ELSE finished_at END,
	updated_at = CURRENT_TIMESTAMP
WHERE id = ?`,
		status,
		nullableString(resumeNodeID),
		nullableString(resumeNodeID),
		status,
		status,
		status,
		id,
	)
	if err != nil {
		return fmt.Errorf("workflowRunRepo.UpdateStatus: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("workflowRunRepo.UpdateStatus: %w", err)
	}

	return nil
}

func (r *SQLiteWorkflowRunRepository) UpdateFailure(ctx context.Context, id, lastNodeID, errMsg string) error {
	res, err := r.db.ExecContext(
		ctx,
		`UPDATE workflow_runs
SET status = 'failed',
	resume_node_id = ?,
	last_node_id = ?,
	error = ?,
	finished_at = CURRENT_TIMESTAMP,
	updated_at = CURRENT_TIMESTAMP
WHERE id = ?`,
		nullableString(lastNodeID),
		nullableString(lastNodeID),
		nullableString(errMsg),
		id,
	)
	if err != nil {
		return fmt.Errorf("workflowRunRepo.UpdateFailure: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("workflowRunRepo.UpdateFailure: %w", err)
	}

	return nil
}

func (r *SQLiteWorkflowRunRepository) UpdateBlocks(ctx context.Context, id string, delta int) error {
	res, err := r.db.ExecContext(
		ctx,
		`UPDATE workflow_runs
SET external_blocks = MAX(0, external_blocks + ?), updated_at = CURRENT_TIMESTAMP
WHERE id = ?`,
		delta,
		id,
	)
	if err != nil {
		return fmt.Errorf("workflowRunRepo.UpdateBlocks: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("workflowRunRepo.UpdateBlocks: %w", err)
	}

	return nil
}

func scanWorkflowRun(scanner interface{ Scan(dest ...any) error }) (*WorkflowRun, error) {
	item := &WorkflowRun{}
	var resumeNodeID sql.NullString
	var lastNodeID sql.NullString
	var sourceDir sql.NullString
	var errMsg sql.NullString
	var startedAt any
	var finishedAt any
	var createdAt any
	var updatedAt any

	err := scanner.Scan(
		&item.ID,
		&item.JobID,
		&item.FolderID,
		&sourceDir,
		&item.WorkflowDefID,
		&item.Status,
		&resumeNodeID,
		&lastNodeID,
		&item.ExternalBlocks,
		&errMsg,
		&startedAt,
		&finishedAt,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if sourceDir.Valid {
		item.SourceDir = sourceDir.String
	}
	if resumeNodeID.Valid {
		item.ResumeNodeID = resumeNodeID.String
	}
	if lastNodeID.Valid {
		item.LastNodeID = lastNodeID.String
	}
	if errMsg.Valid {
		item.Error = errMsg.String
	}
	item.StartedAt, err = parseNullableTime(startedAt)
	if err != nil {
		return nil, fmt.Errorf("scanWorkflowRun parse started_at: %w", err)
	}
	item.FinishedAt, err = parseNullableTime(finishedAt)
	if err != nil {
		return nil, fmt.Errorf("scanWorkflowRun parse finished_at: %w", err)
	}
	item.CreatedAt, err = parseDBTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("scanWorkflowRun parse created_at: %w", err)
	}
	item.UpdatedAt, err = parseDBTime(updatedAt)
	if err != nil {
		return nil, fmt.Errorf("scanWorkflowRun parse updated_at: %w", err)
	}

	return item, nil
}
