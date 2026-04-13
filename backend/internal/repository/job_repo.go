package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type SQLiteJobRepository struct {
	db *sql.DB
}

func NewJobRepository(db *sql.DB) JobRepository {
	return &SQLiteJobRepository{db: db}
}

func (r *SQLiteJobRepository) Create(ctx context.Context, job *Job) error {
	_, err := r.db.ExecContext(
		ctx,
		`INSERT INTO jobs (
			id, type, workflow_def_id, source_dir, status, folder_ids, total, done, failed, error, started_at, finished_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		job.ID,
		job.Type,
		nullableString(job.WorkflowDefID),
		job.SourceDir,
		job.Status,
		job.FolderIDs,
		job.Total,
		job.Done,
		job.Failed,
		nullableString(job.Error),
		nullableTime(job.StartedAt),
		nullableTime(job.FinishedAt),
	)
	if err != nil {
		return fmt.Errorf("jobRepo.Create: %w", err)
	}

	return nil
}

func (r *SQLiteJobRepository) GetByID(ctx context.Context, id string) (*Job, error) {
	job, err := scanJob(r.db.QueryRowContext(ctx, `
SELECT id, type, workflow_def_id, source_dir, status, folder_ids, total, done, failed, error, started_at, finished_at, created_at, updated_at
FROM jobs
WHERE id = ?`, id))
	if err != nil {
		return nil, fmt.Errorf("jobRepo.GetByID: %w", err)
	}

	return job, nil
}

func (r *SQLiteJobRepository) List(ctx context.Context, filter JobListFilter) ([]*Job, int, error) {
	where := make([]string, 0, 1)
	args := make([]any, 0, 1)

	if filter.Status != "" {
		where = append(where, "status = ?")
		args = append(args, filter.Status)
	}

	whereClause := ""
	if len(where) > 0 {
		whereClause = " WHERE " + strings.Join(where, " AND ")
	}

	var total int
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM jobs"+whereClause, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("jobRepo.List count: %w", err)
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
SELECT id, type, workflow_def_id, source_dir, status, folder_ids, total, done, failed, error, started_at, finished_at, created_at, updated_at
FROM jobs`+whereClause+`
ORDER BY created_at DESC
LIMIT ? OFFSET ?`, listArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("jobRepo.List query: %w", err)
	}
	defer rows.Close()

	jobs := make([]*Job, 0)
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("jobRepo.List scan: %w", err)
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("jobRepo.List rows: %w", err)
	}

	return jobs, total, nil
}

func (r *SQLiteJobRepository) UpdateTotal(ctx context.Context, id string, total int) error {
	res, err := r.db.ExecContext(
		ctx,
		`UPDATE jobs
SET total = ?, updated_at = CURRENT_TIMESTAMP
WHERE id = ?`,
		total,
		id,
	)
	if err != nil {
		return fmt.Errorf("jobRepo.UpdateTotal: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("jobRepo.UpdateTotal: %w", err)
	}

	return nil
}

func (r *SQLiteJobRepository) UpdateStatus(ctx context.Context, id, status, errMsg string) error {
	query := `UPDATE jobs SET status = ?, error = ?, updated_at = CURRENT_TIMESTAMP`
	args := []any{status, nullableString(errMsg)}

	if status == "running" {
		query += ", started_at = COALESCE(started_at, CURRENT_TIMESTAMP)"
	}
	if status == "succeeded" || status == "failed" || status == "cancelled" || status == "partial" || status == "rolled_back" {
		query += ", finished_at = CURRENT_TIMESTAMP"
	}

	query += " WHERE id = ?"
	args = append(args, id)

	res, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("jobRepo.UpdateStatus: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("jobRepo.UpdateStatus: %w", err)
	}

	return nil
}

func (r *SQLiteJobRepository) IncrementProgress(ctx context.Context, id string, successDelta, failedDelta int) error {
	res, err := r.db.ExecContext(ctx, `
UPDATE jobs
SET done = done + ?,
	failed = failed + ?,
	updated_at = CURRENT_TIMESTAMP
WHERE id = ?`, successDelta, failedDelta, id)
	if err != nil {
		return fmt.Errorf("jobRepo.IncrementProgress: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("jobRepo.IncrementProgress: %w", err)
	}

	return nil
}

func scanJob(scanner interface{ Scan(dest ...any) error }) (*Job, error) {
	job := &Job{}
	var workflowDefID sql.NullString
	var sourceDir sql.NullString
	var errMsg sql.NullString
	var startedAt any
	var finishedAt any
	var createdAt any
	var updatedAt any

	err := scanner.Scan(
		&job.ID,
		&job.Type,
		&workflowDefID,
		&sourceDir,
		&job.Status,
		&job.FolderIDs,
		&job.Total,
		&job.Done,
		&job.Failed,
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

	if errMsg.Valid {
		job.Error = errMsg.String
	}
	if workflowDefID.Valid {
		job.WorkflowDefID = workflowDefID.String
	}
	if sourceDir.Valid {
		job.SourceDir = sourceDir.String
	}

	if job.StartedAt, err = parseNullableTime(startedAt); err != nil {
		return nil, fmt.Errorf("scanJob parse started_at: %w", err)
	}
	if job.FinishedAt, err = parseNullableTime(finishedAt); err != nil {
		return nil, fmt.Errorf("scanJob parse finished_at: %w", err)
	}
	if job.CreatedAt, err = parseDBTime(createdAt); err != nil {
		return nil, fmt.Errorf("scanJob parse created_at: %w", err)
	}
	if job.UpdatedAt, err = parseDBTime(updatedAt); err != nil {
		return nil, fmt.Errorf("scanJob parse updated_at: %w", err)
	}

	return job, nil
}
