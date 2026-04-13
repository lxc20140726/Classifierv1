package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

type SQLiteScheduledWorkflowRepository struct {
	db *sql.DB
}

func NewScheduledWorkflowRepository(db *sql.DB) ScheduledWorkflowRepository {
	return &SQLiteScheduledWorkflowRepository{db: db}
}

func (r *SQLiteScheduledWorkflowRepository) Create(ctx context.Context, item *ScheduledWorkflow) error {
	_, err := r.db.ExecContext(
		ctx,
		`INSERT INTO scheduled_workflows (
			id, name, job_type, workflow_def_id, folder_ids, source_dirs, cron_spec, enabled, last_run_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		item.ID,
		item.Name,
		item.JobType,
		item.WorkflowDefID,
		item.FolderIDs,
		item.SourceDirs,
		item.CronSpec,
		boolToInt(item.Enabled),
		nullableTime(item.LastRunAt),
	)
	if err != nil {
		return fmt.Errorf("scheduledWorkflowRepo.Create: %w", err)
	}

	return nil
}

func (r *SQLiteScheduledWorkflowRepository) GetByID(ctx context.Context, id string) (*ScheduledWorkflow, error) {
	item, err := scanScheduledWorkflow(r.db.QueryRowContext(ctx, `
SELECT id, name, job_type, workflow_def_id, folder_ids, source_dirs, cron_spec, enabled, last_run_at, created_at, updated_at
FROM scheduled_workflows
WHERE id = ?`, id))
	if err != nil {
		return nil, fmt.Errorf("scheduledWorkflowRepo.GetByID: %w", err)
	}

	return item, nil
}

func (r *SQLiteScheduledWorkflowRepository) List(ctx context.Context, filter ScheduledWorkflowListFilter) ([]*ScheduledWorkflow, int, error) {
	var total int
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM scheduled_workflows").Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("scheduledWorkflowRepo.List count: %w", err)
	}

	page := filter.Page
	if page <= 0 {
		page = 1
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	offset := (page - 1) * limit

	rows, err := r.db.QueryContext(ctx, `
SELECT id, name, job_type, workflow_def_id, folder_ids, source_dirs, cron_spec, enabled, last_run_at, created_at, updated_at
FROM scheduled_workflows
ORDER BY created_at DESC
LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("scheduledWorkflowRepo.List query: %w", err)
	}
	defer rows.Close()

	items := make([]*ScheduledWorkflow, 0)
	for rows.Next() {
		item, scanErr := scanScheduledWorkflow(rows)
		if scanErr != nil {
			return nil, 0, fmt.Errorf("scheduledWorkflowRepo.List scan: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("scheduledWorkflowRepo.List rows: %w", err)
	}

	return items, total, nil
}

func (r *SQLiteScheduledWorkflowRepository) ListEnabled(ctx context.Context) ([]*ScheduledWorkflow, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT id, name, job_type, workflow_def_id, folder_ids, source_dirs, cron_spec, enabled, last_run_at, created_at, updated_at
FROM scheduled_workflows
WHERE enabled = 1
ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("scheduledWorkflowRepo.ListEnabled query: %w", err)
	}
	defer rows.Close()

	items := make([]*ScheduledWorkflow, 0)
	for rows.Next() {
		item, scanErr := scanScheduledWorkflow(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("scheduledWorkflowRepo.ListEnabled scan: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("scheduledWorkflowRepo.ListEnabled rows: %w", err)
	}

	return items, nil
}

func (r *SQLiteScheduledWorkflowRepository) Update(ctx context.Context, item *ScheduledWorkflow) error {
	res, err := r.db.ExecContext(
		ctx,
		`UPDATE scheduled_workflows
		SET name = ?, job_type = ?, workflow_def_id = ?, folder_ids = ?, source_dirs = ?, cron_spec = ?, enabled = ?, updated_at = CURRENT_TIMESTAMP
		WHERE id = ?`,
		item.Name,
		item.JobType,
		item.WorkflowDefID,
		item.FolderIDs,
		item.SourceDirs,
		item.CronSpec,
		boolToInt(item.Enabled),
		item.ID,
	)
	if err != nil {
		return fmt.Errorf("scheduledWorkflowRepo.Update: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("scheduledWorkflowRepo.Update: %w", err)
	}

	return nil
}

func (r *SQLiteScheduledWorkflowRepository) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, "DELETE FROM scheduled_workflows WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("scheduledWorkflowRepo.Delete: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("scheduledWorkflowRepo.Delete: %w", err)
	}

	return nil
}

func (r *SQLiteScheduledWorkflowRepository) UpdateLastRunAt(ctx context.Context, id string, at time.Time) error {
	res, err := r.db.ExecContext(ctx, `
UPDATE scheduled_workflows
SET last_run_at = ?, updated_at = CURRENT_TIMESTAMP
WHERE id = ?`, at.UTC(), id)
	if err != nil {
		return fmt.Errorf("scheduledWorkflowRepo.UpdateLastRunAt: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("scheduledWorkflowRepo.UpdateLastRunAt: %w", err)
	}

	return nil
}

func scanScheduledWorkflow(scanner interface{ Scan(dest ...any) error }) (*ScheduledWorkflow, error) {
	item := &ScheduledWorkflow{}
	var enabledInt int
	var lastRunAt any
	var createdAt any
	var updatedAt any

	err := scanner.Scan(
		&item.ID,
		&item.Name,
		&item.JobType,
		&item.WorkflowDefID,
		&item.FolderIDs,
		&item.SourceDirs,
		&item.CronSpec,
		&enabledInt,
		&lastRunAt,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	item.Enabled = intToBool(enabledInt)
	item.LastRunAt, err = parseNullableTime(lastRunAt)
	if err != nil {
		return nil, fmt.Errorf("scanScheduledWorkflow parse last_run_at: %w", err)
	}
	item.CreatedAt, err = parseDBTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("scanScheduledWorkflow parse created_at: %w", err)
	}
	item.UpdatedAt, err = parseDBTime(updatedAt)
	if err != nil {
		return nil, fmt.Errorf("scanScheduledWorkflow parse updated_at: %w", err)
	}

	return item, nil
}
