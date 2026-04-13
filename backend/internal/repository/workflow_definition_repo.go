package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type SQLiteWorkflowDefinitionRepository struct {
	db *sql.DB
}

func NewWorkflowDefinitionRepository(db *sql.DB) WorkflowDefinitionRepository {
	return &SQLiteWorkflowDefinitionRepository{db: db}
}

func (r *SQLiteWorkflowDefinitionRepository) Create(ctx context.Context, item *WorkflowDefinition) error {
	_, err := r.db.ExecContext(
		ctx,
		`INSERT INTO workflow_definitions (
	id, name, description, graph_json, is_active, version, created_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)`,
		item.ID,
		item.Name,
		item.Description,
		item.GraphJSON,
		boolToInt(item.IsActive),
		item.Version,
	)
	if err != nil {
		return fmt.Errorf("workflowDefinitionRepo.Create: %w", err)
	}

	return nil
}

func (r *SQLiteWorkflowDefinitionRepository) GetByID(ctx context.Context, id string) (*WorkflowDefinition, error) {
	item, err := scanWorkflowDefinition(r.db.QueryRowContext(ctx, `
SELECT id, name, description, graph_json, is_active, version, created_at, updated_at
FROM workflow_definitions
WHERE id = ?`, id))
	if err != nil {
		return nil, fmt.Errorf("workflowDefinitionRepo.GetByID: %w", err)
	}

	return item, nil
}

func (r *SQLiteWorkflowDefinitionRepository) List(ctx context.Context, filter WorkflowDefListFilter) ([]*WorkflowDefinition, int, error) {
	var total int
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM workflow_definitions").Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("workflowDefinitionRepo.List count: %w", err)
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

	rows, err := r.db.QueryContext(ctx, `
SELECT id, name, description, graph_json, is_active, version, created_at, updated_at
FROM workflow_definitions
ORDER BY created_at DESC
LIMIT ? OFFSET ?`, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("workflowDefinitionRepo.List query: %w", err)
	}
	defer rows.Close()

	items := make([]*WorkflowDefinition, 0)
	for rows.Next() {
		item, scanErr := scanWorkflowDefinition(rows)
		if scanErr != nil {
			return nil, 0, fmt.Errorf("workflowDefinitionRepo.List scan: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("workflowDefinitionRepo.List rows: %w", err)
	}

	return items, total, nil
}

func (r *SQLiteWorkflowDefinitionRepository) Update(ctx context.Context, item *WorkflowDefinition) error {
	res, err := r.db.ExecContext(
		ctx,
		`UPDATE workflow_definitions
SET name = ?, description = ?, graph_json = ?, is_active = ?, version = ?, updated_at = CURRENT_TIMESTAMP
WHERE id = ?`,
		item.Name,
		item.Description,
		item.GraphJSON,
		boolToInt(item.IsActive),
		item.Version,
		item.ID,
	)
	if err != nil {
		return fmt.Errorf("workflowDefinitionRepo.Update: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("workflowDefinitionRepo.Update: %w", err)
	}

	return nil
}

func (r *SQLiteWorkflowDefinitionRepository) Delete(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(ctx, "DELETE FROM workflow_definitions WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("workflowDefinitionRepo.Delete: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("workflowDefinitionRepo.Delete: %w", err)
	}

	return nil
}

func scanWorkflowDefinition(scanner interface{ Scan(dest ...any) error }) (*WorkflowDefinition, error) {
	item := &WorkflowDefinition{}
	var activeInt int
	var createdAt any
	var updatedAt any

	err := scanner.Scan(
		&item.ID,
		&item.Name,
		&item.Description,
		&item.GraphJSON,
		&activeInt,
		&item.Version,
		&createdAt,
		&updatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	item.IsActive = intToBool(activeInt)
	item.CreatedAt, err = parseDBTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("scanWorkflowDefinition parse created_at: %w", err)
	}
	item.UpdatedAt, err = parseDBTime(updatedAt)
	if err != nil {
		return nil, fmt.Errorf("scanWorkflowDefinition parse updated_at: %w", err)
	}

	return item, nil
}
