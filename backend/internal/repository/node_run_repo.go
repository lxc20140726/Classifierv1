package repository

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type SQLiteNodeRunRepository struct {
	db *sql.DB
}

func NewNodeRunRepository(db *sql.DB) NodeRunRepository {
	return &SQLiteNodeRunRepository{db: db}
}

func (r *SQLiteNodeRunRepository) Create(ctx context.Context, item *NodeRun) error {
	_, err := r.db.ExecContext(
		ctx,
		`INSERT INTO node_runs (
	id, workflow_run_id, node_id, node_type, sequence, status, input_json, output_json, input_signature, resume_token, resume_data, error,
	progress_percent, progress_done, progress_total, progress_stage, progress_message, progress_source_path, progress_target_path, progress_updated_at,
	started_at, finished_at, created_at
 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)`,
		item.ID,
		item.WorkflowRunID,
		item.NodeID,
		item.NodeType,
		item.Sequence,
		item.Status,
		nullableString(item.InputJSON),
		nullableString(item.OutputJSON),
		nullableString(item.InputSignature),
		nullableString(item.ResumeToken),
		item.ResumeData,
		nullableString(item.Error),
		item.ProgressPercent,
		item.ProgressDone,
		item.ProgressTotal,
		nullableString(ptrString(item.ProgressStage)),
		nullableString(ptrString(item.ProgressMessage)),
		nullableString(ptrString(item.ProgressSourcePath)),
		nullableString(ptrString(item.ProgressTargetPath)),
		nullableTime(item.ProgressUpdatedAt),
		nullableTime(item.StartedAt),
		nullableTime(item.FinishedAt),
	)
	if err != nil {
		return fmt.Errorf("nodeRunRepo.Create: %w", err)
	}

	return nil
}

func (r *SQLiteNodeRunRepository) GetByID(ctx context.Context, id string) (*NodeRun, error) {
	item, err := scanNodeRun(r.db.QueryRowContext(ctx, `
SELECT id, workflow_run_id, node_id, node_type, sequence, status, input_json, output_json, input_signature, resume_token, resume_data, error,
       progress_percent, progress_done, progress_total, progress_stage, progress_message, progress_source_path, progress_target_path, progress_updated_at,
       started_at, finished_at, created_at
FROM node_runs
WHERE id = ?`, id))
	if err != nil {
		return nil, fmt.Errorf("nodeRunRepo.GetByID: %w", err)
	}

	return item, nil
}

func (r *SQLiteNodeRunRepository) List(ctx context.Context, filter NodeRunListFilter) ([]*NodeRun, int, error) {
	if filter.WorkflowRunID == "" {
		return []*NodeRun{}, 0, nil
	}

	var total int
	if err := r.db.QueryRowContext(
		ctx,
		"SELECT COUNT(*) FROM node_runs WHERE workflow_run_id = ?",
		filter.WorkflowRunID,
	).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("nodeRunRepo.List count: %w", err)
	}

	page := filter.Page
	if page <= 0 {
		page = 1
	}
	limit := filter.Limit
	if limit <= 0 {
		limit = 200
	}
	offset := (page - 1) * limit

	rows, err := r.db.QueryContext(ctx, `
SELECT id, workflow_run_id, node_id, node_type, sequence, status, input_json, output_json, input_signature, resume_token, resume_data, error,
       progress_percent, progress_done, progress_total, progress_stage, progress_message, progress_source_path, progress_target_path, progress_updated_at,
       started_at, finished_at, created_at
FROM node_runs
WHERE workflow_run_id = ?
ORDER BY sequence ASC, created_at ASC
LIMIT ? OFFSET ?`, filter.WorkflowRunID, limit, offset)
	if err != nil {
		return nil, 0, fmt.Errorf("nodeRunRepo.List query: %w", err)
	}
	defer rows.Close()

	items := make([]*NodeRun, 0)
	for rows.Next() {
		item, scanErr := scanNodeRun(rows)
		if scanErr != nil {
			return nil, 0, fmt.Errorf("nodeRunRepo.List scan: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("nodeRunRepo.List rows: %w", err)
	}

	return items, total, nil
}

func (r *SQLiteNodeRunRepository) GetLatestByNodeID(ctx context.Context, workflowRunID, nodeID string) (*NodeRun, error) {
	item, err := scanNodeRun(r.db.QueryRowContext(ctx, `
SELECT id, workflow_run_id, node_id, node_type, sequence, status, input_json, output_json, input_signature, resume_token, resume_data, error,
       progress_percent, progress_done, progress_total, progress_stage, progress_message, progress_source_path, progress_target_path, progress_updated_at,
       started_at, finished_at, created_at
FROM node_runs
WHERE workflow_run_id = ? AND node_id = ?
ORDER BY sequence DESC, created_at DESC
LIMIT 1`, workflowRunID, nodeID))
	if err != nil {
		return nil, fmt.Errorf("nodeRunRepo.GetLatestByNodeID: %w", err)
	}

	return item, nil
}

func (r *SQLiteNodeRunRepository) UpdateStart(ctx context.Context, id, inputJSON string) error {
	res, err := r.db.ExecContext(
		ctx,
		`UPDATE node_runs
SET status = 'running', input_json = ?, started_at = COALESCE(started_at, CURRENT_TIMESTAMP),
    progress_percent = NULL, progress_done = NULL, progress_total = NULL, progress_stage = NULL, progress_message = NULL,
    progress_source_path = NULL, progress_target_path = NULL, progress_updated_at = NULL
WHERE id = ?`,
		nullableString(inputJSON),
		id,
	)
	if err != nil {
		return fmt.Errorf("nodeRunRepo.UpdateStart: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("nodeRunRepo.UpdateStart: %w", err)
	}

	return nil
}

func (r *SQLiteNodeRunRepository) UpdateProgress(ctx context.Context, id string, progress NodeRunProgress) error {
	res, err := r.db.ExecContext(
		ctx,
		`UPDATE node_runs
SET progress_percent = ?, progress_done = ?, progress_total = ?, progress_stage = ?, progress_message = ?,
    progress_source_path = ?, progress_target_path = ?, progress_updated_at = CURRENT_TIMESTAMP
WHERE id = ?`,
		progress.Percent,
		progress.Done,
		progress.Total,
		nullableString(progress.Stage),
		nullableString(progress.Message),
		nullableString(progress.SourcePath),
		nullableString(progress.TargetPath),
		id,
	)
	if err != nil {
		return fmt.Errorf("nodeRunRepo.UpdateProgress: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("nodeRunRepo.UpdateProgress: %w", err)
	}

	return nil
}

func (r *SQLiteNodeRunRepository) UpdateFinish(ctx context.Context, id, status, outputJSON, errMsg string) error {
	res, err := r.db.ExecContext(
		ctx,
		`UPDATE node_runs
SET status = ?, output_json = ?, error = ?, finished_at = CURRENT_TIMESTAMP
WHERE id = ?`,
		status,
		nullableString(outputJSON),
		nullableString(errMsg),
		id,
	)
	if err != nil {
		return fmt.Errorf("nodeRunRepo.UpdateFinish: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("nodeRunRepo.UpdateFinish: %w", err)
	}

	return nil
}

func (r *SQLiteNodeRunRepository) UpdateResumeData(ctx context.Context, nodeRunID, resumeData string) error {
	res, err := r.db.ExecContext(
		ctx,
		`UPDATE node_runs
SET resume_data = ?
WHERE id = ?`,
		resumeData,
		nodeRunID,
	)
	if err != nil {
		return fmt.Errorf("nodeRunRepo.UpdateResumeData: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("nodeRunRepo.UpdateResumeData: %w", err)
	}

	return nil
}

func (r *SQLiteNodeRunRepository) GetWaitingInputByWorkflowRunID(ctx context.Context, workflowRunID string) (*NodeRun, error) {
	item, err := scanNodeRun(r.db.QueryRowContext(ctx, `
SELECT id, workflow_run_id, node_id, node_type, sequence, status, input_json, output_json, input_signature, resume_token, resume_data, error,
       progress_percent, progress_done, progress_total, progress_stage, progress_message, progress_source_path, progress_target_path, progress_updated_at,
       started_at, finished_at, created_at
FROM node_runs
WHERE workflow_run_id = ? AND status = 'waiting_input'
ORDER BY sequence DESC, created_at DESC
LIMIT 1`, workflowRunID))
	if err != nil {
		return nil, fmt.Errorf("nodeRunRepo.GetWaitingInputByWorkflowRunID: %w", err)
	}

	return item, nil
}

func (r *SQLiteNodeRunRepository) SetResumeToken(ctx context.Context, nodeRunID, token string) error {
	res, err := r.db.ExecContext(
		ctx,
		`UPDATE node_runs
SET resume_token = ?
WHERE id = ?`,
		nullableString(token),
		nodeRunID,
	)
	if err != nil {
		return fmt.Errorf("nodeRunRepo.SetResumeToken: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("nodeRunRepo.SetResumeToken: %w", err)
	}

	return nil
}

func scanNodeRun(scanner interface{ Scan(dest ...any) error }) (*NodeRun, error) {
	item := &NodeRun{}
	var inputJSON sql.NullString
	var outputJSON sql.NullString
	var inputSignature sql.NullString
	var resumeToken sql.NullString
	var resumeData sql.NullString
	var errMsg sql.NullString
	var progressPercent sql.NullInt64
	var progressDone sql.NullInt64
	var progressTotal sql.NullInt64
	var progressStage sql.NullString
	var progressMessage sql.NullString
	var progressSourcePath sql.NullString
	var progressTargetPath sql.NullString
	var progressUpdatedAt any
	var startedAt any
	var finishedAt any
	var createdAt any

	err := scanner.Scan(
		&item.ID,
		&item.WorkflowRunID,
		&item.NodeID,
		&item.NodeType,
		&item.Sequence,
		&item.Status,
		&inputJSON,
		&outputJSON,
		&inputSignature,
		&resumeToken,
		&resumeData,
		&errMsg,
		&progressPercent,
		&progressDone,
		&progressTotal,
		&progressStage,
		&progressMessage,
		&progressSourcePath,
		&progressTargetPath,
		&progressUpdatedAt,
		&startedAt,
		&finishedAt,
		&createdAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if inputJSON.Valid {
		item.InputJSON = inputJSON.String
	}
	if outputJSON.Valid {
		item.OutputJSON = outputJSON.String
	}
	if inputSignature.Valid {
		item.InputSignature = inputSignature.String
	}
	if resumeToken.Valid {
		item.ResumeToken = resumeToken.String
	}
	if resumeData.Valid {
		item.ResumeData = resumeData.String
	}
	if errMsg.Valid {
		item.Error = errMsg.String
	}
	if progressPercent.Valid {
		value := int(progressPercent.Int64)
		item.ProgressPercent = &value
	}
	if progressDone.Valid {
		value := int(progressDone.Int64)
		item.ProgressDone = &value
	}
	if progressTotal.Valid {
		value := int(progressTotal.Int64)
		item.ProgressTotal = &value
	}
	if progressStage.Valid {
		value := progressStage.String
		item.ProgressStage = &value
	}
	if progressMessage.Valid {
		value := progressMessage.String
		item.ProgressMessage = &value
	}
	if progressSourcePath.Valid {
		value := progressSourcePath.String
		item.ProgressSourcePath = &value
	}
	if progressTargetPath.Valid {
		value := progressTargetPath.String
		item.ProgressTargetPath = &value
	}
	item.ProgressUpdatedAt, err = parseNullableTime(progressUpdatedAt)
	if err != nil {
		return nil, fmt.Errorf("scanNodeRun parse progress_updated_at: %w", err)
	}
	item.StartedAt, err = parseNullableTime(startedAt)
	if err != nil {
		return nil, fmt.Errorf("scanNodeRun parse started_at: %w", err)
	}
	item.FinishedAt, err = parseNullableTime(finishedAt)
	if err != nil {
		return nil, fmt.Errorf("scanNodeRun parse finished_at: %w", err)
	}
	item.CreatedAt, err = parseDBTime(createdAt)
	if err != nil {
		return nil, fmt.Errorf("scanNodeRun parse created_at: %w", err)
	}

	return item, nil
}

func ptrString(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
