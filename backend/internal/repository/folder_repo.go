package repository

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

type SQLiteFolderRepository struct {
	db *sql.DB
}

const (
	workflowStageStatusNotRun       = "not_run"
	workflowStageStatusRunning      = "running"
	workflowStageStatusSucceeded    = "succeeded"
	workflowStageStatusFailed       = "failed"
	workflowStageStatusWaitingInput = "waiting_input"
	workflowStageStatusPartial      = "partial"
	workflowStageStatusRolledBack   = "rolled_back"
	workflowStageStatusCancelled    = "cancelled"
	folderObservationSelectColumns  = "id, folder_id, path, source_dir, relative_path, is_current, first_seen_at, last_seen_at"
	folderSelectColumns             = `id, path, source_dir, relative_path, identity_fingerprint, name, category, category_source, status,
	image_count, video_count, other_file_count, has_other_files, total_files, total_size, marked_for_move,
	deleted_at, delete_staging_path, cover_image_path, output_check_summary, scanned_at, updated_at`
	folderSelectColumnsWithAliasF = `f.id, f.path, f.source_dir, f.relative_path, f.identity_fingerprint, f.name, f.category, f.category_source, f.status,
	f.image_count, f.video_count, f.other_file_count, f.has_other_files, f.total_files, f.total_size, f.marked_for_move,
	f.deleted_at, f.delete_staging_path, f.cover_image_path, f.output_check_summary, f.scanned_at, f.updated_at`
	folderSortByUpdatedAt = "updated_at"
	folderSortByTotalSize = "total_size"
	folderSortOrderAsc    = "asc"
	folderSortOrderDesc   = "desc"
)

func NewFolderRepository(db *sql.DB) FolderRepository {
	return &SQLiteFolderRepository{db: db}
}

func normalizeFolderPathForCompare(raw string) string {
	return strings.ReplaceAll(trimFolderPathComparePrefix(raw), "\\", "/")
}

func trimFolderPathComparePrefix(raw string) string {
	trimmed := strings.TrimSpace(raw)
	for len(trimmed) > 1 {
		if strings.HasSuffix(trimmed, "/") && !isFolderPathRoot(trimmed) {
			trimmed = strings.TrimSuffix(trimmed, "/")
			continue
		}
		if strings.HasSuffix(trimmed, `\`) && !isFolderPathRoot(trimmed) {
			trimmed = strings.TrimSuffix(trimmed, `\`)
			continue
		}
		break
	}
	return trimmed
}

func isFolderPathRoot(path string) bool {
	if path == "/" || path == `\` {
		return true
	}
	if len(path) == 3 && path[1] == ':' && (path[2] == '/' || path[2] == '\\') {
		return true
	}
	return false
}

func folderPathChildLike(prefix, separator string) string {
	if strings.HasSuffix(prefix, "/") || strings.HasSuffix(prefix, `\`) {
		return prefix + "%"
	}
	return prefix + separator + "%"
}

func (r *SQLiteFolderRepository) Upsert(ctx context.Context, f *Folder) error {
	if f == nil {
		return fmt.Errorf("folderRepo.Upsert: folder is required")
	}

	now := f.ScannedAt.UTC()
	if now.IsZero() {
		now = time.Now().UTC()
	}

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("folderRepo.Upsert begin tx: %w", err)
	}
	defer rollbackTx(tx)

	query := `
INSERT INTO folders (
	id, path, source_dir, relative_path, identity_fingerprint, name, category, category_source, status,
	image_count, video_count, other_file_count, has_other_files, total_files, total_size, marked_for_move,
	deleted_at, delete_staging_path, cover_image_path, output_check_summary, scanned_at, updated_at
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
ON CONFLICT(id) DO UPDATE SET
	path = excluded.path,
	source_dir = excluded.source_dir,
	relative_path = excluded.relative_path,
	identity_fingerprint = excluded.identity_fingerprint,
	name = excluded.name,
	category = excluded.category,
	category_source = excluded.category_source,
	status = excluded.status,
	image_count = excluded.image_count,
	video_count = excluded.video_count,
	other_file_count = excluded.other_file_count,
	has_other_files = excluded.has_other_files,
	total_files = excluded.total_files,
	total_size = excluded.total_size,
	marked_for_move = excluded.marked_for_move,
	deleted_at = excluded.deleted_at,
	delete_staging_path = excluded.delete_staging_path,
	cover_image_path = excluded.cover_image_path,
	output_check_summary = COALESCE(NULLIF(excluded.output_check_summary, ''), folders.output_check_summary),
	scanned_at = excluded.scanned_at,
	updated_at = CURRENT_TIMESTAMP
`

	if _, err := tx.ExecContext(
		ctx,
		query,
		f.ID,
		f.Path,
		f.SourceDir,
		f.RelativePath,
		strings.TrimSpace(f.IdentityFingerprint),
		f.Name,
		f.Category,
		f.CategorySource,
		f.Status,
		f.ImageCount,
		f.VideoCount,
		f.OtherFileCount,
		boolToInt(f.HasOtherFiles),
		f.TotalFiles,
		f.TotalSize,
		boolToInt(f.MarkedForMove),
		nullableTime(f.DeletedAt),
		nullableString(f.DeleteStagingPath),
		strings.TrimSpace(f.CoverImagePath),
		marshalFolderOutputCheckSummary(f.OutputCheckSummary),
		now.Format("2006-01-02 15:04:05"),
	); err != nil {
		return fmt.Errorf("folderRepo.Upsert: %w", err)
	}

	if strings.TrimSpace(f.Path) != "" {
		if err := r.recordObservationTx(ctx, tx, f.ID, f.Path, f.SourceDir, f.RelativePath, now); err != nil {
			return fmt.Errorf("folderRepo.Upsert record observation: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("folderRepo.Upsert commit: %w", err)
	}

	return nil
}

func (r *SQLiteFolderRepository) GetByID(ctx context.Context, id string) (*Folder, error) {
	folder, err := scanFolder(
		r.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT %s
FROM folders
WHERE id = ?
`, folderSelectColumns), id),
	)
	if err != nil {
		return nil, fmt.Errorf("folderRepo.GetByID: %w", err)
	}

	return folder, nil
}

func (r *SQLiteFolderRepository) GetByPath(ctx context.Context, path string) (*Folder, error) {
	return r.GetCurrentByPath(ctx, path)
}

func (r *SQLiteFolderRepository) GetCurrentByPath(ctx context.Context, path string) (*Folder, error) {
	trimmedPath := trimFolderPathComparePrefix(path)
	normalizedPath := normalizeFolderPathForCompare(path)
	folder, err := scanFolder(
		r.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT %s
FROM folders
WHERE deleted_at IS NULL AND (path = ? OR REPLACE(path, char(92), '/') = ?)
ORDER BY CASE WHEN path = ? THEN 0 ELSE 1 END, updated_at DESC, scanned_at DESC, id ASC
LIMIT 1
`, folderSelectColumns), trimmedPath, normalizedPath, trimmedPath),
	)
	if err != nil {
		return nil, fmt.Errorf("folderRepo.GetCurrentByPath: %w", err)
	}

	return folder, nil
}

func (r *SQLiteFolderRepository) GetByHistoricalPath(ctx context.Context, path string) (*Folder, error) {
	trimmedPath := trimFolderPathComparePrefix(path)
	normalizedPath := normalizeFolderPathForCompare(path)
	folder, err := scanFolder(
		r.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT %s
FROM folders f
JOIN folder_path_observations o ON o.folder_id = f.id
WHERE f.deleted_at IS NULL AND (o.path = ? OR REPLACE(o.path, char(92), '/') = ?)
ORDER BY CASE WHEN o.path = ? THEN 0 ELSE 1 END, o.last_seen_at DESC, o.first_seen_at DESC
LIMIT 1
`, folderSelectColumnsWithAliasF), trimmedPath, normalizedPath, trimmedPath),
	)
	if err != nil {
		return nil, fmt.Errorf("folderRepo.GetByHistoricalPath: %w", err)
	}

	return folder, nil
}

func (r *SQLiteFolderRepository) GetCurrentBySourceAndRelativePath(ctx context.Context, sourceDir, relativePath string) (*Folder, error) {
	folder, err := scanFolder(
		r.db.QueryRowContext(ctx, fmt.Sprintf(`
SELECT %s
FROM folders f
JOIN folder_path_observations o ON o.folder_id = f.id
WHERE o.source_dir = ? AND o.relative_path = ? AND o.is_current = 1 AND f.deleted_at IS NULL
LIMIT 1
`, folderSelectColumnsWithAliasF), sourceDir, relativePath),
	)
	if err != nil {
		return nil, fmt.Errorf("folderRepo.GetCurrentBySourceAndRelativePath: %w", err)
	}

	return folder, nil
}

func (r *SQLiteFolderRepository) ResolveScanTarget(ctx context.Context, path, sourceDir, relativePath string) (*Folder, FolderScanMatchType, error) {
	if folder, err := r.GetCurrentByPath(ctx, path); err == nil {
		return folder, FolderScanMatchTypeCurrentPathMatch, nil
	} else if !errors.Is(err, ErrNotFound) {
		return nil, "", fmt.Errorf("folderRepo.ResolveScanTarget current path: %w", err)
	}

	if sourceDir != "" || relativePath != "" {
		if folder, err := r.GetCurrentBySourceAndRelativePath(ctx, sourceDir, relativePath); err == nil {
			return folder, FolderScanMatchTypeSourceRelativeMatch, nil
		} else if !errors.Is(err, ErrNotFound) {
			return nil, "", fmt.Errorf("folderRepo.ResolveScanTarget source+relative: %w", err)
		}
	}

	if folder, err := r.GetByHistoricalPath(ctx, path); err == nil {
		return folder, FolderScanMatchTypeHistoricalPathMatch, nil
	} else if !errors.Is(err, ErrNotFound) {
		return nil, "", fmt.Errorf("folderRepo.ResolveScanTarget historical path: %w", err)
	}

	return nil, FolderScanMatchTypeNewDiscovery, nil
}

func (r *SQLiteFolderRepository) RecordObservation(ctx context.Context, folderID, path, sourceDir, relativePath string, observedAt time.Time) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("folderRepo.RecordObservation begin tx: %w", err)
	}
	defer rollbackTx(tx)

	if err := r.recordObservationTx(ctx, tx, folderID, path, sourceDir, relativePath, observedAt); err != nil {
		return fmt.Errorf("folderRepo.RecordObservation: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("folderRepo.RecordObservation commit: %w", err)
	}

	return nil
}

func (r *SQLiteFolderRepository) ListPathObservationsByFolderID(ctx context.Context, folderID string) ([]*FolderPathObservation, error) {
	rows, err := r.db.QueryContext(
		ctx,
		fmt.Sprintf(`
SELECT %s
FROM folder_path_observations
WHERE folder_id = ?
ORDER BY first_seen_at ASC, last_seen_at ASC, id ASC
`, folderObservationSelectColumns),
		folderID,
	)
	if err != nil {
		return nil, fmt.Errorf("folderRepo.ListPathObservationsByFolderID query: %w", err)
	}
	defer rows.Close()

	items := make([]*FolderPathObservation, 0)
	for rows.Next() {
		item, scanErr := scanFolderPathObservation(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("folderRepo.ListPathObservationsByFolderID scan: %w", scanErr)
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("folderRepo.ListPathObservationsByFolderID rows: %w", err)
	}

	return items, nil
}

func (r *SQLiteFolderRepository) List(ctx context.Context, filter FolderListFilter) ([]*Folder, int, error) {
	where := make([]string, 0, 4)
	args := make([]any, 0, 4)

	if filter.OnlyDeleted {
		where = append(where, "deleted_at IS NOT NULL")
	} else if !filter.IncludeDeleted {
		where = append(where, "deleted_at IS NULL")
	}

	if filter.Status != "" {
		where = append(where, "status = ?")
		args = append(args, filter.Status)
	}

	if filter.Category != "" {
		where = append(where, "category = ?")
		args = append(args, filter.Category)
	}

	if filter.Q != "" {
		where = append(where, "(path LIKE ? OR name LIKE ?)")
		term := "%" + filter.Q + "%"
		args = append(args, term, term)
	}
	if filter.TopLevelOnly {
		where = append(where, "relative_path <> ''", "instr(relative_path, '/') = 0", "instr(relative_path, char(92)) = 0")
	}

	whereClause := ""
	if len(where) > 0 {
		whereClause = " WHERE " + strings.Join(where, " AND ")
	}

	var total int
	countQuery := "SELECT COUNT(*) FROM folders" + whereClause
	if err := r.db.QueryRowContext(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("folderRepo.List count: %w", err)
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
	orderByClause := buildFolderListOrderBy(filter)

	rows, err := r.db.QueryContext(
		ctx,
		fmt.Sprintf(`SELECT %s
FROM folders%s
ORDER BY %s
LIMIT ? OFFSET ?`, folderSelectColumns, whereClause, orderByClause),
		listArgs...,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("folderRepo.List query: %w", err)
	}
	defer rows.Close()

	folders := make([]*Folder, 0)
	for rows.Next() {
		folder, err := scanFolder(rows)
		if err != nil {
			return nil, 0, fmt.Errorf("folderRepo.List scan: %w", err)
		}
		folders = append(folders, folder)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("folderRepo.List rows: %w", err)
	}

	return folders, total, nil
}

func buildFolderListOrderBy(filter FolderListFilter) string {
	sortBy := strings.TrimSpace(filter.SortBy)
	switch sortBy {
	case folderSortByUpdatedAt, folderSortByTotalSize:
	default:
		sortBy = folderSortByUpdatedAt
	}

	sortOrder := strings.ToLower(strings.TrimSpace(filter.SortOrder))
	switch sortOrder {
	case folderSortOrderAsc, folderSortOrderDesc:
	default:
		sortOrder = folderSortOrderDesc
	}

	return fmt.Sprintf("%s %s, updated_at DESC, id ASC", sortBy, sortOrder)
}

func (r *SQLiteFolderRepository) ListWorkflowSummariesByFolderIDs(ctx context.Context, folderIDs []string) (map[string]FolderWorkflowSummary, error) {
	summaries := make(map[string]FolderWorkflowSummary, len(folderIDs))
	if len(folderIDs) == 0 {
		return summaries, nil
	}

	uniqueFolderIDs := make([]string, 0, len(folderIDs))
	seen := make(map[string]struct{}, len(folderIDs))
	for _, folderID := range folderIDs {
		trimmed := strings.TrimSpace(folderID)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		uniqueFolderIDs = append(uniqueFolderIDs, trimmed)
		summaries[trimmed] = defaultFolderWorkflowSummary()
	}
	if len(uniqueFolderIDs) == 0 {
		return summaries, nil
	}

	if err := r.populateClassificationWorkflowSummaries(ctx, summaries, uniqueFolderIDs); err != nil {
		return nil, fmt.Errorf("folderRepo.ListWorkflowSummariesByFolderIDs classification: %w", err)
	}
	if err := r.populateProcessingWorkflowSummaries(ctx, summaries, uniqueFolderIDs); err != nil {
		return nil, fmt.Errorf("folderRepo.ListWorkflowSummariesByFolderIDs processing: %w", err)
	}

	return summaries, nil
}

func (r *SQLiteFolderRepository) populateClassificationWorkflowSummaries(ctx context.Context, summaries map[string]FolderWorkflowSummary, folderIDs []string) error {
	placeholders := strings.TrimRight(strings.Repeat("?,", len(folderIDs)), ",")
	args := make([]any, 0, len(folderIDs)*2)
	for _, folderID := range folderIDs {
		args = append(args, folderID)
	}
	for _, folderID := range folderIDs {
		args = append(args, folderID)
	}

	rows, err := r.db.QueryContext(
		ctx,
		`SELECT
	m.folder_id,
	wr.id,
	wr.job_id,
	wr.status,
	wr.updated_at,
	MAX(CASE WHEN m.source_kind = 'snapshot' THEN 1 ELSE 0 END) AS has_classify_snapshot,
	MAX(CASE WHEN nr.node_type IN (
		'ext-ratio-classifier',
		'name-keyword-classifier',
		'file-tree-classifier',
		'confidence-check',
		'subtree-aggregator',
		'classification-db-result-preview',
		'classification-writer'
	) THEN 1 ELSE 0 END) AS has_classification,
	MAX(CASE WHEN nr.node_type = 'classification-writer' AND nr.status = 'succeeded' THEN 1 ELSE 0 END) AS classification_writer_succeeded
FROM (
	SELECT folder_id, id AS workflow_run_id, 'workflow_run' AS source_kind
	FROM workflow_runs
	WHERE folder_id IN (`+placeholders+`)
	UNION
	SELECT s.folder_id, wr.id AS workflow_run_id, 'snapshot' AS source_kind
	FROM snapshots s
	JOIN workflow_runs wr ON wr.job_id = s.job_id
	WHERE s.operation_type = 'classify' AND s.folder_id IN (`+placeholders+`)
) m
JOIN workflow_runs wr ON wr.id = m.workflow_run_id
LEFT JOIN node_runs nr ON nr.workflow_run_id = wr.id
GROUP BY m.folder_id, wr.id, wr.job_id, wr.status, wr.updated_at, wr.created_at
HAVING MAX(CASE WHEN m.source_kind = 'snapshot' THEN 1 ELSE 0 END) = 1
	OR MAX(CASE WHEN nr.node_type IN (
		'ext-ratio-classifier',
		'name-keyword-classifier',
		'file-tree-classifier',
		'confidence-check',
		'subtree-aggregator',
		'classification-db-result-preview',
		'classification-writer'
	) THEN 1 ELSE 0 END) = 1
ORDER BY wr.updated_at DESC, wr.created_at DESC`,
		args...,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var folderID string
		var workflowRunID string
		var jobID string
		var runStatus string
		var updatedAtRaw any
		var hasClassifySnapshot int
		var hasClassification int
		var classificationWriterSucceeded int

		if scanErr := rows.Scan(
			&folderID,
			&workflowRunID,
			&jobID,
			&runStatus,
			&updatedAtRaw,
			&hasClassifySnapshot,
			&hasClassification,
			&classificationWriterSucceeded,
		); scanErr != nil {
			return scanErr
		}

		summary, ok := summaries[folderID]
		if !ok || summary.Classification.Status != workflowStageStatusNotRun {
			continue
		}

		updatedAt, parseErr := parseDBTime(updatedAtRaw)
		if parseErr != nil {
			return fmt.Errorf("parse classification updated_at: %w", parseErr)
		}

		classification := buildWorkflowStageSummary(workflowRunID, jobID, runStatus, updatedAt)
		if classificationWriterSucceeded == 1 || hasClassifySnapshot == 1 || hasClassification == 1 && strings.TrimSpace(runStatus) == "succeeded" {
			classification.Status = workflowStageStatusSucceeded
		}
		summary.Classification = classification
		summaries[folderID] = summary
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (r *SQLiteFolderRepository) populateProcessingWorkflowSummaries(ctx context.Context, summaries map[string]FolderWorkflowSummary, folderIDs []string) error {
	placeholders := strings.TrimRight(strings.Repeat("?,", len(folderIDs)), ",")
	args := make([]any, 0, len(folderIDs)*2)
	for _, folderID := range folderIDs {
		args = append(args, folderID)
	}
	for _, folderID := range folderIDs {
		args = append(args, folderID)
	}

	rows, err := r.db.QueryContext(
		ctx,
		`SELECT
	m.folder_id,
	wr.id,
	wr.job_id,
	wr.status,
	wr.updated_at
FROM (
	SELECT folder_id, id AS workflow_run_id
	FROM workflow_runs
	WHERE folder_id IN (`+placeholders+`)
	UNION
	SELECT folder_id, workflow_run_id
	FROM processing_review_items
	WHERE folder_id IN (`+placeholders+`)
) m
JOIN workflow_runs wr ON wr.id = m.workflow_run_id
LEFT JOIN node_runs nr ON nr.workflow_run_id = wr.id
GROUP BY m.folder_id, wr.id, wr.job_id, wr.status, wr.updated_at, wr.created_at
HAVING MAX(CASE WHEN nr.node_type IN (
	'rename-node',
	'move-node',
	'compress-node',
	'thumbnail-node'
) THEN 1 ELSE 0 END) = 1
ORDER BY wr.updated_at DESC, wr.created_at DESC`,
		args...,
	)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var folderID string
		var workflowRunID string
		var jobID string
		var runStatus string
		var updatedAtRaw any

		if scanErr := rows.Scan(
			&folderID,
			&workflowRunID,
			&jobID,
			&runStatus,
			&updatedAtRaw,
		); scanErr != nil {
			return scanErr
		}

		summary, ok := summaries[folderID]
		if !ok || summary.Processing.Status != workflowStageStatusNotRun {
			continue
		}

		updatedAt, parseErr := parseDBTime(updatedAtRaw)
		if parseErr != nil {
			return fmt.Errorf("parse processing updated_at: %w", parseErr)
		}

		summary.Processing = buildWorkflowStageSummary(workflowRunID, jobID, runStatus, updatedAt)
		summaries[folderID] = summary
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return nil
}

func (r *SQLiteFolderRepository) ListByPathPrefix(ctx context.Context, prefix string) ([]*Folder, error) {
	trimmedPrefix := trimFolderPathComparePrefix(prefix)
	if trimmedPrefix == "" {
		return []*Folder{}, nil
	}
	normalizedPrefix := normalizeFolderPathForCompare(trimmedPrefix)

	rows, err := r.db.QueryContext(
		ctx,
		fmt.Sprintf(`SELECT %s
FROM folders
WHERE deleted_at IS NULL AND (
	path = ?
	OR path LIKE ?
	OR path LIKE ?
	OR REPLACE(path, char(92), '/') = ?
	OR REPLACE(path, char(92), '/') LIKE ?
)
ORDER BY LENGTH(REPLACE(path, char(92), '/')) ASC, path ASC`, folderSelectColumns),
		trimmedPrefix,
		folderPathChildLike(trimmedPrefix, "/"),
		folderPathChildLike(trimmedPrefix, `\`),
		normalizedPrefix,
		folderPathChildLike(normalizedPrefix, "/"),
	)
	if err != nil {
		return nil, fmt.Errorf("folderRepo.ListByPathPrefix query: %w", err)
	}
	defer rows.Close()

	folders := make([]*Folder, 0)
	for rows.Next() {
		folder, scanErr := scanFolder(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("folderRepo.ListByPathPrefix scan: %w", scanErr)
		}
		folders = append(folders, folder)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("folderRepo.ListByPathPrefix rows: %w", err)
	}

	return folders, nil
}

func (r *SQLiteFolderRepository) ListByRelativePath(ctx context.Context, relativePath string) ([]*Folder, error) {
	trimmedRelativePath := strings.TrimSpace(relativePath)
	if trimmedRelativePath == "" {
		return []*Folder{}, nil
	}

	rows, err := r.db.QueryContext(
		ctx,
		fmt.Sprintf(`SELECT %s
FROM folders
WHERE deleted_at IS NULL AND relative_path = ?
ORDER BY updated_at DESC, scanned_at DESC, id ASC`, folderSelectColumns),
		trimmedRelativePath,
	)
	if err != nil {
		return nil, fmt.Errorf("folderRepo.ListByRelativePath query: %w", err)
	}
	defer rows.Close()

	folders := make([]*Folder, 0)
	for rows.Next() {
		folder, scanErr := scanFolder(rows)
		if scanErr != nil {
			return nil, fmt.Errorf("folderRepo.ListByRelativePath scan: %w", scanErr)
		}
		folders = append(folders, folder)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("folderRepo.ListByRelativePath rows: %w", err)
	}

	return folders, nil
}

func (r *SQLiteFolderRepository) UpdateCategory(ctx context.Context, id, category, source string) error {
	res, err := r.db.ExecContext(
		ctx,
		"UPDATE folders SET category = ?, category_source = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
		category,
		source,
		id,
	)
	if err != nil {
		return fmt.Errorf("folderRepo.UpdateCategory: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("folderRepo.UpdateCategory: %w", err)
	}

	return nil
}

func (r *SQLiteFolderRepository) UpdateStatus(ctx context.Context, id, status string) error {
	res, err := r.db.ExecContext(
		ctx,
		"UPDATE folders SET status = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
		status,
		id,
	)
	if err != nil {
		return fmt.Errorf("folderRepo.UpdateStatus: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("folderRepo.UpdateStatus: %w", err)
	}

	return nil
}

func (r *SQLiteFolderRepository) UpdatePath(ctx context.Context, id, newPath, sourceDir, relativePath string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("folderRepo.UpdatePath begin tx: %w", err)
	}
	defer rollbackTx(tx)

	name := deriveFolderNameFromPath(newPath)
	res, err := tx.ExecContext(
		ctx,
		"UPDATE folders SET path = ?, source_dir = ?, relative_path = ?, name = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
		newPath,
		sourceDir,
		relativePath,
		name,
		id,
	)
	if err != nil {
		return fmt.Errorf("folderRepo.UpdatePath: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("folderRepo.UpdatePath: %w", err)
	}

	if err := r.recordObservationTx(ctx, tx, id, newPath, sourceDir, relativePath, time.Now().UTC()); err != nil {
		return fmt.Errorf("folderRepo.UpdatePath record observation: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("folderRepo.UpdatePath commit: %w", err)
	}

	return nil
}

func (r *SQLiteFolderRepository) UpdateCoverImagePath(ctx context.Context, id, coverImagePath string) error {
	res, err := r.db.ExecContext(
		ctx,
		"UPDATE folders SET cover_image_path = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?",
		strings.TrimSpace(coverImagePath),
		id,
	)
	if err != nil {
		return fmt.Errorf("folderRepo.UpdateCoverImagePath: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("folderRepo.UpdateCoverImagePath: %w", err)
	}

	return nil
}

func (r *SQLiteFolderRepository) IsSuppressedPath(ctx context.Context, path string) (bool, error) {
	var exists int
	err := r.db.QueryRowContext(
		ctx,
		`SELECT 1
FROM folders f
JOIN folder_path_observations o ON o.folder_id = f.id
WHERE o.path = ? AND o.is_current = 1 AND f.deleted_at IS NOT NULL
LIMIT 1`,
		path,
	).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("folderRepo.IsSuppressedPath: %w", err)
	}

	return true, nil
}

func (r *SQLiteFolderRepository) Suppress(ctx context.Context, id, currentPath, originalPath string) error {
	res, err := r.db.ExecContext(
		ctx,
		"UPDATE folders SET deleted_at = CURRENT_TIMESTAMP, delete_staging_path = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND deleted_at IS NULL",
		id,
	)
	if err != nil {
		return fmt.Errorf("folderRepo.Suppress: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("folderRepo.Suppress: %w", err)
	}

	return nil
}

func (r *SQLiteFolderRepository) Unsuppress(ctx context.Context, id string) error {
	res, err := r.db.ExecContext(
		ctx,
		"UPDATE folders SET deleted_at = NULL, delete_staging_path = NULL, updated_at = CURRENT_TIMESTAMP WHERE id = ? AND deleted_at IS NOT NULL",
		id,
	)
	if err != nil {
		return fmt.Errorf("folderRepo.Unsuppress: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("folderRepo.Unsuppress: %w", err)
	}

	return nil
}

func (r *SQLiteFolderRepository) Delete(ctx context.Context, id string) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("folderRepo.Delete begin tx: %w", err)
	}
	defer rollbackTx(tx)

	if _, err := tx.ExecContext(ctx, "DELETE FROM folder_path_observations WHERE folder_id = ?", id); err != nil {
		return fmt.Errorf("folderRepo.Delete observations: %w", err)
	}

	res, err := tx.ExecContext(ctx, "DELETE FROM folders WHERE id = ?", id)
	if err != nil {
		return fmt.Errorf("folderRepo.Delete: %w", err)
	}

	if err := assertRowsAffected(res); err != nil {
		return fmt.Errorf("folderRepo.Delete: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("folderRepo.Delete commit: %w", err)
	}

	return nil
}

func (r *SQLiteFolderRepository) recordObservationTx(ctx context.Context, tx *sql.Tx, folderID, path, sourceDir, relativePath string, observedAt time.Time) error {
	trimmedFolderID := strings.TrimSpace(folderID)
	trimmedPath := strings.TrimSpace(path)
	trimmedSourceDir := strings.TrimSpace(sourceDir)
	trimmedRelativePath := strings.TrimSpace(relativePath)
	if trimmedFolderID == "" || trimmedPath == "" {
		return nil
	}

	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}

	existing, err := scanFolderPathObservation(
		tx.QueryRowContext(ctx, fmt.Sprintf(`
SELECT %s
FROM folder_path_observations
WHERE folder_id = ? AND path = ?
LIMIT 1
`, folderObservationSelectColumns), trimmedFolderID, trimmedPath),
	)
	if err != nil && !errors.Is(err, ErrNotFound) {
		return err
	}

	if _, err := tx.ExecContext(
		ctx,
		"UPDATE folder_path_observations SET is_current = 0 WHERE path = ? AND folder_id <> ? AND is_current = 1",
		trimmedPath,
		trimmedFolderID,
	); err != nil {
		return fmt.Errorf("clear current observation by path: %w", err)
	}

	if trimmedSourceDir != "" && trimmedRelativePath != "" {
		if _, err := tx.ExecContext(
			ctx,
			"UPDATE folder_path_observations SET is_current = 0 WHERE source_dir = ? AND relative_path = ? AND folder_id <> ? AND is_current = 1",
			trimmedSourceDir,
			trimmedRelativePath,
			trimmedFolderID,
		); err != nil {
			return fmt.Errorf("clear current observation by source and relative path: %w", err)
		}
	}

	if _, err := tx.ExecContext(ctx, "UPDATE folder_path_observations SET is_current = 0 WHERE folder_id = ? AND is_current = 1", trimmedFolderID); err != nil {
		return fmt.Errorf("clear current observation: %w", err)
	}

	firstSeenAt := observedAt
	observationID := uuid.NewString()
	if existing != nil {
		firstSeenAt = existing.FirstSeenAt
		observationID = existing.ID
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO folder_path_observations (
	id, folder_id, path, source_dir, relative_path, is_current, first_seen_at, last_seen_at
) VALUES (?, ?, ?, ?, ?, 1, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	source_dir = excluded.source_dir,
	relative_path = excluded.relative_path,
	is_current = 1,
	first_seen_at = excluded.first_seen_at,
	last_seen_at = excluded.last_seen_at`,
		observationID,
		trimmedFolderID,
		trimmedPath,
		trimmedSourceDir,
		trimmedRelativePath,
		firstSeenAt.Format("2006-01-02 15:04:05"),
		observedAt.Format("2006-01-02 15:04:05"),
	); err != nil {
		return fmt.Errorf("upsert observation: %w", err)
	}

	return nil
}

func scanFolderPathObservation(scanner interface{ Scan(dest ...any) error }) (*FolderPathObservation, error) {
	observation := &FolderPathObservation{}
	var isCurrent int
	var firstSeenAt any
	var lastSeenAt any

	err := scanner.Scan(
		&observation.ID,
		&observation.FolderID,
		&observation.Path,
		&observation.SourceDir,
		&observation.RelativePath,
		&isCurrent,
		&firstSeenAt,
		&lastSeenAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	observation.IsCurrent = intToBool(isCurrent)
	observation.FirstSeenAt, err = parseDBTime(firstSeenAt)
	if err != nil {
		return nil, fmt.Errorf("scanFolderPathObservation parse first_seen_at: %w", err)
	}
	observation.LastSeenAt, err = parseDBTime(lastSeenAt)
	if err != nil {
		return nil, fmt.Errorf("scanFolderPathObservation parse last_seen_at: %w", err)
	}

	return observation, nil
}

func scanFolder(scanner interface{ Scan(dest ...any) error }) (*Folder, error) {
	folder := &Folder{}
	folder.WorkflowSummary = defaultFolderWorkflowSummary()
	var markedForMove int
	var hasOtherFiles int
	var deletedAt any
	var deleteStagingPath sql.NullString
	var coverImagePath sql.NullString
	var outputCheckSummaryRaw sql.NullString
	var scannedAt any
	var updatedAt any

	err := scanner.Scan(
		&folder.ID,
		&folder.Path,
		&folder.SourceDir,
		&folder.RelativePath,
		&folder.IdentityFingerprint,
		&folder.Name,
		&folder.Category,
		&folder.CategorySource,
		&folder.Status,
		&folder.ImageCount,
		&folder.VideoCount,
		&folder.OtherFileCount,
		&hasOtherFiles,
		&folder.TotalFiles,
		&folder.TotalSize,
		&markedForMove,
		&deletedAt,
		&deleteStagingPath,
		&coverImagePath,
		&outputCheckSummaryRaw,
		&scannedAt,
		&updatedAt,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	folder.MarkedForMove = intToBool(markedForMove)
	folder.HasOtherFiles = intToBool(hasOtherFiles)
	if folder.DeletedAt, err = parseNullableTime(deletedAt); err != nil {
		return nil, fmt.Errorf("scanFolder parse deleted_at: %w", err)
	}
	if deleteStagingPath.Valid {
		folder.DeleteStagingPath = deleteStagingPath.String
	}
	if coverImagePath.Valid {
		folder.CoverImagePath = coverImagePath.String
	}
	if outputCheckSummaryRaw.Valid {
		folder.OutputCheckSummary = parseFolderOutputCheckSummary(outputCheckSummaryRaw.String)
	} else {
		folder.OutputCheckSummary = defaultFolderOutputCheckSummary()
	}

	folder.ScannedAt, err = parseDBTime(scannedAt)
	if err != nil {
		return nil, fmt.Errorf("scanFolder parse scanned_at: %w", err)
	}

	folder.UpdatedAt, err = parseDBTime(updatedAt)
	if err != nil {
		return nil, fmt.Errorf("scanFolder parse updated_at: %w", err)
	}

	return folder, nil
}

func defaultFolderOutputCheckSummary() FolderOutputCheckSummary {
	return FolderOutputCheckSummary{
		Status:        "pending",
		WorkflowRunID: "",
		CheckedAt:     nil,
		MismatchCount: 0,
		FailedFiles:   []string{},
	}
}

func parseFolderOutputCheckSummary(raw string) FolderOutputCheckSummary {
	summary := defaultFolderOutputCheckSummary()
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return summary
	}

	if err := json.Unmarshal([]byte(trimmed), &summary); err != nil {
		return defaultFolderOutputCheckSummary()
	}
	if strings.TrimSpace(summary.Status) == "" {
		summary.Status = "pending"
	}
	if summary.FailedFiles == nil {
		summary.FailedFiles = []string{}
	}

	return summary
}

func marshalFolderOutputCheckSummary(summary FolderOutputCheckSummary) string {
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
		return `{"status":"pending","workflow_run_id":"","checked_at":null,"mismatch_count":0,"failed_files":[]}`
	}

	return string(raw)
}

func defaultFolderWorkflowSummary() FolderWorkflowSummary {
	return FolderWorkflowSummary{
		Classification: WorkflowStageSummary{Status: workflowStageStatusNotRun},
		Processing:     WorkflowStageSummary{Status: workflowStageStatusNotRun},
	}
}

func buildWorkflowStageSummary(workflowRunID, jobID, runStatus string, updatedAt time.Time) WorkflowStageSummary {
	return WorkflowStageSummary{
		Status:        mapWorkflowRunStatusToStageStatus(runStatus),
		WorkflowRunID: workflowRunID,
		JobID:         jobID,
		UpdatedAt:     &updatedAt,
	}
}

func mapWorkflowRunStatusToStageStatus(status string) string {
	switch strings.TrimSpace(status) {
	case "succeeded":
		return workflowStageStatusSucceeded
	case "failed":
		return workflowStageStatusFailed
	case "waiting_input":
		return workflowStageStatusWaitingInput
	case "partial":
		return workflowStageStatusPartial
	case "rolled_back":
		return workflowStageStatusRolledBack
	case "cancelled":
		return workflowStageStatusCancelled
	case "running", "pending":
		return workflowStageStatusRunning
	default:
		return workflowStageStatusRunning
	}
}

func assertRowsAffected(res sql.Result) error {
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	if rows == 0 {
		return ErrNotFound
	}

	return nil
}

func deriveFolderNameFromPath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}

	normalized := strings.ReplaceAll(trimmed, `\`, "/")
	normalized = strings.TrimRight(normalized, "/")
	if normalized == "" {
		return ""
	}

	lastSlash := strings.LastIndex(normalized, "/")
	if lastSlash < 0 {
		return normalized
	}

	return normalized[lastSlash+1:]
}

func rollbackTx(tx *sql.Tx) {
	if tx != nil {
		_ = tx.Rollback()
	}
}
