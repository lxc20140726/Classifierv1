package service

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/sse"
)

type MoveFolderInput struct {
	FolderIDs []string
	TargetDir string
	JobID     string
}

type SnapshotRecorder interface {
	CreateBefore(ctx context.Context, jobID, folderID, operationType string) (string, error)
	CommitAfter(ctx context.Context, snapshotID string, after json.RawMessage) error
}

type AuditWriter interface {
	Write(ctx context.Context, log *repository.AuditLog) error
}

type MoveService struct {
	fs        fs.FSAdapter
	jobs      repository.JobRepository
	folders   repository.FolderRepository
	snapshots SnapshotRecorder
	audit     AuditWriter
	broker    *sse.Broker
}

func NewMoveService(
	fsAdapter fs.FSAdapter,
	jobRepo repository.JobRepository,
	folderRepo repository.FolderRepository,
	snapshots SnapshotRecorder,
	audit AuditWriter,
	broker *sse.Broker,
) *MoveService {
	return &MoveService{
		fs:        fsAdapter,
		jobs:      jobRepo,
		folders:   folderRepo,
		snapshots: snapshots,
		audit:     audit,
		broker:    broker,
	}
}

func (s *MoveService) MoveFolders(ctx context.Context, input MoveFolderInput) error {
	if input.TargetDir == "" {
		return fmt.Errorf("moveService.MoveFolders: target dir is required")
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	if s.jobs != nil {
		if err := s.jobs.UpdateStatus(ctx, input.JobID, "running", ""); err != nil {
			return fmt.Errorf("moveService.MoveFolders start job %q: %w", input.JobID, err)
		}
	}

	total := len(input.FolderIDs)
	failedCount := 0
	for i, folderID := range input.FolderIDs {
		if err := s.moveOne(ctx, input, i, total, folderID); err != nil {
			failedCount++
			if s.jobs != nil {
				_ = s.jobs.IncrementProgress(ctx, input.JobID, 0, 1)
			}
			s.publish("job.error", map[string]any{
				"job_id":    input.JobID,
				"folder_id": folderID,
				"error":     err.Error(),
				"done":      i + 1,
				"total":     total,
			})
		}
	}

	status := "succeeded"
	if failedCount > 0 {
		status = "partial"
	}
	if s.jobs != nil {
		if err := s.jobs.UpdateStatus(ctx, input.JobID, status, ""); err != nil {
			return fmt.Errorf("moveService.MoveFolders finish job %q: %w", input.JobID, err)
		}
	}

	s.publish("job.done", map[string]any{
		"job_id": input.JobID,
		"status": status,
		"total":  total,
	})

	return nil
}

func (s *MoveService) moveOne(ctx context.Context, input MoveFolderInput, i, total int, folderID string) error {
	folder, err := s.folders.GetByID(ctx, folderID)
	if err != nil {
		s.writeFailureAudit(ctx, input.JobID, folderID, "", err)
		return fmt.Errorf("moveService.MoveFolders get folder %q: %w", folderID, err)
	}

	snapshotID, err := s.snapshots.CreateBefore(ctx, input.JobID, folder.ID, "move")
	if err != nil {
		s.writeFailureAudit(ctx, input.JobID, folder.ID, folder.Path, err)
		return fmt.Errorf("moveService.MoveFolders create snapshot for folder %q: %w", folder.ID, err)
	}

	if detailRecorder, ok := s.snapshots.(interface {
		UpdateDetail(context.Context, string, json.RawMessage) error
	}); ok {
		detailJSON, marshalErr := json.Marshal(map[string]any{
			"source_path": folder.Path,
			"target_dir":  input.TargetDir,
			"folder_name": folder.Name,
			"category":    folder.Category,
		})
		if marshalErr != nil {
			s.writeFailureAudit(ctx, input.JobID, folder.ID, folder.Path, marshalErr)
			return fmt.Errorf("moveService.MoveFolders marshal snapshot detail for folder %q: %w", folder.ID, marshalErr)
		}
		if err := detailRecorder.UpdateDetail(ctx, snapshotID, detailJSON); err != nil {
			s.writeFailureAudit(ctx, input.JobID, folder.ID, folder.Path, err)
			return fmt.Errorf("moveService.MoveFolders update snapshot detail for folder %q: %w", folder.ID, err)
		}
	}

	if err := s.fs.MkdirAll(ctx, input.TargetDir, 0o755); err != nil {
		s.writeFailureAudit(ctx, input.JobID, folder.ID, folder.Path, err)
		return fmt.Errorf("moveService.MoveFolders create target dir %q: %w", input.TargetDir, err)
	}

	dst := filepath.Join(input.TargetDir, folder.Name)
	if err := s.fs.MoveDir(ctx, folder.Path, dst); err != nil {
		s.writeFailureAudit(ctx, input.JobID, folder.ID, folder.Path, err)
		return fmt.Errorf("moveService.MoveFolders move folder %q to %q: %w", folder.Path, dst, err)
	}

	afterPayload := []map[string]string{{
		"original_path": folder.Path,
		"current_path":  dst,
	}}
	afterJSON, err := json.Marshal(afterPayload)
	if err != nil {
		s.writeFailureAudit(ctx, input.JobID, folder.ID, dst, err)
		return fmt.Errorf("moveService.MoveFolders marshal after payload for folder %q: %w", folder.ID, err)
	}

	if err := s.snapshots.CommitAfter(ctx, snapshotID, afterJSON); err != nil {
		s.writeFailureAudit(ctx, input.JobID, folder.ID, dst, err)
		return fmt.Errorf("moveService.MoveFolders commit snapshot %q: %w", snapshotID, err)
	}

	nextSourceDir := folder.SourceDir
	nextRelativePath := relativePathFromSourceDir(folder.SourceDir, dst)
	if nextRelativePath == "" || strings.HasPrefix(nextRelativePath, "..") {
		nextSourceDir = filepath.Dir(dst)
		nextRelativePath = filepath.Base(dst)
	}

	if err := s.folders.UpdatePath(ctx, folder.ID, dst, nextSourceDir, nextRelativePath); err != nil {
		s.writeFailureAudit(ctx, input.JobID, folder.ID, dst, err)
		return fmt.Errorf("moveService.MoveFolders update folder path %q: %w", folder.ID, err)
	}

	if s.jobs != nil {
		if err := s.jobs.IncrementProgress(ctx, input.JobID, 1, 0); err != nil {
			return fmt.Errorf("moveService.MoveFolders update job progress %q: %w", input.JobID, err)
		}
	}

	s.publish("job.progress", map[string]any{
		"job_id":    input.JobID,
		"folder_id": folder.ID,
		"done":      i + 1,
		"total":     total,
	})

	if err := s.audit.Write(ctx, &repository.AuditLog{
		ID:         fmt.Sprintf("audit-move-success-%s-%d", folder.ID, time.Now().UTC().UnixNano()),
		JobID:      input.JobID,
		FolderID:   folder.ID,
		FolderPath: dst,
		Action:     "move",
		Level:      "info",
		Detail:     json.RawMessage(fmt.Sprintf(`{"source_path":%q,"target_dir":%q,"category":%q}`, folder.Path, input.TargetDir, folder.Category)),
		Result:     "success",
	}); err != nil {
		return fmt.Errorf("moveService.MoveFolders write success audit for folder %q: %w", folder.ID, err)
	}

	return nil
}

func (s *MoveService) writeFailureAudit(ctx context.Context, jobID, folderID, folderPath string, moveErr error) {
	if s.audit == nil {
		return
	}

	_ = s.audit.Write(ctx, &repository.AuditLog{
		ID:         fmt.Sprintf("audit-move-failed-%s-%d", folderID, time.Now().UTC().UnixNano()),
		JobID:      jobID,
		FolderID:   folderID,
		FolderPath: folderPath,
		Action:     "move",
		Level:      "error",
		Result:     "failed",
		ErrorMsg:   moveErr.Error(),
	})
}

func (s *MoveService) publish(eventType string, payload any) {
	if s.broker == nil {
		return
	}

	_ = s.broker.Publish(eventType, payload)
}
