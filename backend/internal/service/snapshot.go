package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

type snapshotPathState struct {
	OriginalPath string `json:"original_path"`
	CurrentPath  string `json:"current_path"`
}

var errSnapshotAlreadyReverted = errors.New("snapshot already reverted")

// RevertResult describes the outcome of a revert attempt.
type RevertResult struct {
	OK           bool                `json:"ok"`
	ErrorMessage string              `json:"error_message,omitempty"`
	CurrentState []snapshotPathState `json:"current_state"`
	PreflightErr string              `json:"preflight_error,omitempty"`
}

type SnapshotService struct {
	fs        fs.FSAdapter
	snapshots repository.SnapshotRepository
	folders   repository.FolderRepository
}

func NewSnapshotService(fsAdapter fs.FSAdapter, snapshotRepo repository.SnapshotRepository, folderRepo repository.FolderRepository) *SnapshotService {
	return &SnapshotService{fs: fsAdapter, snapshots: snapshotRepo, folders: folderRepo}
}

func (s *SnapshotService) CreateBefore(ctx context.Context, jobID, folderID, operationType string) (string, error) {
	folder, err := s.folders.GetByID(ctx, folderID)
	if err != nil {
		return "", fmt.Errorf("snapshot.CreateBefore load folder %q: %w", folderID, err)
	}

	beforeState, err := json.Marshal([]snapshotPathState{{
		OriginalPath: folder.Path,
		CurrentPath:  folder.Path,
	}})
	if err != nil {
		return "", fmt.Errorf("snapshot.CreateBefore marshal before state: %w", err)
	}

	return s.CreateBeforeWithState(ctx, jobID, folderID, operationType, beforeState)
}

func (s *SnapshotService) CreateBeforeWithState(ctx context.Context, jobID, folderID, operationType string, before json.RawMessage) (string, error) {
	trimmedBefore := before
	if len(trimmedBefore) == 0 {
		trimmedBefore = json.RawMessage(`null`)
	}

	snapshotID := uuid.NewString()
	if err := s.snapshots.Create(ctx, &repository.Snapshot{
		ID:            snapshotID,
		JobID:         jobID,
		FolderID:      folderID,
		OperationType: operationType,
		Before:        trimmedBefore,
		Status:        "pending",
	}); err != nil {
		return "", fmt.Errorf("snapshot.CreateBeforeWithState create snapshot: %w", err)
	}

	return snapshotID, nil
}

func (s *SnapshotService) CommitAfter(ctx context.Context, snapshotID string, after json.RawMessage) error {
	if err := s.snapshots.CommitAfter(ctx, snapshotID, after); err != nil {
		return fmt.Errorf("snapshot.CommitAfter persist after state: %w", err)
	}

	if err := s.snapshots.UpdateStatus(ctx, snapshotID, "committed"); err != nil {
		return fmt.Errorf("snapshot.CommitAfter update status: %w", err)
	}

	return nil
}

func (s *SnapshotService) UpdateDetail(ctx context.Context, snapshotID string, detail json.RawMessage) error {
	if err := s.snapshots.UpdateDetail(ctx, snapshotID, detail); err != nil {
		return fmt.Errorf("snapshot.UpdateDetail persist detail: %w", err)
	}

	return nil
}

// Revert reverts the snapshot. It performs a preflight check first to ensure
// the target (original) path is clear before touching the filesystem.
// Returns a RevertResult with full detail regardless of success/failure so
// the caller can surface accurate state to the user.
func (s *SnapshotService) Revert(ctx context.Context, snapshotID string) (*RevertResult, error) {
	snapshot, err := s.snapshots.GetByID(ctx, snapshotID)
	if err != nil {
		return nil, fmt.Errorf("snapshot.Revert load snapshot %q: %w", snapshotID, err)
	}

	if snapshot.Status == "reverted" {
		return nil, errSnapshotAlreadyReverted
	}

	stateJSON := snapshot.After
	if len(stateJSON) == 0 {
		stateJSON = snapshot.Before
	}

	var states []snapshotPathState
	if err := json.Unmarshal(stateJSON, &states); err != nil {
		return nil, fmt.Errorf("snapshot.Revert parse snapshot state: %w", err)
	}

	// Preflight: check each target (original) path is not already occupied
	// by a different directory, which would indicate a collision risk.
	for _, state := range states {
		if state.CurrentPath == state.OriginalPath {
			// Already at original position, nothing to move.
			continue
		}

		// Verify current path still exists (the file must still be there to move back).
		currentExists, existsErr := s.fs.Exists(ctx, state.CurrentPath)
		if existsErr != nil {
			result := &RevertResult{
				OK:           false,
				PreflightErr: fmt.Sprintf("无法检查当前路径 %q 是否存在：%s", state.CurrentPath, existsErr.Error()),
				CurrentState: states,
			}
			return result, fmt.Errorf("snapshot.Revert preflight stat current %q: %w", state.CurrentPath, existsErr)
		}
		if !currentExists {
			result := &RevertResult{
				OK:           false,
				PreflightErr: fmt.Sprintf("当前路径 %q 不存在，无法回退（文件可能已被移动或删除）", state.CurrentPath),
				CurrentState: states,
			}
			return result, fmt.Errorf("snapshot.Revert preflight: current path %q not found", state.CurrentPath)
		}

		// Verify original (target) path is clear.
		originalExists, existsErr := s.fs.Exists(ctx, state.OriginalPath)
		if existsErr != nil {
			result := &RevertResult{
				OK:           false,
				PreflightErr: fmt.Sprintf("无法检查目标路径 %q 是否存在：%s", state.OriginalPath, existsErr.Error()),
				CurrentState: states,
			}
			return result, fmt.Errorf("snapshot.Revert preflight stat original %q: %w", state.OriginalPath, existsErr)
		}
		if originalExists {
			result := &RevertResult{
				OK:           false,
				PreflightErr: fmt.Sprintf("目标路径 %q 已存在其他内容，回退会造成冲突，操作已取消", state.OriginalPath),
				CurrentState: states,
			}
			return result, fmt.Errorf("snapshot.Revert preflight: original path %q already exists", state.OriginalPath)
		}
	}

	// All preflight checks passed — now actually move.
	for _, state := range states {
		if state.CurrentPath == state.OriginalPath {
			continue
		}

		if err := s.fs.MoveDir(ctx, state.CurrentPath, state.OriginalPath); err != nil {
			result := &RevertResult{
				OK:           false,
				ErrorMessage: fmt.Sprintf("移动 %q → %q 失败：%s", state.CurrentPath, state.OriginalPath, err.Error()),
				CurrentState: states,
			}
			return result, fmt.Errorf("snapshot.Revert move %q to %q: %w", state.CurrentPath, state.OriginalPath, err)
		}
	}

	// Update DB folder path records.
	for _, state := range states {
		folder, err := s.folders.GetByID(ctx, snapshot.FolderID)
		if err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				continue
			}
			return &RevertResult{
				OK:           false,
				ErrorMessage: fmt.Sprintf("回退文件系统成功，但更新数据库记录失败：%s", err.Error()),
				CurrentState: []snapshotPathState{{OriginalPath: state.OriginalPath, CurrentPath: state.OriginalPath}},
			}, fmt.Errorf("snapshot.Revert load folder %q: %w", snapshot.FolderID, err)
		}

		nextSourceDir := folder.SourceDir
		nextRelativePath := relativePathFromSourceDir(folder.SourceDir, state.OriginalPath)
		if nextRelativePath == "" || strings.HasPrefix(nextRelativePath, "..") {
			nextSourceDir = filepath.Dir(state.OriginalPath)
			nextRelativePath = filepath.Base(state.OriginalPath)
		}

		if err := s.folders.UpdatePath(ctx, folder.ID, state.OriginalPath, nextSourceDir, nextRelativePath); err != nil {
			return &RevertResult{
				OK:           false,
				ErrorMessage: fmt.Sprintf("回退文件系统成功，但更新数据库路径失败：%s", err.Error()),
				CurrentState: []snapshotPathState{{OriginalPath: state.OriginalPath, CurrentPath: state.OriginalPath}},
			}, fmt.Errorf("snapshot.Revert update folder path for %q: %w", folder.ID, err)
		}
	}

	if err := s.snapshots.UpdateStatus(ctx, snapshot.ID, "reverted"); err != nil {
		return &RevertResult{
			OK:           false,
			ErrorMessage: fmt.Sprintf("回退完成，但标记快照状态失败：%s", err.Error()),
			CurrentState: states,
		}, fmt.Errorf("snapshot.Revert update snapshot status: %w", err)
	}

	return &RevertResult{
		OK:           true,
		CurrentState: states,
	}, nil
}
