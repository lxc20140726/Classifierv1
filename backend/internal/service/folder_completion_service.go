package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/liqiye/classifier/internal/repository"
)

type FolderCompletionService struct {
	folders      repository.FolderRepository
	outputChecks repository.OutputCheckRepository
}

func NewFolderCompletionService(
	folderRepo repository.FolderRepository,
	outputCheckRepo repository.OutputCheckRepository,
) *FolderCompletionService {
	return &FolderCompletionService{
		folders:      folderRepo,
		outputChecks: outputCheckRepo,
	}
}

func (s *FolderCompletionService) Sync(ctx context.Context, folderID string, check *repository.FolderOutputCheck) error {
	if strings.TrimSpace(folderID) == "" {
		return nil
	}

	targetStatus := "pending"
	if check != nil && strings.EqualFold(strings.TrimSpace(check.Status), "passed") {
		targetStatus = "done"
	}
	if err := s.folders.UpdateStatus(ctx, folderID, targetStatus); err != nil {
		return fmt.Errorf("folderCompletion.Sync update folder %q status to %q: %w", folderID, targetStatus, err)
	}

	return nil
}

func (s *FolderCompletionService) MarkPending(ctx context.Context, folderID string) error {
	if strings.TrimSpace(folderID) == "" {
		return nil
	}
	if err := s.folders.UpdateStatus(ctx, folderID, "pending"); err != nil {
		return fmt.Errorf("folderCompletion.MarkPending update folder %q status: %w", folderID, err)
	}
	if s.outputChecks != nil {
		if err := s.outputChecks.MarkFolderPending(ctx, folderID); err != nil {
			return fmt.Errorf("folderCompletion.MarkPending mark folder %q pending summary: %w", folderID, err)
		}
	}

	return nil
}
