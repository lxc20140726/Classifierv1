package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/liqiye/classifier/internal/repository"
)

type ScheduledWorkflowRunner interface {
	StartJob(ctx context.Context, input StartWorkflowJobInput) (string, error)
}

type ScheduledScanRunner interface {
	StartScheduledJob(ctx context.Context, sourceDirs []string) (string, bool, error)
}

type ScheduledWorkflowService struct {
	repo       repository.ScheduledWorkflowRepository
	runner     ScheduledWorkflowRunner
	scanRunner ScheduledScanRunner
}

func NewScheduledWorkflowService(repo repository.ScheduledWorkflowRepository, runner ScheduledWorkflowRunner, scanRunner ScheduledScanRunner) *ScheduledWorkflowService {
	return &ScheduledWorkflowService{repo: repo, runner: runner, scanRunner: scanRunner}
}

func (s *ScheduledWorkflowService) Create(ctx context.Context, jobType, name, workflowDefID, cronSpec string, enabled bool, folderIDs, sourceDirs []string) (*repository.ScheduledWorkflow, error) {
	item, err := newScheduledWorkflow(jobType, name, workflowDefID, cronSpec, enabled, folderIDs, sourceDirs)
	if err != nil {
		return nil, err
	}
	item.ID = uuid.NewString()

	if err := s.repo.Create(ctx, item); err != nil {
		return nil, fmt.Errorf("scheduledWorkflowService.Create: %w", err)
	}

	return s.repo.GetByID(ctx, item.ID)
}

func (s *ScheduledWorkflowService) Update(ctx context.Context, id, jobType, name, workflowDefID, cronSpec string, enabled bool, folderIDs, sourceDirs []string) (*repository.ScheduledWorkflow, error) {
	item, err := newScheduledWorkflow(jobType, name, workflowDefID, cronSpec, enabled, folderIDs, sourceDirs)
	if err != nil {
		return nil, err
	}
	item.ID = id

	existing, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("scheduledWorkflowService.Update get existing: %w", err)
	}
	item.LastRunAt = existing.LastRunAt

	if err := s.repo.Update(ctx, item); err != nil {
		return nil, fmt.Errorf("scheduledWorkflowService.Update: %w", err)
	}

	return s.repo.GetByID(ctx, id)
}

func (s *ScheduledWorkflowService) RunNow(ctx context.Context, id string) (string, error) {
	if s.runner == nil {
		return "", fmt.Errorf("scheduledWorkflowService.RunNow: workflow runner not configured")
	}

	item, err := s.repo.GetByID(ctx, id)
	if err != nil {
		return "", fmt.Errorf("scheduledWorkflowService.RunNow get scheduled workflow: %w", err)
	}

	var jobID string
	switch strings.TrimSpace(item.JobType) {
	case "scan":
		if s.scanRunner == nil {
			return "", fmt.Errorf("scheduledWorkflowService.RunNow: scan runner not configured")
		}
		sourceDirs, sourceErr := scheduledWorkflowSourceDirs(item)
		if sourceErr != nil {
			return "", fmt.Errorf("scheduledWorkflowService.RunNow parse source_dirs: %w", sourceErr)
		}
		startedJobID, started, startErr := s.scanRunner.StartScheduledJob(ctx, sourceDirs)
		if startErr != nil {
			return "", fmt.Errorf("scheduledWorkflowService.RunNow start scan job: %w", startErr)
		}
		if !started {
			return "", fmt.Errorf("scheduledWorkflowService.RunNow scan job deduplicated")
		}
		jobID = startedJobID
	default:
		startedJobID, startErr := s.runner.StartJob(ctx, StartWorkflowJobInput{
			WorkflowDefID: item.WorkflowDefID,
		})
		if startErr != nil {
			return "", fmt.Errorf("scheduledWorkflowService.RunNow start workflow job: %w", startErr)
		}
		jobID = startedJobID
	}

	if err := s.repo.UpdateLastRunAt(ctx, item.ID, time.Now().UTC()); err != nil {
		return "", fmt.Errorf("scheduledWorkflowService.RunNow update last_run_at: %w", err)
	}

	return jobID, nil
}

func newScheduledWorkflow(jobType, name, workflowDefID, cronSpec string, enabled bool, folderIDs, sourceDirs []string) (*repository.ScheduledWorkflow, error) {
	cleanJobType := strings.TrimSpace(jobType)
	if cleanJobType == "" {
		cleanJobType = "workflow"
	}
	if cleanJobType != "workflow" && cleanJobType != "scan" {
		return nil, fmt.Errorf("job_type is invalid")
	}
	cleanName := strings.TrimSpace(name)
	if cleanName == "" {
		return nil, fmt.Errorf("scheduled workflow name is required")
	}
	cleanCron := strings.TrimSpace(cronSpec)
	if cleanCron == "" {
		return nil, fmt.Errorf("cron_spec is required")
	}

	cleanWorkflowDefID := strings.TrimSpace(workflowDefID)
	normalizedFolderIDs := normalizeScheduledWorkflowFolderIDs(folderIDs)
	normalizedSourceDirs := normalizeScanSourceDirs(sourceDirs)
	if cleanJobType == "workflow" {
		if cleanWorkflowDefID == "" {
			return nil, fmt.Errorf("workflow_def_id is required")
		}
	}
	if cleanJobType == "scan" && len(normalizedSourceDirs) == 0 {
		return nil, fmt.Errorf("source_dirs is required")
	}

	folderIDsJSON, err := json.Marshal(normalizedFolderIDs)
	if err != nil {
		return nil, fmt.Errorf("marshal folder_ids: %w", err)
	}
	sourceDirsJSON, err := json.Marshal(normalizedSourceDirs)
	if err != nil {
		return nil, fmt.Errorf("marshal source_dirs: %w", err)
	}

	return &repository.ScheduledWorkflow{
		Name:          cleanName,
		JobType:       cleanJobType,
		WorkflowDefID: cleanWorkflowDefID,
		FolderIDs:     string(folderIDsJSON),
		SourceDirs:    string(sourceDirsJSON),
		CronSpec:      cleanCron,
		Enabled:       enabled,
	}, nil
}

func normalizeScheduledWorkflowFolderIDs(folderIDs []string) []string {
	seen := make(map[string]struct{}, len(folderIDs))
	out := make([]string, 0, len(folderIDs))
	for _, folderID := range folderIDs {
		trimmed := strings.TrimSpace(folderID)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}

	return out
}

func scheduledWorkflowFolderIDs(item *repository.ScheduledWorkflow) ([]string, error) {
	if item == nil || strings.TrimSpace(item.FolderIDs) == "" {
		return nil, nil
	}

	var folderIDs []string
	if err := json.Unmarshal([]byte(item.FolderIDs), &folderIDs); err != nil {
		return nil, err
	}

	return normalizeScheduledWorkflowFolderIDs(folderIDs), nil
}

func ScheduledWorkflowFolderIDsForAPI(item *repository.ScheduledWorkflow) ([]string, error) {
	return scheduledWorkflowFolderIDs(item)
}

func scheduledWorkflowSourceDirs(item *repository.ScheduledWorkflow) ([]string, error) {
	if item == nil || strings.TrimSpace(item.SourceDirs) == "" {
		return nil, nil
	}

	var sourceDirs []string
	if err := json.Unmarshal([]byte(item.SourceDirs), &sourceDirs); err != nil {
		return nil, err
	}

	return normalizeScanSourceDirs(sourceDirs), nil
}

func ScheduledWorkflowSourceDirsForAPI(item *repository.ScheduledWorkflow) ([]string, error) {
	return scheduledWorkflowSourceDirs(item)
}

func (s *ScheduledWorkflowService) BootstrapLegacyScanCron(ctx context.Context, configRepo repository.ConfigRepository) error {
	if s.repo == nil || configRepo == nil {
		return nil
	}

	items, _, err := s.repo.List(ctx, repository.ScheduledWorkflowListFilter{Page: 1, Limit: 1000})
	if err != nil {
		return fmt.Errorf("scheduledWorkflowService.BootstrapLegacyScanCron list jobs: %w", err)
	}
	for _, item := range items {
		if strings.TrimSpace(item.JobType) == "scan" {
			return nil
		}
	}

	cfg, err := configRepo.GetAppConfig(ctx)
	if err != nil {
		return fmt.Errorf("scheduledWorkflowService.BootstrapLegacyScanCron get app config: %w", err)
	}
	if strings.TrimSpace(cfg.ScanCron) == "" {
		return nil
	}
	sourceDirs := normalizeScanSourceDirs(cfg.ScanInputDirs)
	if len(sourceDirs) == 0 {
		return nil
	}

	_, err = s.Create(ctx, "scan", "定时扫描", "", cfg.ScanCron, true, nil, sourceDirs)
	if err != nil {
		return fmt.Errorf("scheduledWorkflowService.BootstrapLegacyScanCron create scan job: %w", err)
	}

	return nil
}
