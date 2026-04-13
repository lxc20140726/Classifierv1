package service

import (
	"context"
	"fmt"
	"sync"

	"github.com/robfig/cron/v3"

	"github.com/liqiye/classifier/internal/repository"
)

type ScheduledWorkflowScheduler struct {
	repo    repository.ScheduledWorkflowRepository
	service *ScheduledWorkflowService
	cron    *cron.Cron

	mu       sync.Mutex
	entryIDs map[string]cron.EntryID
}

func NewScheduledWorkflowScheduler(repo repository.ScheduledWorkflowRepository, service *ScheduledWorkflowService) *ScheduledWorkflowScheduler {
	return &ScheduledWorkflowScheduler{
		repo:     repo,
		service:  service,
		cron:     cron.New(),
		entryIDs: make(map[string]cron.EntryID),
	}
}

func (s *ScheduledWorkflowScheduler) Start(ctx context.Context) error {
	if s.cron == nil {
		return nil
	}
	s.cron.Start()

	return s.Sync(ctx)
}

func (s *ScheduledWorkflowScheduler) Sync(ctx context.Context) error {
	if s.repo == nil || s.service == nil || s.cron == nil {
		return nil
	}

	items, err := s.repo.ListEnabled(ctx)
	if err != nil {
		return fmt.Errorf("scheduledWorkflowScheduler.Sync list enabled: %w", err)
	}

	s.mu.Lock()
	for id, entryID := range s.entryIDs {
		s.cron.Remove(entryID)
		delete(s.entryIDs, id)
	}
	for _, item := range items {
		if _, err := cron.ParseStandard(item.CronSpec); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("scheduledWorkflowScheduler.Sync invalid cron for %q: %w", item.ID, err)
		}

		scheduledID := item.ID
		entryID, addErr := s.cron.AddFunc(item.CronSpec, func() {
			_, _ = s.service.RunNow(context.Background(), scheduledID)
		})
		if addErr != nil {
			s.mu.Unlock()
			return fmt.Errorf("scheduledWorkflowScheduler.Sync add cron entry for %q: %w", item.ID, addErr)
		}
		s.entryIDs[item.ID] = entryID
	}
	s.mu.Unlock()

	return nil
}

func (s *ScheduledWorkflowScheduler) Stop(ctx context.Context) error {
	if s.cron == nil {
		return nil
	}

	stopCtx := s.cron.Stop()
	select {
	case <-stopCtx.Done():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
