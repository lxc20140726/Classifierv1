package service

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/robfig/cron/v3"

	"github.com/liqiye/classifier/internal/repository"
)

type ScheduledScanStarter interface {
	StartScheduledJob(ctx context.Context, sourceDirs []string) (string, bool, error)
}

type ScanScheduler struct {
	config  repository.ConfigRepository
	starter ScheduledScanStarter
	cron    *cron.Cron

	mu      sync.Mutex
	entryID cron.EntryID
	spec    string
}

func NewScanScheduler(configRepo repository.ConfigRepository, starter ScheduledScanStarter) *ScanScheduler {
	return &ScanScheduler{
		config:  configRepo,
		starter: starter,
		cron:    cron.New(),
	}
}

func (s *ScanScheduler) Start(ctx context.Context) error {
	if s.cron == nil {
		return nil
	}
	s.cron.Start()

	return s.Sync(ctx)
}

func (s *ScanScheduler) Sync(ctx context.Context) error {
	if s.config == nil || s.starter == nil || s.cron == nil {
		return nil
	}

	cfg, err := s.config.GetAppConfig(ctx)
	if err != nil {
		return fmt.Errorf("scanScheduler.Sync load config: %w", err)
	}

	spec := strings.TrimSpace(cfg.ScanCron)
	if spec == "" {
		s.clear()
		return nil
	}
	if _, err := cron.ParseStandard(spec); err != nil {
		return fmt.Errorf("scanScheduler.Sync invalid scan_cron %q: %w", spec, err)
	}

	sourceDirs := normalizeScanSourceDirs(cfg.ScanInputDirs)
	if len(sourceDirs) == 0 {
		s.clear()
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.spec == spec && s.entryID != 0 {
		return nil
	}
	if s.entryID != 0 {
		s.cron.Remove(s.entryID)
		s.entryID = 0
	}

	entryID, err := s.cron.AddFunc(spec, func() {
		_, _, _ = s.starter.StartScheduledJob(context.Background(), sourceDirs)
	})
	if err != nil {
		return fmt.Errorf("scanScheduler.Sync add cron entry: %w", err)
	}

	s.entryID = entryID
	s.spec = spec
	return nil
}

func (s *ScanScheduler) Stop(ctx context.Context) error {
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

func (s *ScanScheduler) clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entryID != 0 {
		s.cron.Remove(s.entryID)
		s.entryID = 0
	}
	s.spec = ""
}
