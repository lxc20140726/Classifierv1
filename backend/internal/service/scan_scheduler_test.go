package service

import (
	"context"
	"testing"

	"github.com/liqiye/classifier/internal/repository"
)

type stubScheduledScanStarter struct {
	called int
}

func (s *stubScheduledScanStarter) StartScheduledJob(_ context.Context, _ []string) (string, bool, error) {
	s.called++
	return "job-1", true, nil
}

func TestScanSchedulerSyncRegistersCronEntry(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	configRepo := repository.NewConfigRepository(database)
	if err := configRepo.SaveAppConfig(context.Background(), &repository.AppConfig{
		ScanInputDirs: []string{"/source"},
		ScanCron:      "*/15 * * * *",
	}); err != nil {
		t.Fatalf("SaveAppConfig() error = %v", err)
	}

	starter := &stubScheduledScanStarter{}
	scheduler := NewScanScheduler(configRepo, starter)
	if err := scheduler.Sync(context.Background()); err != nil {
		t.Fatalf("Sync() error = %v", err)
	}
	if scheduler.entryID == 0 {
		t.Fatalf("entryID = 0, want non-zero")
	}
	if scheduler.spec != "*/15 * * * *" {
		t.Fatalf("spec = %q, want */15 * * * *", scheduler.spec)
	}
}

func TestScanSchedulerSyncRejectsInvalidCron(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	configRepo := repository.NewConfigRepository(database)
	if err := configRepo.SaveAppConfig(context.Background(), &repository.AppConfig{
		ScanInputDirs: []string{"/source"},
		ScanCron:      "bad cron",
	}); err != nil {
		t.Fatalf("SaveAppConfig() error = %v", err)
	}

	scheduler := NewScanScheduler(configRepo, &stubScheduledScanStarter{})
	if err := scheduler.Sync(context.Background()); err == nil {
		t.Fatalf("Sync() error = nil, want invalid cron error")
	}
}
