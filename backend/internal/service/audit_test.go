package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/liqiye/classifier/internal/repository"
)

func TestAuditServiceWriteFillsDefaultsAndPersistsRow(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	repo := repository.NewAuditRepository(database)
	svc := NewAuditService(repo)

	log := &repository.AuditLog{
		FolderPath: "/media/a",
		Action:     "scan",
		Result:     "success",
	}

	if err := svc.Write(context.Background(), log); err != nil {
		t.Fatalf("Write() error = %v", err)
	}

	if log.ID == "" {
		t.Fatalf("Write() did not fill ID")
	}

	if _, err := uuid.Parse(log.ID); err != nil {
		t.Fatalf("Write() filled invalid UUID %q: %v", log.ID, err)
	}

	if log.Level != "info" {
		t.Fatalf("Write() level = %q, want info", log.Level)
	}

	if log.CreatedAt.IsZero() {
		t.Fatalf("Write() did not fill CreatedAt")
	}

	if !log.CreatedAt.Equal(log.CreatedAt.UTC()) {
		t.Fatalf("Write() CreatedAt = %v, want UTC", log.CreatedAt)
	}

	persisted, err := repo.GetByID(context.Background(), log.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if persisted.Action != "scan" || persisted.Result != "success" || persisted.Level != "info" {
		t.Fatalf("persisted action/result/level = %q/%q/%q, want scan/success/info", persisted.Action, persisted.Result, persisted.Level)
	}

	if persisted.CreatedAt.IsZero() {
		t.Fatalf("persisted CreatedAt is zero")
	}
}

func TestAuditServiceWriteNilReturnsError(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	svc := NewAuditService(repository.NewAuditRepository(database))

	err := svc.Write(context.Background(), nil)
	if !errors.Is(err, errNilAuditLog) {
		t.Fatalf("Write(nil) error = %v, want errNilAuditLog", err)
	}
}

func TestAuditServiceListReturnsLogsAndTotal(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	repo := repository.NewAuditRepository(database)
	svc := NewAuditService(repo)

	first := &repository.AuditLog{
		ID:         "audit-1",
		FolderPath: "/media/a",
		Action:     "scan",
		Level:      "info",
		Result:     "success",
		CreatedAt:  time.Now().UTC(),
	}

	second := &repository.AuditLog{
		ID:         "audit-2",
		FolderPath: "/media/b",
		Action:     "move",
		Level:      "error",
		Result:     "failed",
		CreatedAt:  time.Now().UTC(),
	}

	if err := svc.Write(context.Background(), first); err != nil {
		t.Fatalf("Write(first) error = %v", err)
	}

	if err := svc.Write(context.Background(), second); err != nil {
		t.Fatalf("Write(second) error = %v", err)
	}

	items, total, err := svc.List(context.Background(), repository.AuditListFilter{Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}

	if total != 2 {
		t.Fatalf("List() total = %d, want 2", total)
	}

	if len(items) != 2 {
		t.Fatalf("List() len = %d, want 2", len(items))
	}
}
