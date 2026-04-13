package service

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

func TestSnapshotServiceCreateBeforeWritesPendingSnapshot(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)
	snapshotRepo := repository.NewSnapshotRepository(database)
	svc := NewSnapshotService(fs.NewMockAdapter(), snapshotRepo, folderRepo)

	folder := &repository.Folder{
		ID:             "folder-create",
		Path:           "/library/create",
		Name:           "create",
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
		ScannedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	if err := folderRepo.Upsert(context.Background(), folder); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	snapshotID, err := svc.CreateBefore(context.Background(), "job-create", folder.ID, "move")
	if err != nil {
		t.Fatalf("CreateBefore() error = %v", err)
	}

	stored, err := snapshotRepo.GetByID(context.Background(), snapshotID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if stored.Status != "pending" {
		t.Fatalf("snapshot status = %q, want pending", stored.Status)
	}

	var before []snapshotPathState
	if err := json.Unmarshal(stored.Before, &before); err != nil {
		t.Fatalf("unmarshal before state error = %v", err)
	}

	if len(before) != 1 {
		t.Fatalf("before state len = %d, want 1", len(before))
	}

	if before[0].OriginalPath != folder.Path || before[0].CurrentPath != folder.Path {
		t.Fatalf("before[0] = %+v, want original/current path %q", before[0], folder.Path)
	}
}

func TestSnapshotServiceCreateBeforeWithStateUsesProvidedBeforePayload(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)
	snapshotRepo := repository.NewSnapshotRepository(database)
	svc := NewSnapshotService(fs.NewMockAdapter(), snapshotRepo, folderRepo)

	folder := &repository.Folder{
		ID:             "folder-create-with-state",
		Path:           "/library/create-with-state",
		Name:           "create-with-state",
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
		ScannedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	if err := folderRepo.Upsert(context.Background(), folder); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	before := json.RawMessage(`{"category":"photo","category_source":"manual"}`)
	snapshotID, err := svc.CreateBeforeWithState(context.Background(), "job-create-with-state", folder.ID, "classify", before)
	if err != nil {
		t.Fatalf("CreateBeforeWithState() error = %v", err)
	}

	stored, err := snapshotRepo.GetByID(context.Background(), snapshotID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if string(stored.Before) != string(before) {
		t.Fatalf("snapshot before = %s, want %s", string(stored.Before), string(before))
	}
}

func TestSnapshotServiceCommitAfterUpdatesAfterAndCommittedStatus(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)
	snapshotRepo := repository.NewSnapshotRepository(database)
	svc := NewSnapshotService(fs.NewMockAdapter(), snapshotRepo, folderRepo)

	folder := &repository.Folder{
		ID:             "folder-commit",
		Path:           "/library/commit",
		Name:           "commit",
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
		ScannedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	if err := folderRepo.Upsert(context.Background(), folder); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	snapshotID, err := svc.CreateBefore(context.Background(), "job-commit", folder.ID, "rename")
	if err != nil {
		t.Fatalf("CreateBefore() error = %v", err)
	}

	after := json.RawMessage(`[{"original_path":"/library/commit","current_path":"/library/commit-renamed"}]`)
	if err := svc.CommitAfter(context.Background(), snapshotID, after); err != nil {
		t.Fatalf("CommitAfter() error = %v", err)
	}

	stored, err := snapshotRepo.GetByID(context.Background(), snapshotID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}

	if string(stored.After) != string(after) {
		t.Fatalf("snapshot after = %s, want %s", string(stored.After), string(after))
	}

	if stored.Status != "committed" {
		t.Fatalf("snapshot status = %q, want committed", stored.Status)
	}
}

func TestSnapshotServiceRevertMovesPathBackUpdatesFolderAndMarksReverted(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)
	snapshotRepo := repository.NewSnapshotRepository(database)
	adapter := fs.NewMockAdapter()
	svc := NewSnapshotService(adapter, snapshotRepo, folderRepo)

	folder := &repository.Folder{
		ID:             "folder-revert",
		Path:           "/library/revert-original",
		Name:           "revert",
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
		ScannedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	if err := folderRepo.Upsert(context.Background(), folder); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	snapshotID, err := svc.CreateBefore(context.Background(), "job-revert", folder.ID, "move")
	if err != nil {
		t.Fatalf("CreateBefore() error = %v", err)
	}

	after := json.RawMessage(`[{"original_path":"/library/revert-original","current_path":"/library/revert-current"}]`)
	if err := svc.CommitAfter(context.Background(), snapshotID, after); err != nil {
		t.Fatalf("CommitAfter() error = %v", err)
	}

	if err := folderRepo.UpdatePath(context.Background(), folder.ID, "/library/revert-current", "/library", "revert-current"); err != nil {
		t.Fatalf("UpdatePath() error = %v", err)
	}

	adapter.AddDir("/library/revert-current", []fs.DirEntry{{Name: "file.jpg", IsDir: false, Size: 10}})

	if _, err := svc.Revert(context.Background(), snapshotID); err != nil {
		t.Fatalf("Revert() error = %v", err)
	}

	existsOriginal, err := adapter.Exists(context.Background(), "/library/revert-original")
	if err != nil {
		t.Fatalf("Exists(original) error = %v", err)
	}
	if !existsOriginal {
		t.Fatalf("expected original path to exist after revert")
	}

	existsCurrent, err := adapter.Exists(context.Background(), "/library/revert-current")
	if err != nil {
		t.Fatalf("Exists(current) error = %v", err)
	}
	if existsCurrent {
		t.Fatalf("expected current path to be removed after revert")
	}

	updatedFolder, err := folderRepo.GetByID(context.Background(), folder.ID)
	if err != nil {
		t.Fatalf("GetByID() error = %v", err)
	}
	if updatedFolder.Path != "/library/revert-original" {
		t.Fatalf("folder path = %q, want /library/revert-original", updatedFolder.Path)
	}

	stored, err := snapshotRepo.GetByID(context.Background(), snapshotID)
	if err != nil {
		t.Fatalf("GetByID(snapshot) error = %v", err)
	}
	if stored.Status != "reverted" {
		t.Fatalf("snapshot status = %q, want reverted", stored.Status)
	}
}

func TestSnapshotServiceRevertAlreadyRevertedReturnsError(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)
	snapshotRepo := repository.NewSnapshotRepository(database)
	svc := NewSnapshotService(fs.NewMockAdapter(), snapshotRepo, folderRepo)

	folder := &repository.Folder{
		ID:             "folder-already-reverted",
		Path:           "/library/already-reverted",
		Name:           "already-reverted",
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
		ScannedAt:      time.Now().UTC(),
		UpdatedAt:      time.Now().UTC(),
	}
	if err := folderRepo.Upsert(context.Background(), folder); err != nil {
		t.Fatalf("Upsert() error = %v", err)
	}

	snapshot := &repository.Snapshot{
		ID:            "snapshot-already-reverted",
		JobID:         "job-already-reverted",
		FolderID:      folder.ID,
		OperationType: "move",
		Before:        json.RawMessage(`[{"original_path":"/library/already-reverted","current_path":"/library/already-reverted"}]`),
		Status:        "reverted",
	}
	if err := snapshotRepo.Create(context.Background(), snapshot); err != nil {
		t.Fatalf("Create() error = %v", err)
	}

	_, err := svc.Revert(context.Background(), snapshot.ID)
	if err == nil {
		t.Fatalf("Revert() error = nil, want non-nil")
	}

	if !errors.Is(err, errSnapshotAlreadyReverted) {
		t.Fatalf("Revert() error = %v, want errSnapshotAlreadyReverted", err)
	}
}
