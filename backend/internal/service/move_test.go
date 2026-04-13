package service

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/sse"
)

type snapshotCreateCall struct {
	jobID         string
	folderID      string
	operationType string
}

type fakeSnapshotRecorder struct {
	mu sync.Mutex

	createCalls []snapshotCreateCall
	commitCalls map[string]json.RawMessage
}

func newFakeSnapshotRecorder() *fakeSnapshotRecorder {
	return &fakeSnapshotRecorder{
		commitCalls: make(map[string]json.RawMessage),
	}
}

func (f *fakeSnapshotRecorder) CreateBefore(_ context.Context, jobID, folderID, operationType string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.createCalls = append(f.createCalls, snapshotCreateCall{
		jobID:         jobID,
		folderID:      folderID,
		operationType: operationType,
	})

	return "snapshot-" + folderID, nil
}

func (f *fakeSnapshotRecorder) CommitAfter(_ context.Context, snapshotID string, after json.RawMessage) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	cloned := make(json.RawMessage, len(after))
	copy(cloned, after)
	f.commitCalls[snapshotID] = cloned

	return nil
}

type fakeAuditWriter struct {
	mu   sync.Mutex
	logs []*repository.AuditLog
}

func (f *fakeAuditWriter) Write(_ context.Context, log *repository.AuditLog) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	cloned := *log
	f.logs = append(f.logs, &cloned)
	return nil
}

func (f *fakeAuditWriter) snapshotLogs() []*repository.AuditLog {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]*repository.AuditLog, 0, len(f.logs))
	for _, log := range f.logs {
		cloned := *log
		out = append(out, &cloned)
	}

	return out
}

func TestMoveServiceMoveFolders(t *testing.T) {
	t.Parallel()

	t.Run("happy path moves folder, commits snapshot, updates repo and emits progress done", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		database := newServiceTestDB(t)
		jobRepo := repository.NewJobRepository(database)
		folderRepo := repository.NewFolderRepository(database)
		adapter := fs.NewMockAdapter()
		broker := sse.NewBroker()
		events := broker.Subscribe()
		defer broker.Unsubscribe(events)

		snapshots := newFakeSnapshotRecorder()
		audit := &fakeAuditWriter{}

		sourcePath := "/source/album-a"
		folder := &repository.Folder{
			ID:             "folder-a",
			Path:           sourcePath,
			Name:           "album-a",
			Category:       "photo",
			CategorySource: "auto",
			Status:         "pending",
		}
		if err := folderRepo.Upsert(ctx, folder); err != nil {
			t.Fatalf("Upsert() error = %v", err)
		}

		adapter.AddDir(sourcePath, []fs.DirEntry{{Name: "x.jpg", IsDir: false}})

		if err := jobRepo.Create(ctx, &repository.Job{ID: "op-1", Type: "move", Status: "pending", FolderIDs: "[\"folder-a\"]", Total: 1}); err != nil {
			t.Fatalf("jobRepo.Create() error = %v", err)
		}

		svc := NewMoveService(adapter, jobRepo, folderRepo, snapshots, audit, broker)
		input := MoveFolderInput{
			FolderIDs: []string{folder.ID},
			TargetDir: "/target",
			JobID:     "op-1",
		}

		if err := svc.MoveFolders(ctx, input); err != nil {
			t.Fatalf("MoveFolders() error = %v", err)
		}

		updated, err := folderRepo.GetByID(ctx, folder.ID)
		if err != nil {
			t.Fatalf("GetByID() error = %v", err)
		}

		wantDst := filepath.Join(input.TargetDir, folder.Name)
		if updated.Path != wantDst {
			t.Fatalf("updated.Path = %q, want %q", updated.Path, wantDst)
		}

		if len(snapshots.createCalls) != 1 {
			t.Fatalf("CreateBefore calls = %d, want 1", len(snapshots.createCalls))
		}

		createCall := snapshots.createCalls[0]
		if createCall.jobID != input.JobID || createCall.folderID != folder.ID || createCall.operationType != "move" {
			t.Fatalf("CreateBefore call = %+v, want job=%q folder=%q op=move", createCall, input.JobID, folder.ID)
		}

		afterJSON, ok := snapshots.commitCalls["snapshot-"+folder.ID]
		if !ok {
			t.Fatalf("CommitAfter() missing snapshot id %q", "snapshot-"+folder.ID)
		}

		var after []map[string]string
		if err := json.Unmarshal(afterJSON, &after); err != nil {
			t.Fatalf("Unmarshal(afterJSON) error = %v", err)
		}
		if len(after) != 1 || after[0]["original_path"] != sourcePath || after[0]["current_path"] != wantDst {
			t.Fatalf("after payload = %+v, want original=%q current=%q", after, sourcePath, wantDst)
		}

		logs := audit.snapshotLogs()
		if len(logs) != 1 {
			t.Fatalf("audit logs len = %d, want 1", len(logs))
		}
		if logs[0].Action != "move" || logs[0].Result != "success" || logs[0].FolderPath != wantDst {
			t.Fatalf("audit success log = %+v", *logs[0])
		}

		received := collectEvents(t, events, 2)
		if received[0].Type != "job.progress" {
			t.Fatalf("event[0].Type = %q, want job.progress", received[0].Type)
		}
		if received[1].Type != "job.done" {
			t.Fatalf("event[1].Type = %q, want job.done", received[1].Type)
		}

		progressPayload := decodeEventPayload(t, received[0])
		if progressPayload["job_id"] != input.JobID || progressPayload["folder_id"] != folder.ID {
			t.Fatalf("progress payload ids = %+v", progressPayload)
		}
		if progressPayload["done"] != float64(1) || progressPayload["total"] != float64(1) {
			t.Fatalf("progress done/total = %+v", progressPayload)
		}

		job, err := jobRepo.GetByID(ctx, input.JobID)
		if err != nil {
			t.Fatalf("jobRepo.GetByID() error = %v", err)
		}
		if job.Status != "succeeded" || job.Done != 1 || job.Failed != 0 {
			t.Fatalf("job state = %+v", *job)
		}
	})

	t.Run("MoveDir failure writes failed audit, emits job.error, and continues", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		database := newServiceTestDB(t)
		jobRepo := repository.NewJobRepository(database)
		folderRepo := repository.NewFolderRepository(database)
		adapter := fs.NewMockAdapter()
		broker := sse.NewBroker()
		events := broker.Subscribe()
		defer broker.Unsubscribe(events)

		snapshots := newFakeSnapshotRecorder()
		audit := &fakeAuditWriter{}

		failedFolder := &repository.Folder{ID: "f1", Path: "/src/missing", Name: "missing", Category: "other", CategorySource: "auto", Status: "pending"}
		successFolder := &repository.Folder{ID: "f2", Path: "/src/ok", Name: "ok", Category: "other", CategorySource: "auto", Status: "pending"}

		if err := folderRepo.Upsert(ctx, failedFolder); err != nil {
			t.Fatalf("Upsert(failedFolder) error = %v", err)
		}
		if err := folderRepo.Upsert(ctx, successFolder); err != nil {
			t.Fatalf("Upsert(successFolder) error = %v", err)
		}

		adapter.AddDir(successFolder.Path, []fs.DirEntry{{Name: "item.txt", IsDir: false}})

		if err := jobRepo.Create(ctx, &repository.Job{ID: "op-2", Type: "move", Status: "pending", FolderIDs: "[\"f1\",\"f2\"]", Total: 2}); err != nil {
			t.Fatalf("jobRepo.Create() error = %v", err)
		}

		svc := NewMoveService(adapter, jobRepo, folderRepo, snapshots, audit, broker)
		input := MoveFolderInput{
			FolderIDs: []string{failedFolder.ID, successFolder.ID},
			TargetDir: "/target",
			JobID:     "op-2",
		}

		if err := svc.MoveFolders(ctx, input); err != nil {
			t.Fatalf("MoveFolders() error = %v, want nil", err)
		}

		gotFailed, err := folderRepo.GetByID(ctx, failedFolder.ID)
		if err != nil {
			t.Fatalf("GetByID(failed) error = %v", err)
		}
		if gotFailed.Path != failedFolder.Path {
			t.Fatalf("failed folder path = %q, want unchanged %q", gotFailed.Path, failedFolder.Path)
		}

		gotSuccess, err := folderRepo.GetByID(ctx, successFolder.ID)
		if err != nil {
			t.Fatalf("GetByID(success) error = %v", err)
		}
		wantSuccessDst := filepath.Join(input.TargetDir, successFolder.Name)
		if gotSuccess.Path != wantSuccessDst {
			t.Fatalf("success folder path = %q, want %q", gotSuccess.Path, wantSuccessDst)
		}

		if len(snapshots.createCalls) != 2 {
			t.Fatalf("CreateBefore calls = %d, want 2", len(snapshots.createCalls))
		}
		if len(snapshots.commitCalls) != 1 {
			t.Fatalf("CommitAfter calls = %d, want 1", len(snapshots.commitCalls))
		}

		logs := audit.snapshotLogs()
		if len(logs) != 2 {
			t.Fatalf("audit logs len = %d, want 2", len(logs))
		}

		var failedLog *repository.AuditLog
		var successLog *repository.AuditLog
		for _, log := range logs {
			switch {
			case log.FolderID == failedFolder.ID && log.Result == "failed":
				failedLog = log
			case log.FolderID == successFolder.ID && log.Result == "success":
				successLog = log
			}
		}

		if failedLog == nil {
			t.Fatalf("missing failed audit log for folder %q", failedFolder.ID)
		}
		if failedLog.ErrorMsg == "" {
			t.Fatalf("failed audit log error msg is empty")
		}
		if successLog == nil {
			t.Fatalf("missing success audit log for folder %q", successFolder.ID)
		}

		received := collectEvents(t, events, 3)
		typeCounts := map[string]int{}
		for _, evt := range received {
			typeCounts[evt.Type]++
		}

		if typeCounts["job.error"] != 1 {
			t.Fatalf("job.error count = %d, want 1", typeCounts["job.error"])
		}
		if typeCounts["job.progress"] != 1 {
			t.Fatalf("job.progress count = %d, want 1", typeCounts["job.progress"])
		}
		if typeCounts["job.done"] != 1 {
			t.Fatalf("job.done count = %d, want 1", typeCounts["job.done"])
		}

		for _, evt := range received {
			if evt.Type != "job.error" {
				continue
			}

			errorPayload := decodeEventPayload(t, evt)
			if errorPayload["folder_id"] != failedFolder.ID {
				t.Fatalf("job.error folder_id = %v, want %q", errorPayload["folder_id"], failedFolder.ID)
			}
		}

		job, err := jobRepo.GetByID(ctx, input.JobID)
		if err != nil {
			t.Fatalf("jobRepo.GetByID() error = %v", err)
		}
		if job.Status != "partial" || job.Done != 1 || job.Failed != 1 {
			t.Fatalf("job state = %+v", *job)
		}
	})

	t.Run("empty target dir returns error immediately", func(t *testing.T) {
		t.Parallel()

		ctx := context.Background()
		database := newServiceTestDB(t)
		jobRepo := repository.NewJobRepository(database)
		folderRepo := repository.NewFolderRepository(database)
		adapter := fs.NewMockAdapter()
		broker := sse.NewBroker()
		events := broker.Subscribe()
		defer broker.Unsubscribe(events)

		snapshots := newFakeSnapshotRecorder()
		audit := &fakeAuditWriter{}

		svc := NewMoveService(adapter, jobRepo, folderRepo, snapshots, audit, broker)
		err := svc.MoveFolders(ctx, MoveFolderInput{
			FolderIDs: []string{"f1"},
			TargetDir: "",
			JobID:     "op-invalid",
		})
		if err == nil {
			t.Fatal("MoveFolders() error = nil, want non-nil")
		}

		if len(snapshots.createCalls) != 0 {
			t.Fatalf("CreateBefore calls = %d, want 0", len(snapshots.createCalls))
		}

		if len(audit.snapshotLogs()) != 0 {
			t.Fatalf("audit logs len = %d, want 0", len(audit.snapshotLogs()))
		}

		select {
		case evt := <-events:
			t.Fatalf("unexpected event type = %q", evt.Type)
		default:
		}
	})
}

func collectEvents(t *testing.T, ch chan sse.Event, want int) []sse.Event {
	t.Helper()

	out := make([]sse.Event, 0, want)
	deadline := time.After(1 * time.Second)

	for len(out) < want {
		select {
		case evt := <-ch:
			out = append(out, evt)
		case <-deadline:
			t.Fatalf("timed out collecting %d events, got %d", want, len(out))
		}
	}

	return out
}

func decodeEventPayload(t *testing.T, evt sse.Event) map[string]any {
	t.Helper()

	payload := make(map[string]any)
	if err := json.Unmarshal(evt.Data, &payload); err != nil {
		t.Fatalf("json.Unmarshal(event payload) error = %v", err)
	}

	return payload
}
