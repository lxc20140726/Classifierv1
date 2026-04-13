package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	dbpkg "github.com/liqiye/classifier/internal/db"
	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/service"
)

var snapshotHandlerDBCounter uint64

type stubSnapshotReverter struct {
	calledWith []string
	err        error
}

func (s *stubSnapshotReverter) Revert(_ context.Context, snapshotID string) (*service.RevertResult, error) {
	s.calledWith = append(s.calledWith, snapshotID)
	if s.err != nil {
		return &service.RevertResult{OK: false, ErrorMessage: s.err.Error()}, s.err
	}
	return &service.RevertResult{OK: true}, nil
}

func newSnapshotHandlerTestRepos(t *testing.T) (repository.SnapshotRepository, repository.FolderRepository) {
	t.Helper()

	id := atomic.AddUint64(&snapshotHandlerDBCounter, 1)
	dsn := fmt.Sprintf("file:classifier_snapshot_handler_%d?cache=shared&mode=memory", id)

	database, err := dbpkg.Open(dsn)
	if err != nil {
		t.Fatalf("db.Open(%q) error = %v", dsn, err)
	}

	t.Cleanup(func() {
		_ = database.Close()
	})

	return repository.NewSnapshotRepository(database), repository.NewFolderRepository(database)
}

func seedSnapshotFolder(t *testing.T, repo repository.FolderRepository, id string) {
	t.Helper()

	err := repo.Upsert(context.Background(), &repository.Folder{
		ID:             id,
		Path:           "/media/" + id,
		Name:           id,
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
	})
	if err != nil {
		t.Fatalf("seed folder Upsert(%s) error = %v", id, err)
	}
}

func seedSnapshotRecord(t *testing.T, repo repository.SnapshotRepository, snapshotID, jobID, folderID string) {
	t.Helper()

	err := repo.Create(context.Background(), &repository.Snapshot{
		ID:            snapshotID,
		JobID:         jobID,
		FolderID:      folderID,
		OperationType: "move",
		Before:        json.RawMessage(`[{"original_path":"/a","current_path":"/b"}]`),
		Status:        "pending",
	})
	if err != nil {
		t.Fatalf("seed snapshot Create(%s) error = %v", snapshotID, err)
	}
}

func setupSnapshotRouter(snapshotRepo repository.SnapshotRepository, reverter SnapshotReverter) *gin.Engine {
	g := gin.New()
	h := NewSnapshotHandler(snapshotRepo, reverter)

	g.GET("/snapshots", h.List)
	g.POST("/snapshots/:id/revert", h.Revert)

	return g
}

func TestSnapshotHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	snapshotRepo, folderRepo := newSnapshotHandlerTestRepos(t)
	reverter := &stubSnapshotReverter{}
	router := setupSnapshotRouter(snapshotRepo, reverter)

	seedSnapshotFolder(t, folderRepo, "f1")
	seedSnapshotFolder(t, folderRepo, "f2")
	seedSnapshotRecord(t, snapshotRepo, "s1", "j1", "f1")
	seedSnapshotRecord(t, snapshotRepo, "s2", "j2", "f2")
	seedSnapshotRecord(t, snapshotRepo, "s3", "j1", "f2")

	t.Run("list by folder id", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/snapshots?folder_id=f1", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}

		var payload struct {
			Data []repository.Snapshot `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}

		if len(payload.Data) != 1 {
			t.Fatalf("len(data) = %d, want 1", len(payload.Data))
		}

		if payload.Data[0].FolderID != "f1" {
			t.Fatalf("folder_id = %q, want f1", payload.Data[0].FolderID)
		}
	})

	t.Run("list by job id", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/snapshots?job_id=j1", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}

		var payload struct {
			Data []repository.Snapshot `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}

		if len(payload.Data) != 2 {
			t.Fatalf("len(data) = %d, want 2", len(payload.Data))
		}
	})

	t.Run("missing both params returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/snapshots", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("revert ok returns 200", func(t *testing.T) {
		reverter.err = nil
		reverter.calledWith = nil

		req := httptest.NewRequest(http.MethodPost, "/snapshots/s1/revert", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}

		if len(reverter.calledWith) != 1 || reverter.calledWith[0] != "s1" {
			t.Fatalf("reverter calls = %#v, want [\"s1\"]", reverter.calledWith)
		}
	})

	t.Run("revert already reverted returns 409", func(t *testing.T) {
		reverter.err = errors.New("snapshot already reverted")
		reverter.calledWith = nil

		req := httptest.NewRequest(http.MethodPost, "/snapshots/s1/revert", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusConflict {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusConflict, w.Body.String())
		}
	})
}
