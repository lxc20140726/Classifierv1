package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

type stubManifestReaderWithErr struct {
	items []*repository.FolderSourceManifest
	err   error
}

func (s *stubManifestReaderWithErr) ListLatestByFolderID(_ context.Context, _ string) ([]*repository.FolderSourceManifest, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.items, nil
}

func setupFolderTreeRouter(
	folderRepo repository.FolderRepository,
	configRepo repository.ConfigRepository,
	scheduledRepo repository.ScheduledWorkflowRepository,
	fsAdapter fs.FSAdapter,
	manifestReader FolderSourceManifestReader,
) *gin.Engine {
	h := NewFolderHandler(folderRepo, configRepo, scheduledRepo, &stubScanStarter{called: make(chan scannerCall, 1)}, fsAdapter, "/test/source", "/test/delete")
	h.SetSourceManifestReader(manifestReader)

	r := gin.New()
	r.GET("/folders/:id/classification-tree", h.GetClassificationTree)
	return r
}

func TestFolderTreeHandler_GetClassificationTree(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("success with manifest and filter", func(t *testing.T) {
		database := newHandlerTestDB(t)
		folderRepo := repository.NewFolderRepository(database)
		configRepo := repository.NewConfigRepository(database)
		scheduledRepo := repository.NewScheduledWorkflowRepository(database)
		fsAdapter := fs.NewMockAdapter()

		if err := folderRepo.Upsert(context.Background(), &repository.Folder{
			ID:             "root-1",
			Path:           "/media/root-1",
			SourceDir:      "/media",
			RelativePath:   "root-1",
			Name:           "root-1",
			Category:       "other",
			CategorySource: "auto",
			Status:         "pending",
		}); err != nil {
			t.Fatalf("folderRepo.Upsert(root) error = %v", err)
		}
		if err := folderRepo.Upsert(context.Background(), &repository.Folder{
			ID:             "child-1",
			Path:           "/media/root-1/sub-a",
			SourceDir:      "/media",
			RelativePath:   "root-1/sub-a",
			Name:           "sub-a",
			Category:       "video",
			CategorySource: "workflow",
			Status:         "done",
		}); err != nil {
			t.Fatalf("folderRepo.Upsert(child) error = %v", err)
		}

		if err := configRepo.SaveAppConfig(context.Background(), &repository.AppConfig{
			Version: 1,
			OutputDirs: repository.AppConfigOutputDirs{
				Video: []string{"/media/root-1/should-exclude"},
			},
		}); err != nil {
			t.Fatalf("configRepo.SaveAppConfig() error = %v", err)
		}

		manifestReader := &stubManifestReaderWithErr{items: []*repository.FolderSourceManifest{
			{FolderID: "root-1", SourcePath: `/media/root-1/cover.jpg`, FileName: "cover.jpg", SizeBytes: 123},
			{FolderID: "root-1", SourcePath: `/media/root-1/sub-a/video.mp4`, FileName: "video.mp4", SizeBytes: 456},
		}}

		router := setupFolderTreeRouter(folderRepo, configRepo, scheduledRepo, fsAdapter, manifestReader)
		req := httptest.NewRequest(http.MethodGet, "/folders/root-1/classification-tree", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d body=%s", resp.Code, http.StatusOK, resp.Body.String())
		}

		var payload struct {
			Data folderClassificationTreeEntry `json:"data"`
		}
		if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		if payload.Data.FolderID != "root-1" {
			t.Fatalf("folder_id = %q, want root-1", payload.Data.FolderID)
		}
		if len(payload.Data.Files) != 1 || payload.Data.Files[0].Kind != "photo" {
			t.Fatalf("root files = %#v, want single photo file", payload.Data.Files)
		}
		if len(payload.Data.Subtree) != 1 {
			t.Fatalf("subtree len = %d, want 1", len(payload.Data.Subtree))
		}
		if len(payload.Data.Subtree[0].Files) != 1 || payload.Data.Subtree[0].Files[0].Kind != "video" {
			t.Fatalf("child files = %#v, want single video file", payload.Data.Subtree[0].Files)
		}
	})

	t.Run("bad request and not found", func(t *testing.T) {
		database := newHandlerTestDB(t)
		router := setupFolderTreeRouter(
			repository.NewFolderRepository(database),
			repository.NewConfigRepository(database),
			repository.NewScheduledWorkflowRepository(database),
			fs.NewMockAdapter(),
			nil,
		)

		badReq := httptest.NewRequest(http.MethodGet, "/folders/%20/classification-tree", nil)
		badResp := httptest.NewRecorder()
		router.ServeHTTP(badResp, badReq)
		if badResp.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", badResp.Code, http.StatusBadRequest)
		}

		notFoundReq := httptest.NewRequest(http.MethodGet, "/folders/missing/classification-tree", nil)
		notFoundResp := httptest.NewRecorder()
		router.ServeHTTP(notFoundResp, notFoundReq)
		if notFoundResp.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", notFoundResp.Code, http.StatusNotFound)
		}
	})

	t.Run("manifest read error returns 500", func(t *testing.T) {
		database := newHandlerTestDB(t)
		folderRepo := repository.NewFolderRepository(database)
		if err := folderRepo.Upsert(context.Background(), &repository.Folder{
			ID: "root-2", Path: "/media/root-2", Name: "root-2", Category: "photo", CategorySource: "auto", Status: "pending",
		}); err != nil {
			t.Fatalf("folderRepo.Upsert(root-2) error = %v", err)
		}
		router := setupFolderTreeRouter(
			folderRepo,
			repository.NewConfigRepository(database),
			repository.NewScheduledWorkflowRepository(database),
			fs.NewMockAdapter(),
			&stubManifestReaderWithErr{err: errors.New("manifest down")},
		)

		req := httptest.NewRequest(http.MethodGet, "/folders/root-2/classification-tree", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", resp.Code, http.StatusInternalServerError)
		}
	})

	t.Run("fs read error returns 500", func(t *testing.T) {
		database := newHandlerTestDB(t)
		folderRepo := repository.NewFolderRepository(database)
		if err := folderRepo.Upsert(context.Background(), &repository.Folder{
			ID: "root-3", Path: "/media/root-3", Name: "root-3", Category: "photo", CategorySource: "auto", Status: "pending",
		}); err != nil {
			t.Fatalf("folderRepo.Upsert(root-3) error = %v", err)
		}

		router := setupFolderTreeRouter(
			folderRepo,
			repository.NewConfigRepository(database),
			repository.NewScheduledWorkflowRepository(database),
			fs.NewMockAdapter(),
			nil,
		)
		req := httptest.NewRequest(http.MethodGet, "/folders/root-3/classification-tree", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d body=%s", resp.Code, http.StatusInternalServerError, resp.Body.String())
		}
	})
}
