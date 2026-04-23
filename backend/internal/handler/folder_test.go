package handler

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	dbpkg "github.com/liqiye/classifier/internal/db"
	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/service"
)

var folderHandlerDBCounter uint64

type scannerCall struct {
	sourceDirs []string
}

type stubScanStarter struct {
	called chan scannerCall
}

type stubOutputValidator struct {
	canMarkDone bool
}

type stubSourceManifestReader struct {
	byFolderID map[string][]*repository.FolderSourceManifest
}

func (s *stubScanStarter) StartJob(_ context.Context, sourceDirs []string) (string, error) {
	s.called <- scannerCall{sourceDirs: sourceDirs}
	return "scan-job-1", nil
}

func (s *stubSourceManifestReader) ListLatestByFolderID(_ context.Context, folderID string) ([]*repository.FolderSourceManifest, error) {
	if s == nil || s.byFolderID == nil {
		return nil, nil
	}
	return s.byFolderID[folderID], nil
}

func (s *stubOutputValidator) ValidateWorkflowRun(_ context.Context, _ string) ([]*repository.FolderOutputCheck, error) {
	return nil, nil
}

func (s *stubOutputValidator) ValidateFolder(_ context.Context, _ string) (*repository.FolderOutputCheck, error) {
	return nil, nil
}

func (s *stubOutputValidator) GetLatestDetail(_ context.Context, folderID string) (*service.FolderOutputCheckDetail, error) {
	return &service.FolderOutputCheckDetail{FolderID: folderID}, nil
}

func (s *stubOutputValidator) CanMarkDone(_ context.Context, _ string) (bool, error) {
	return s.canMarkDone, nil
}

func newHandlerTestDB(t *testing.T) *sql.DB {
	t.Helper()

	id := atomic.AddUint64(&folderHandlerDBCounter, 1)
	dsn := fmt.Sprintf("file:classifier_handler_%d?cache=shared&mode=memory", id)

	database, err := dbpkg.Open(dsn)
	if err != nil {
		t.Fatalf("db.Open(%q) error = %v", dsn, err)
	}

	t.Cleanup(func() {
		_ = database.Close()
	})

	return database
}

func seedFolder(t *testing.T, repo repository.FolderRepository, folder *repository.Folder) {
	t.Helper()

	if err := repo.Upsert(context.Background(), folder); err != nil {
		t.Fatalf("seed Upsert(%s) error = %v", folder.ID, err)
	}
}

func setupRouter(folderRepo repository.FolderRepository, configRepo repository.ConfigRepository, scheduledRepo repository.ScheduledWorkflowRepository, starter FolderScanStarter, fsAdapter fs.FSAdapter) *gin.Engine {
	g := gin.New()
	h := NewFolderHandler(folderRepo, configRepo, scheduledRepo, starter, fsAdapter, "/test/source", "/test/delete-staging")

	g.GET("/folders", h.List)
	g.GET("/folders/:id", h.Get)
	g.GET("/folders/:id/classification-tree", h.GetClassificationTree)
	g.POST("/folders/scan", h.Scan)
	g.POST("/folders/:id/restore", h.Restore)
	g.PATCH("/folders/:id/category", h.UpdateCategory)
	g.PATCH("/folders/:id/status", h.UpdateStatus)
	g.DELETE("/folders/:id", h.Delete)

	return g
}

func TestFolderHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	database := newHandlerTestDB(t)
	repo := repository.NewFolderRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	configRepo := repository.NewConfigRepository(database)
	scheduledRepo := repository.NewScheduledWorkflowRepository(database)
	starter := &stubScanStarter{called: make(chan scannerCall, 1)}
	fsAdapter := fs.NewMockAdapter()
	router := setupRouter(repo, configRepo, scheduledRepo, starter, fsAdapter)
	manifestReader := &stubSourceManifestReader{byFolderID: map[string][]*repository.FolderSourceManifest{}}

	seedFolder(t, repo, &repository.Folder{
		ID:             "f1",
		Path:           "/media/f1",
		Name:           "f1",
		Category:       "photo",
		CategorySource: "auto",
		Status:         "pending",
	})

	if err := workflowRunRepo.Create(context.Background(), &repository.WorkflowRun{
		ID:            "wr-f1-classification",
		JobID:         "job-f1-classification",
		FolderID:      "f1",
		WorkflowDefID: "def-classification",
		Status:        "succeeded",
	}); err != nil {
		t.Fatalf("workflowRunRepo.Create(f1 classification) error = %v", err)
	}
	if err := nodeRunRepo.Create(context.Background(), &repository.NodeRun{
		ID:            "nr-f1-writer",
		WorkflowRunID: "wr-f1-classification",
		NodeID:        "writer",
		NodeType:      "classification-writer",
		Sequence:      1,
		Status:        "succeeded",
	}); err != nil {
		t.Fatalf("nodeRunRepo.Create(f1 writer) error = %v", err)
	}
	if err := workflowRunRepo.Create(context.Background(), &repository.WorkflowRun{
		ID:            "wr-f2-processing",
		JobID:         "job-f2-processing",
		FolderID:      "f2",
		WorkflowDefID: "def-processing",
		Status:        "failed",
	}); err != nil {
		t.Fatalf("workflowRunRepo.Create(f2 processing) error = %v", err)
	}
	if err := nodeRunRepo.Create(context.Background(), &repository.NodeRun{
		ID:            "nr-f2-move",
		WorkflowRunID: "wr-f2-processing",
		NodeID:        "move",
		NodeType:      "move-node",
		Sequence:      1,
		Status:        "failed",
	}); err != nil {
		t.Fatalf("nodeRunRepo.Create(f2 move) error = %v", err)
	}
	seedFolder(t, repo, &repository.Folder{
		ID:             "f2",
		Path:           "/media/f2",
		Name:           "f2",
		Category:       "video",
		CategorySource: "auto",
		Status:         "done",
	})
	fsAdapter.AddDir("/media/f2", []fs.DirEntry{{Name: "a.txt", IsDir: false}})

	t.Run("list folders", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/folders?page=1&limit=10", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}

		var payload struct {
			Data  []repository.Folder `json:"data"`
			Total int                 `json:"total"`
			Page  int                 `json:"page"`
			Limit int                 `json:"limit"`
		}

		if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}

		if payload.Total != 2 {
			t.Fatalf("total = %d, want 2", payload.Total)
		}

		if len(payload.Data) != 2 {
			t.Fatalf("len(data) = %d, want 2", len(payload.Data))
		}
		var gotF1 *repository.Folder
		var gotF2 *repository.Folder
		for i := range payload.Data {
			if payload.Data[i].ID == "f1" {
				gotF1 = &payload.Data[i]
			}
			if payload.Data[i].ID == "f2" {
				gotF2 = &payload.Data[i]
			}
		}
		if gotF1 == nil || gotF2 == nil {
			t.Fatalf("list result missing f1 or f2")
		}
		if gotF1.WorkflowSummary.Classification.Status != "succeeded" {
			t.Fatalf("f1 classification status = %q, want succeeded", gotF1.WorkflowSummary.Classification.Status)
		}
		if gotF1.WorkflowSummary.Processing.Status != "not_run" {
			t.Fatalf("f1 processing status = %q, want not_run", gotF1.WorkflowSummary.Processing.Status)
		}
		if gotF2.WorkflowSummary.Classification.Status != "not_run" {
			t.Fatalf("f2 classification status = %q, want not_run", gotF2.WorkflowSummary.Classification.Status)
		}
		if gotF2.WorkflowSummary.Processing.Status != "failed" {
			t.Fatalf("f2 processing status = %q, want failed", gotF2.WorkflowSummary.Processing.Status)
		}

		if payload.Page != 1 || payload.Limit != 10 {
			t.Fatalf("page/limit = %d/%d, want 1/10", payload.Page, payload.Limit)
		}
	})

	t.Run("get folder by id", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/folders/f1", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}

		var payload struct {
			Data repository.Folder `json:"data"`
		}

		if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}

		if payload.Data.ID != "f1" {
			t.Fatalf("id = %q, want f1", payload.Data.ID)
		}
		if payload.Data.WorkflowSummary.Classification.Status != "succeeded" {
			t.Fatalf("classification status = %q, want succeeded", payload.Data.WorkflowSummary.Classification.Status)
		}
		if payload.Data.WorkflowSummary.Processing.Status != "not_run" {
			t.Fatalf("processing status = %q, want not_run", payload.Data.WorkflowSummary.Processing.Status)
		}
	})

	t.Run("get folder classification tree", func(t *testing.T) {
		seedFolder(t, repo, &repository.Folder{
			ID:             "f1-child",
			Path:           "/media/f1/sub-a",
			SourceDir:      "/media",
			RelativePath:   "f1/sub-a",
			Name:           "sub-a",
			Category:       "video",
			CategorySource: "workflow",
			Status:         "pending",
		})
		fsAdapter.AddDir("/media/f1", []fs.DirEntry{
			{Name: "cover.jpg", IsDir: false, Size: 12},
			{Name: "sub-a", IsDir: true},
		})
		fsAdapter.AddDir("/media/f1/sub-a", []fs.DirEntry{
			{Name: "clip.mp4", IsDir: false, Size: 34},
		})

		req := httptest.NewRequest(http.MethodGet, "/folders/f1/classification-tree", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}

		var payload struct {
			Data struct {
				FolderID string `json:"folder_id"`
				Path     string `json:"path"`
				Category string `json:"category"`
				Files    []struct {
					Name string `json:"name"`
					Kind string `json:"kind"`
				} `json:"files"`
				Subtree []struct {
					FolderID string `json:"folder_id"`
					Path     string `json:"path"`
					Category string `json:"category"`
					Files    []struct {
						Name string `json:"name"`
						Kind string `json:"kind"`
					} `json:"files"`
				} `json:"subtree"`
			} `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}

		if payload.Data.FolderID != "f1" {
			t.Fatalf("root folder_id = %q, want f1", payload.Data.FolderID)
		}
		if payload.Data.Path != "/media/f1" {
			t.Fatalf("root path = %q, want /media/f1", payload.Data.Path)
		}
		if payload.Data.Category != "photo" {
			t.Fatalf("root category = %q, want photo", payload.Data.Category)
		}
		if len(payload.Data.Files) != 1 || payload.Data.Files[0].Name != "cover.jpg" || payload.Data.Files[0].Kind != "photo" {
			t.Fatalf("root files = %#v, want cover.jpg/photo", payload.Data.Files)
		}
		if len(payload.Data.Subtree) != 1 {
			t.Fatalf("len(subtree) = %d, want 1", len(payload.Data.Subtree))
		}
		child := payload.Data.Subtree[0]
		if child.FolderID != "f1-child" {
			t.Fatalf("child folder_id = %q, want f1-child", child.FolderID)
		}
		if child.Path != "/media/f1/sub-a" {
			t.Fatalf("child path = %q, want /media/f1/sub-a", child.Path)
		}
		if child.Category != "video" {
			t.Fatalf("child category = %q, want video", child.Category)
		}
		if len(child.Files) != 1 || child.Files[0].Name != "clip.mp4" || child.Files[0].Kind != "video" {
			t.Fatalf("child files = %#v, want clip.mp4/video", child.Files)
		}
	})

	t.Run("classification tree prefers source manifest files and sizes", func(t *testing.T) {
		handler := NewFolderHandler(repo, configRepo, scheduledRepo, starter, fsAdapter, "/test/source", "/test/delete-staging")
		handler.SetSourceManifestReader(manifestReader)

		manifestRouter := gin.New()
		manifestRouter.GET("/folders/:id/classification-tree", handler.GetClassificationTree)

		fsAdapter.AddDir("/media/f1", []fs.DirEntry{
			{Name: "cover.jpg", IsDir: false, Size: 0},
			{Name: "processed.jpg", IsDir: false, Size: 0},
		})
		manifestReader.byFolderID["f1"] = []*repository.FolderSourceManifest{
			{
				FolderID:     "f1",
				SourcePath:   "/media/f1/cover.jpg",
				RelativePath: "cover.jpg",
				FileName:     "cover.jpg",
				SizeBytes:    123,
			},
		}

		req := httptest.NewRequest(http.MethodGet, "/folders/f1/classification-tree", nil)
		w := httptest.NewRecorder()
		manifestRouter.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}

		var payload struct {
			Data struct {
				Files []struct {
					Name      string `json:"name"`
					SizeBytes int64  `json:"size_bytes"`
				} `json:"files"`
			} `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}

		if len(payload.Data.Files) != 1 {
			t.Fatalf("len(files) = %d, want 1", len(payload.Data.Files))
		}
		if payload.Data.Files[0].Name != "cover.jpg" {
			t.Fatalf("file name = %q, want cover.jpg", payload.Data.Files[0].Name)
		}
		if payload.Data.Files[0].SizeBytes != 123 {
			t.Fatalf("file size = %d, want 123", payload.Data.Files[0].SizeBytes)
		}
	})

	t.Run("classification tree stats files when readdir size is zero", func(t *testing.T) {
		handler := NewFolderHandler(repo, configRepo, scheduledRepo, starter, fsAdapter, "/test/source", "/test/delete-staging")

		statRouter := gin.New()
		statRouter.GET("/folders/:id/classification-tree", handler.GetClassificationTree)

		fsAdapter.AddDir("/media/f1", []fs.DirEntry{{Name: "stat-size.jpg", IsDir: false, Size: 0}})
		fsAdapter.AddFile("/media/f1/stat-size.jpg", []byte("123456789"))

		req := httptest.NewRequest(http.MethodGet, "/folders/f1/classification-tree", nil)
		w := httptest.NewRecorder()
		statRouter.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}

		var payload struct {
			Data struct {
				Files []struct {
					Name      string `json:"name"`
					SizeBytes int64  `json:"size_bytes"`
				} `json:"files"`
			} `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}

		found := false
		for _, file := range payload.Data.Files {
			if file.Name != "stat-size.jpg" {
				continue
			}
			found = true
			if file.SizeBytes != 9 {
				t.Fatalf("size_bytes = %d, want 9", file.SizeBytes)
			}
		}
		if !found {
			t.Fatalf("stat-size.jpg not found in files = %#v", payload.Data.Files)
		}
	})

	t.Run("classification tree hides configured output subdirectories", func(t *testing.T) {
		handler := NewFolderHandler(repo, configRepo, scheduledRepo, starter, fsAdapter, "/test/source", "/test/delete-staging")

		appConfig, err := configRepo.GetAppConfig(context.Background())
		if err != nil {
			t.Fatalf("configRepo.GetAppConfig() error = %v", err)
		}
		appConfig.OutputDirs.Mixed = []string{"/media/f1/output"}
		if err := configRepo.SaveAppConfig(context.Background(), appConfig); err != nil {
			t.Fatalf("configRepo.SaveAppConfig() error = %v", err)
		}

		seedFolder(t, repo, &repository.Folder{
			ID:             "f1-output",
			Path:           "/media/f1/output",
			SourceDir:      "/media",
			RelativePath:   "f1/output",
			Name:           "output",
			Category:       "mixed",
			CategorySource: "workflow",
			Status:         "pending",
		})

		filterRouter := gin.New()
		filterRouter.GET("/folders/:id/classification-tree", handler.GetClassificationTree)

		req := httptest.NewRequest(http.MethodGet, "/folders/f1/classification-tree", nil)
		w := httptest.NewRecorder()
		filterRouter.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}

		var payload struct {
			Data struct {
				Subtree []struct {
					FolderID string `json:"folder_id"`
					Path     string `json:"path"`
				} `json:"subtree"`
			} `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}

		for _, child := range payload.Data.Subtree {
			if child.Path == "/media/f1/output" || child.FolderID == "f1-output" {
				t.Fatalf("excluded output subtree still present: %#v", payload.Data.Subtree)
			}
		}
	})

	t.Run("scan prefers enabled scan schedules source_dirs", func(t *testing.T) {
		if err := scheduledRepo.Create(context.Background(), &repository.ScheduledWorkflow{
			ID:         "scan-enabled-1",
			Name:       "scan-1",
			JobType:    "scan",
			SourceDirs: `["/cron/source","/cron/source-2","/cron/source"]`,
			CronSpec:   "0 * * * *",
			Enabled:    true,
		}); err != nil {
			t.Fatalf("scheduledRepo.Create() error = %v", err)
		}
		if err := scheduledRepo.Create(context.Background(), &repository.ScheduledWorkflow{
			ID:         "scan-disabled-1",
			Name:       "scan-disabled",
			JobType:    "scan",
			SourceDirs: `["/cron/disabled"]`,
			CronSpec:   "0 * * * *",
			Enabled:    false,
		}); err != nil {
			t.Fatalf("scheduledRepo.Create() error = %v", err)
		}
		if err := scheduledRepo.Create(context.Background(), &repository.ScheduledWorkflow{
			ID:         "workflow-enabled-1",
			Name:       "workflow",
			JobType:    "workflow",
			SourceDirs: `["/cron/workflow"]`,
			CronSpec:   "0 * * * *",
			Enabled:    true,
		}); err != nil {
			t.Fatalf("scheduledRepo.Create() error = %v", err)
		}

		appConfig, err := configRepo.GetAppConfig(context.Background())
		if err != nil {
			t.Fatalf("configRepo.GetAppConfig() error = %v", err)
		}
		appConfig.ScanInputDirs = []string{"/task/source"}
		if err := configRepo.SaveAppConfig(context.Background(), appConfig); err != nil {
			t.Fatalf("configRepo.SaveAppConfig() error = %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/folders/scan", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusAccepted)
		}

		call := <-starter.called
		if len(call.sourceDirs) != 2 || call.sourceDirs[0] != "/cron/source" || call.sourceDirs[1] != "/cron/source-2" {
			t.Fatalf("sourceDirs = %#v, want schedule source_dirs", call.sourceDirs)
		}
	})

	t.Run("scan falls back to scan_input_dirs when no scan schedule", func(t *testing.T) {
		if err := scheduledRepo.Delete(context.Background(), "scan-enabled-1"); err != nil {
			t.Fatalf("scheduledRepo.Delete(scan-enabled-1) error = %v", err)
		}
		if err := scheduledRepo.Delete(context.Background(), "workflow-enabled-1"); err != nil {
			t.Fatalf("scheduledRepo.Delete(workflow-enabled-1) error = %v", err)
		}

		appConfig, err := configRepo.GetAppConfig(context.Background())
		if err != nil {
			t.Fatalf("configRepo.GetAppConfig() error = %v", err)
		}
		appConfig.ScanInputDirs = []string{"/scan/a", "/scan/b"}
		if err := configRepo.SaveAppConfig(context.Background(), appConfig); err != nil {
			t.Fatalf("configRepo.SaveAppConfig() error = %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/folders/scan", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusAccepted)
		}

		call := <-starter.called
		if len(call.sourceDirs) != 2 || call.sourceDirs[0] != "/scan/a" || call.sourceDirs[1] != "/scan/b" {
			t.Fatalf("sourceDirs = %#v, want scan_input_dirs", call.sourceDirs)
		}
	})

	t.Run("scan falls back to default source dir when scan_input_dirs empty", func(t *testing.T) {
		appConfig, err := configRepo.GetAppConfig(context.Background())
		if err != nil {
			t.Fatalf("configRepo.GetAppConfig() error = %v", err)
		}
		appConfig.ScanInputDirs = []string{}
		if err := configRepo.SaveAppConfig(context.Background(), appConfig); err != nil {
			t.Fatalf("configRepo.SaveAppConfig() error = %v", err)
		}

		req := httptest.NewRequest(http.MethodPost, "/folders/scan", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusAccepted)
		}

		call := <-starter.called
		if len(call.sourceDirs) != 1 || call.sourceDirs[0] != "/test/source" {
			t.Fatalf("sourceDirs = %#v, want default source dir fallback", call.sourceDirs)
		}
	})

	t.Run("update category valid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPatch, "/folders/f1/category", bytes.NewBufferString(`{"category":"manga"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}

		folder, err := repo.GetByID(context.Background(), "f1")
		if err != nil {
			t.Fatalf("repo.GetByID() error = %v", err)
		}

		if folder.Category != "manga" {
			t.Fatalf("category = %q, want manga", folder.Category)
		}

		if folder.CategorySource != "manual" {
			t.Fatalf("category_source = %q, want manual", folder.CategorySource)
		}
	})

	t.Run("update category invalid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPatch, "/folders/f1/category", bytes.NewBufferString(`{"category":"unknown"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("update status valid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPatch, "/folders/f1/status", bytes.NewBufferString(`{"status":"skip"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}

		folder, err := repo.GetByID(context.Background(), "f1")
		if err != nil {
			t.Fatalf("repo.GetByID() error = %v", err)
		}

		if folder.Status != "skip" {
			t.Fatalf("status = %q, want skip", folder.Status)
		}
	})

	t.Run("update status invalid", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPatch, "/folders/f1/status", bytes.NewBufferString(`{"status":"unknown"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("delete existing", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodDelete, "/folders/f2", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
		}

		folder, err := repo.GetByID(context.Background(), "f2")
		if err != nil {
			t.Fatalf("repo.GetByID() error = %v", err)
		}
		if folder.DeletedAt == nil {
			t.Fatalf("expected folder to be suppressed")
		}
		if folder.Path != "/media/f2" {
			t.Fatalf("folder.Path = %q, want original path preserved", folder.Path)
		}
		exists, err := fsAdapter.Exists(context.Background(), "/media/f2")
		if err != nil {
			t.Fatalf("fsAdapter.Exists() error = %v", err)
		}
		if !exists {
			t.Fatalf("expected actual folder to remain on filesystem")
		}
	})

	t.Run("get missing returns 404", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/folders/missing", nil)
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusNotFound)
		}
	})
}

func TestFolderHandlerListTopLevelAggregatesWorkflowOutputsIntoSourceFolder(t *testing.T) {
	gin.SetMode(gin.TestMode)

	database := newHandlerTestDB(t)
	repo := repository.NewFolderRepository(database)
	workflowRunRepo := repository.NewWorkflowRunRepository(database)
	nodeRunRepo := repository.NewNodeRunRepository(database)
	configRepo := repository.NewConfigRepository(database)
	scheduledRepo := repository.NewScheduledWorkflowRepository(database)
	router := setupRouter(repo, configRepo, scheduledRepo, nil, fs.NewMockAdapter())

	ctx := context.Background()

	seedFolder(t, repo, &repository.Folder{
		ID:             "agg-root",
		Path:           "/source/album",
		SourceDir:      "/source",
		RelativePath:   "album",
		Name:           "album",
		Category:       "mixed",
		CategorySource: "auto",
		Status:         "pending",
	})
	seedFolder(t, repo, &repository.Folder{
		ID:             "agg-video",
		Path:           "/source/album/Videos",
		SourceDir:      "/source",
		RelativePath:   "album",
		Name:           "Videos",
		Category:       "video",
		CategorySource: "workflow",
		Status:         "done",
	})
	if err := repo.UpdatePath(ctx, "agg-video", "/target/video/album", "/target/video", "album"); err != nil {
		t.Fatalf("UpdatePath(agg-video) error = %v", err)
	}

	seedFolder(t, repo, &repository.Folder{
		ID:             "agg-unrelated",
		Path:           "/other-source/album",
		SourceDir:      "/other-source",
		RelativePath:   "album",
		Name:           "album",
		Category:       "video",
		CategorySource: "workflow",
		Status:         "done",
	})
	if err := repo.UpdatePath(ctx, "agg-unrelated", "/target/other/album", "/target/other", "album"); err != nil {
		t.Fatalf("UpdatePath(agg-unrelated) error = %v", err)
	}

	for _, run := range []*repository.WorkflowRun{
		{
			ID:            "wr-agg-video",
			JobID:         "job-agg-video",
			FolderID:      "agg-video",
			WorkflowDefID: "def-processing",
			Status:        "succeeded",
		},
		{
			ID:            "wr-agg-unrelated",
			JobID:         "job-agg-unrelated",
			FolderID:      "agg-unrelated",
			WorkflowDefID: "def-processing",
			Status:        "failed",
		},
	} {
		if err := workflowRunRepo.Create(ctx, run); err != nil {
			t.Fatalf("workflowRunRepo.Create(%s) error = %v", run.ID, err)
		}
	}

	for _, run := range []*repository.NodeRun{
		{
			ID:            "nr-agg-video-move",
			WorkflowRunID: "wr-agg-video",
			NodeID:        "move",
			NodeType:      "move-node",
			Sequence:      1,
			Status:        "succeeded",
		},
		{
			ID:            "nr-agg-unrelated-move",
			WorkflowRunID: "wr-agg-unrelated",
			NodeID:        "move",
			NodeType:      "move-node",
			Sequence:      1,
			Status:        "failed",
		},
	} {
		if err := nodeRunRepo.Create(ctx, run); err != nil {
			t.Fatalf("nodeRunRepo.Create(%s) error = %v", run.ID, err)
		}
	}

	req := httptest.NewRequest(http.MethodGet, "/folders?top_level_only=true&page=1&limit=10", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
	}

	var payload struct {
		Data  []repository.Folder `json:"data"`
		Total int                 `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if payload.Total != 2 {
		t.Fatalf("total = %d, want 2", payload.Total)
	}
	if len(payload.Data) != 2 {
		t.Fatalf("len(data) = %d, want 2", len(payload.Data))
	}

	foundRoot := false
	foundMovedChild := false
	foundUnrelated := false
	for _, folder := range payload.Data {
		switch folder.ID {
		case "agg-root":
			foundRoot = true
			if folder.Name != "album" {
				t.Fatalf("agg-root name = %q, want album", folder.Name)
			}
			if folder.WorkflowSummary.Processing.Status != "succeeded" {
				t.Fatalf("agg-root processing status = %q, want succeeded", folder.WorkflowSummary.Processing.Status)
			}
		case "agg-video":
			foundMovedChild = true
		case "agg-unrelated":
			foundUnrelated = true
			if folder.WorkflowSummary.Processing.Status != "failed" {
				t.Fatalf("agg-unrelated processing status = %q, want failed", folder.WorkflowSummary.Processing.Status)
			}
		}
	}

	if !foundRoot {
		t.Fatalf("expected aggregated source folder agg-root in response")
	}
	if foundMovedChild {
		t.Fatalf("moved workflow folder agg-video should be hidden after aggregation")
	}
	if !foundUnrelated {
		t.Fatalf("unrelated same-name workflow folder should remain visible")
	}
}

func TestFolderHandlerListTopLevelHidesPromotedWorkflowChildGhosts(t *testing.T) {
	gin.SetMode(gin.TestMode)

	database := newHandlerTestDB(t)
	repo := repository.NewFolderRepository(database)
	configRepo := repository.NewConfigRepository(database)
	scheduledRepo := repository.NewScheduledWorkflowRepository(database)
	router := setupRouter(repo, configRepo, scheduledRepo, nil, fs.NewMockAdapter())

	ctx := context.Background()
	for _, folder := range []*repository.Folder{
		{
			ID:             "ghost-video",
			Path:           "/source/album/video",
			SourceDir:      "/source",
			RelativePath:   "album/video",
			Name:           "video",
			Category:       "video",
			CategorySource: "workflow",
			Status:         "pending",
			VideoCount:     1,
			TotalFiles:     1,
		},
		{
			ID:             "ghost-photo",
			Path:           "/source/album/photo",
			SourceDir:      "/source",
			RelativePath:   "album/photo",
			Name:           "photo",
			Category:       "photo",
			CategorySource: "workflow",
			Status:         "pending",
			ImageCount:     1,
			TotalFiles:     1,
		},
		{
			ID:             "legit-workflow-root",
			Path:           "/workflow-only/legit",
			SourceDir:      "/workflow-only",
			RelativePath:   "legit",
			Name:           "legit",
			Category:       "video",
			CategorySource: "workflow",
			Status:         "pending",
			VideoCount:     1,
			TotalFiles:     1,
		},
	} {
		seedFolder(t, repo, folder)
	}
	if err := repo.UpdatePath(ctx, "ghost-video", "/target/video/album", "/target/video", "album"); err != nil {
		t.Fatalf("UpdatePath(ghost-video) error = %v", err)
	}
	if err := repo.UpdatePath(ctx, "ghost-photo", "/target/photo/album", "/target/photo", "album"); err != nil {
		t.Fatalf("UpdatePath(ghost-photo) error = %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/folders?top_level_only=true&page=1&limit=10", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
	}

	var payload struct {
		Data  []repository.Folder `json:"data"`
		Total int                 `json:"total"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if payload.Total != 1 {
		t.Fatalf("total = %d, want 1", payload.Total)
	}
	if len(payload.Data) != 1 {
		t.Fatalf("len(data) = %d, want 1", len(payload.Data))
	}
	if payload.Data[0].ID != "legit-workflow-root" {
		t.Fatalf("data[0].ID = %q, want legit-workflow-root", payload.Data[0].ID)
	}
}

func TestFolderHandlerUpdateStatusDoneRequiresPassedOutputCheck(t *testing.T) {
	gin.SetMode(gin.TestMode)

	database := newHandlerTestDB(t)
	repo := repository.NewFolderRepository(database)
	configRepo := repository.NewConfigRepository(database)
	scheduledRepo := repository.NewScheduledWorkflowRepository(database)
	starter := &stubScanStarter{called: make(chan scannerCall, 1)}
	fsAdapter := fs.NewMockAdapter()
	validator := &stubOutputValidator{canMarkDone: false}

	handler := NewFolderHandler(repo, configRepo, scheduledRepo, starter, fsAdapter, "/test/source", "/test/delete-staging")
	handler.SetOutputChecker(validator)

	router := gin.New()
	router.PATCH("/folders/:id/status", handler.UpdateStatus)

	seedFolder(t, repo, &repository.Folder{
		ID:             "f-output-check",
		Path:           "/media/output-check",
		Name:           "output-check",
		Category:       "photo",
		CategorySource: "auto",
		Status:         "pending",
	})

	req := httptest.NewRequest(http.MethodPatch, "/folders/f-output-check/status", bytes.NewBufferString(`{"status":"done"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}

	validator.canMarkDone = true
	req = httptest.NewRequest(http.MethodPatch, "/folders/f-output-check/status", bytes.NewBufferString(`{"status":"done"}`))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d body=%s", w.Code, http.StatusOK, w.Body.String())
	}
}
