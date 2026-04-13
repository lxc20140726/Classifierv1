package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/gin-gonic/gin"
	dbpkg "github.com/liqiye/classifier/internal/db"
	"github.com/liqiye/classifier/internal/repository"
)

type stubConfigSyncer struct{}

func (s *stubConfigSyncer) Sync(_ context.Context) error {
	return nil
}

var configHandlerDBCounter uint64

func newConfigHandlerTestRepo(t *testing.T) repository.ConfigRepository {
	t.Helper()

	id := atomic.AddUint64(&configHandlerDBCounter, 1)
	dsn := fmt.Sprintf("file:classifier_config_handler_%d?cache=shared&mode=memory", id)

	database, err := dbpkg.Open(dsn)
	if err != nil {
		t.Fatalf("db.Open(%q) error = %v", dsn, err)
	}

	t.Cleanup(func() {
		_ = database.Close()
	})

	return repository.NewConfigRepository(database)
}

func setupConfigRouter(configRepo repository.ConfigRepository) *gin.Engine {
	g := gin.New()
	h := NewConfigHandler(configRepo, &stubConfigSyncer{})

	g.GET("/config", h.Get)
	g.PUT("/config", h.Put)

	return g
}

func TestConfigHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := newConfigHandlerTestRepo(t)
	router := setupConfigRouter(repo)

	t.Run("get returns stored config", func(t *testing.T) {
		err := repo.SaveAppConfig(context.Background(), &repository.AppConfig{
			ScanInputDirs: []string{"/media/source", "/media/source-2"},
			OutputDirs: repository.AppConfigOutputDirs{
				Video: []string{"/media/target/video"},
				Manga: []string{"/media/target/manga"},
				Photo: []string{"/media/target/photo"},
				Other: []string{"/media/target/other"},
				Mixed: []string{"/media/target/mixed"},
			},
		})
		if err != nil {
			t.Fatalf("repo.SaveAppConfig() error = %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/config", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}

		var payload struct {
			Data repository.AppConfig `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}

		if !reflect.DeepEqual(payload.Data.ScanInputDirs, []string{"/media/source", "/media/source-2"}) {
			t.Fatalf("scan_input_dirs = %#v, want [/media/source /media/source-2]", payload.Data.ScanInputDirs)
		}
		if payload.Data.ScanCron != "" {
			t.Fatalf("scan_cron = %q, want empty", payload.Data.ScanCron)
		}
	})

	t.Run("put saves structured config and no longer rewrites legacy keys", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/config", bytes.NewBufferString(`{
			"scan_input_dirs":["/mnt/source","/mnt/source-2"],
			"scan_cron":"*/15 * * * *",
			"source_dir":"/mnt/source",
			"target_dir":"/mnt/target",
			"output_dirs":{
				"video":["/mnt/target/video","/mnt/target/video-2"],
				"manga":"/mnt/target/manga",
				"photo":["/mnt/target/photo"],
				"other":"/mnt/target/other",
				"mixed":"/mnt/target/mixed"
			}
		}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}

		storedConfig, err := repo.GetAppConfig(context.Background())
		if err != nil {
			t.Fatalf("repo.GetAppConfig() error = %v", err)
		}
		if !reflect.DeepEqual(storedConfig.ScanInputDirs, []string{"/mnt/source", "/mnt/source-2"}) {
			t.Fatalf("scan_input_dirs = %#v, want [/mnt/source /mnt/source-2]", storedConfig.ScanInputDirs)
		}
		if storedConfig.ScanCron != "*/15 * * * *" {
			t.Fatalf("scan_cron = %q, want */15 * * * *", storedConfig.ScanCron)
		}
		if !reflect.DeepEqual(storedConfig.OutputDirs.Video, []string{"/mnt/target/video", "/mnt/target/video-2"}) {
			t.Fatalf("output_dirs.video = %#v, want [/mnt/target/video /mnt/target/video-2]", storedConfig.OutputDirs.Video)
		}
	})

	t.Run("put invalid scan cron returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/config", bytes.NewBufferString(`{"scan_cron":"bad cron"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("put invalid json returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/config", bytes.NewBufferString(`{"scan_input_dirs"`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("put wrong typed value returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/config", bytes.NewBufferString(`{"scan_input_dirs":"/mnt/source"}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("get maps legacy config when app_config is empty", func(t *testing.T) {
		legacyRepo := newConfigHandlerTestRepo(t)
		legacyRouter := setupConfigRouter(legacyRepo)

		if err := legacyRepo.Set(context.Background(), "source_dir", "/legacy/source"); err != nil {
			t.Fatalf("repo.Set(source_dir) error = %v", err)
		}
		if err := legacyRepo.Set(context.Background(), "target_dir", "/legacy/target"); err != nil {
			t.Fatalf("repo.Set(target_dir) error = %v", err)
		}
		if err := legacyRepo.Set(context.Background(), "scan_input_dirs", `["/legacy/source","/legacy/source-2"]`); err != nil {
			t.Fatalf("repo.Set(scan_input_dirs) error = %v", err)
		}

		req := httptest.NewRequest(http.MethodGet, "/config", nil)
		w := httptest.NewRecorder()
		legacyRouter.ServeHTTP(w, req)

		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}

		var payload struct {
			Data repository.AppConfig `json:"data"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}

		expectedVideoDir := filepath.Join("/legacy/target", "video")
		if !reflect.DeepEqual(payload.Data.OutputDirs.Video, []string{expectedVideoDir}) {
			t.Fatalf("output_dirs.video = %#v, want [%q]", payload.Data.OutputDirs.Video, expectedVideoDir)
		}
		if !reflect.DeepEqual(payload.Data.ScanInputDirs, []string{"/legacy/source", "/legacy/source-2"}) {
			t.Fatalf("scan_input_dirs = %#v, want [/legacy/source /legacy/source-2]", payload.Data.ScanInputDirs)
		}
	})

	t.Run("put relative output dir returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodPut, "/config", bytes.NewBufferString(`{
			"output_dirs": {
				"video": ["/ok/path", "relative/path"]
			}
		}`))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()

		router.ServeHTTP(w, req)

		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})
}
