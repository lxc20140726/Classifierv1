package handler

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	internalfs "github.com/liqiye/classifier/internal/fs"
)

func TestFSHandlerListDirs(t *testing.T) {
	t.Parallel()
	gin.SetMode(gin.TestMode)

	rootDir := t.TempDir()
	if err := os.Mkdir(filepath.Join(rootDir, "b"), 0o755); err != nil {
		t.Fatalf("os.Mkdir(b) error = %v", err)
	}
	if err := os.Mkdir(filepath.Join(rootDir, "a"), 0o755); err != nil {
		t.Fatalf("os.Mkdir(a) error = %v", err)
	}
	if err := os.Mkdir(filepath.Join(rootDir, ".hidden"), 0o755); err != nil {
		t.Fatalf("os.Mkdir(.hidden) error = %v", err)
	}

	router := gin.New()
	handler := NewFSHandler(internalfs.NewOSAdapter())
	router.GET("/fs/dirs", handler.ListDirs)

	t.Run("relative path returns 400", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/fs/dirs?path=relative/path", nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
		}
	})

	t.Run("list directory excludes hidden and sorts names", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/fs/dirs?path="+urlPathValue(rootDir), nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}

		body := w.Body.String()
		if !containsAll(body, `"name":"a"`, `"name":"b"`) {
			t.Fatalf("response = %s, want names a and b", body)
		}
		if containsAll(body, `"name":".hidden"`) {
			t.Fatalf("response = %s, hidden directory should be filtered", body)
		}
		if indexOf(body, `"name":"a"`) > indexOf(body, `"name":"b"`) {
			t.Fatalf("response = %s, expected sorted names", body)
		}
	})

	t.Run("root parent is empty", func(t *testing.T) {
		rootPath := "/"
		if runtime.GOOS == "windows" {
			rootPath = filepath.VolumeName(rootDir) + `\`
		}

		req := httptest.NewRequest(http.MethodGet, "/fs/dirs?path="+urlPathValue(rootPath), nil)
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d, body = %s", w.Code, http.StatusOK, w.Body.String())
		}
		if !containsAll(w.Body.String(), `"parent":""`) {
			t.Fatalf("response = %s, want empty parent", w.Body.String())
		}
	})
}

func urlPathValue(path string) string {
	return url.QueryEscape(path)
}

func containsAll(content string, snippets ...string) bool {
	for _, snippet := range snippets {
		if !strings.Contains(content, snippet) {
			return false
		}
	}
	return true
}

func indexOf(content, snippet string) int {
	return strings.Index(content, snippet)
}
