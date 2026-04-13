package handler

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/service"
)

type stubFolderLineageReader struct {
	resp *service.FolderLineageResponse
	err  error
}

func (s *stubFolderLineageReader) GetFolderLineage(_ context.Context, _ string) (*service.FolderLineageResponse, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.resp, nil
}

func TestFolderLineageHandlerNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &FolderHandler{}
	h.SetLineageReader(&stubFolderLineageReader{err: repository.ErrNotFound})

	router := gin.New()
	router.GET("/folders/:id/lineage", h.GetLineage)

	req := httptest.NewRequest(http.MethodGet, "/folders/missing/lineage", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", w.Code)
	}
}

func TestFolderLineageHandlerReturnsValidStructure(t *testing.T) {
	gin.SetMode(gin.TestMode)

	payload := &service.FolderLineageResponse{
		Folder: &repository.Folder{ID: "f1", Path: "/current"},
		Summary: service.FolderLineageSummary{
			OriginalPath: "/origin",
			CurrentPath:  "/current",
			Status:       "done",
			Category:     "video",
		},
		Graph: service.FolderLineageGraph{
			Nodes: []service.FolderLineageNode{
				{ID: "n1", Type: "origin", Path: "/origin"},
				{ID: "n2", Type: "current_path", Path: "/current"},
				{ID: "n3", Type: "artifact", Path: "/out/a.cbz"},
			},
			Edges: []service.FolderLineageEdge{
				{ID: "e1", Type: "moved_to", Source: "n1", Target: "n2"},
				{ID: "e2", Type: "produced", Source: "n2", Target: "n3"},
			},
		},
		Timeline: []service.FolderLineageTimelineEvent{
			{ID: "t1", Type: "scan_discovered", OccurredAt: time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC)},
			{ID: "t2", Type: "move", OccurredAt: time.Date(2026, 1, 1, 2, 0, 0, 0, time.UTC)},
			{ID: "t3", Type: "artifact_created", OccurredAt: time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)},
		},
	}

	h := &FolderHandler{}
	h.SetLineageReader(&stubFolderLineageReader{resp: payload})

	router := gin.New()
	router.GET("/folders/:id/lineage", h.GetLineage)

	req := httptest.NewRequest(http.MethodGet, "/folders/f1/lineage", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 body=%s", w.Code, w.Body.String())
	}

	var decoded service.FolderLineageResponse
	if err := json.Unmarshal(w.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if decoded.Summary.OriginalPath == "" || decoded.Summary.CurrentPath == "" {
		t.Fatalf("summary paths should be present: %#v", decoded.Summary)
	}
	validNodeTypes := map[string]struct{}{
		"origin":          {},
		"historical_path": {},
		"current_path":    {},
		"artifact":        {},
	}
	for _, node := range decoded.Graph.Nodes {
		if _, ok := validNodeTypes[node.Type]; !ok {
			t.Fatalf("invalid node type %q", node.Type)
		}
	}
	for idx := 1; idx < len(decoded.Timeline); idx++ {
		if decoded.Timeline[idx].OccurredAt.Before(decoded.Timeline[idx-1].OccurredAt) {
			t.Fatalf("timeline is not sorted: %#v", decoded.Timeline)
		}
	}
}

func TestFolderLineageHandlerInternalError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	h := &FolderHandler{}
	h.SetLineageReader(&stubFolderLineageReader{err: errors.New("boom")})

	router := gin.New()
	router.GET("/folders/:id/lineage", h.GetLineage)

	req := httptest.NewRequest(http.MethodGet, "/folders/f1/lineage", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", w.Code)
	}
}
