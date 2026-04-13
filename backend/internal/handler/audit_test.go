package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/liqiye/classifier/internal/repository"
)

type stubAuditRepository struct {
	lastFilter repository.AuditListFilter
}

func (s *stubAuditRepository) Write(_ context.Context, _ *repository.AuditLog) error {
	return nil
}

func (s *stubAuditRepository) List(_ context.Context, filter repository.AuditListFilter) ([]*repository.AuditLog, int, error) {
	s.lastFilter = filter
	return []*repository.AuditLog{}, 0, nil
}

func (s *stubAuditRepository) GetByID(_ context.Context, _ string) (*repository.AuditLog, error) {
	return nil, repository.ErrNotFound
}

func setupAuditRouter(auditRepo repository.AuditRepository) *gin.Engine {
	r := gin.New()
	h := NewAuditHandler(auditRepo)
	r.GET("/audit-logs", h.List)
	return r
}

func TestAuditHandlerListParsesFilters(t *testing.T) {
	gin.SetMode(gin.TestMode)

	stubRepo := &stubAuditRepository{}
	router := setupAuditRouter(stubRepo)

	req := httptest.NewRequest(http.MethodGet, "/audit-logs?page=2&limit=30&job_id=j1&workflow_run_id=wr1&node_run_id=nr1&node_id=n1&node_type=move-node&action=move&result=success&folder_id=f1&folder_path=movies&from=2026-03-20T09:10:11Z&to=2026-03-20T10:10:11Z", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	if stubRepo.lastFilter.Page != 2 {
		t.Fatalf("Page = %d, want 2", stubRepo.lastFilter.Page)
	}
	if stubRepo.lastFilter.Limit != 30 {
		t.Fatalf("Limit = %d, want 30", stubRepo.lastFilter.Limit)
	}
	if stubRepo.lastFilter.FolderPathKeyword != "movies" {
		t.Fatalf("FolderPathKeyword = %q, want movies", stubRepo.lastFilter.FolderPathKeyword)
	}
	if stubRepo.lastFilter.From.IsZero() || stubRepo.lastFilter.To.IsZero() {
		t.Fatalf("From/To should be parsed, got from=%v to=%v", stubRepo.lastFilter.From, stubRepo.lastFilter.To)
	}
	if stubRepo.lastFilter.WorkflowRunID != "wr1" || stubRepo.lastFilter.NodeRunID != "nr1" {
		t.Fatalf("WorkflowRunID/NodeRunID = %q/%q, want wr1/nr1", stubRepo.lastFilter.WorkflowRunID, stubRepo.lastFilter.NodeRunID)
	}
	if stubRepo.lastFilter.NodeID != "n1" || stubRepo.lastFilter.NodeType != "move-node" {
		t.Fatalf("NodeID/NodeType = %q/%q, want n1/move-node", stubRepo.lastFilter.NodeID, stubRepo.lastFilter.NodeType)
	}
}

func TestAuditHandlerListInvalidFromReturnsBadRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	stubRepo := &stubAuditRepository{}
	router := setupAuditRouter(stubRepo)

	req := httptest.NewRequest(http.MethodGet, "/audit-logs?from=bad-time", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}

func TestAuditHandlerListInvalidRangeReturnsBadRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)

	stubRepo := &stubAuditRepository{}
	router := setupAuditRouter(stubRepo)

	req := httptest.NewRequest(http.MethodGet, "/audit-logs?from=2026-03-20T10:10:11Z&to=2026-03-20T09:10:11Z", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
}
