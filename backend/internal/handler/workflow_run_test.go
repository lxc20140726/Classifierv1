package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/service"
)

type stubWorkflowRunReader struct {
	resumeWithDataCalled bool
	resumeWithDataRunID  string
	resumeWithData       map[string]any
	resumeWithDataErr    error
	reviewsResp          *service.ProcessingReviewList
	reviewsErr           error
	approveErr           error
	rollbackReviewErr    error
	approveAllErr        error
	rollbackAllErr       error
	approveRunID         string
	approveReviewID      string
	rollbackRunID        string
	rollbackReviewID     string
	approveAllRunID      string
	rollbackAllRunID     string
	approveAllCount      int
	rollbackAllCount     int
}

func (s *stubWorkflowRunReader) ListWorkflowRuns(_ context.Context, _ string, _ int, _ int) ([]*repository.WorkflowRun, int, error) {
	return nil, 0, nil
}

func (s *stubWorkflowRunReader) GetWorkflowRunDetail(_ context.Context, _ string) (*service.WorkflowRunDetail, error) {
	return nil, nil
}

func (s *stubWorkflowRunReader) ResumeWorkflowRun(_ context.Context, _ string) error {
	return nil
}

func (s *stubWorkflowRunReader) ListProcessingReviews(_ context.Context, _ string) (*service.ProcessingReviewList, error) {
	if s.reviewsResp != nil || s.reviewsErr != nil {
		return s.reviewsResp, s.reviewsErr
	}
	return &service.ProcessingReviewList{}, nil
}

func (s *stubWorkflowRunReader) ApproveProcessingReview(_ context.Context, runID, reviewID string) error {
	s.approveRunID = runID
	s.approveReviewID = reviewID
	return s.approveErr
}

func (s *stubWorkflowRunReader) RollbackProcessingReview(_ context.Context, runID, reviewID string) error {
	s.rollbackRunID = runID
	s.rollbackReviewID = reviewID
	return s.rollbackReviewErr
}

func (s *stubWorkflowRunReader) ApproveAllPendingProcessingReviews(_ context.Context, runID string) (int, error) {
	s.approveAllRunID = runID
	if s.approveAllErr != nil {
		return 0, s.approveAllErr
	}
	return s.approveAllCount, nil
}

func (s *stubWorkflowRunReader) RollbackAllPendingProcessingReviews(_ context.Context, runID string) (int, error) {
	s.rollbackAllRunID = runID
	if s.rollbackAllErr != nil {
		return 0, s.rollbackAllErr
	}
	return s.rollbackAllCount, nil
}

func (s *stubWorkflowRunReader) ResumeWorkflowRunWithData(_ context.Context, runID string, resumeData map[string]any) error {
	s.resumeWithDataCalled = true
	s.resumeWithDataRunID = runID
	s.resumeWithData = resumeData
	return s.resumeWithDataErr
}

func (s *stubWorkflowRunReader) RollbackWorkflowRun(_ context.Context, _ string) error {
	return nil
}

func setupWorkflowRunRouter(reader WorkflowRunReader) *gin.Engine {
	r := gin.New()
	h := NewWorkflowRunHandler(reader)
	r.POST("/workflow-runs/:id/provide-input", h.ProvideInput)
	r.GET("/workflow-runs/:id/reviews", h.ListReviews)
	r.POST("/workflow-runs/:id/reviews/:reviewId/approve", h.ApproveReview)
	r.POST("/workflow-runs/:id/reviews/:reviewId/rollback", h.RollbackReview)
	r.POST("/workflow-runs/:id/reviews/approve-all", h.ApproveAllReviews)
	r.POST("/workflow-runs/:id/reviews/rollback-all", h.RollbackAllReviews)
	return r
}

func TestWorkflowRunHandler_ProvideInput_OK(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reader := &stubWorkflowRunReader{}
	router := setupWorkflowRunRouter(reader)

	req := httptest.NewRequest(http.MethodPost, "/workflow-runs/run-123/provide-input", bytes.NewBufferString(`{"category":"manga"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusNoContent)
	}
	if !reader.resumeWithDataCalled {
		t.Fatalf("ResumeWorkflowRunWithData called = false, want true")
	}
	if reader.resumeWithDataRunID != "run-123" {
		t.Fatalf("runID = %q, want run-123", reader.resumeWithDataRunID)
	}
	rawCategory, ok := reader.resumeWithData["category"]
	if !ok {
		t.Fatalf("resume data missing category")
	}
	category, ok := rawCategory.(string)
	if !ok || category != "manga" {
		t.Fatalf("category = %#v, want manga", rawCategory)
	}
}

func TestWorkflowRunHandler_ProvideInput_InvalidCategory(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reader := &stubWorkflowRunReader{}
	router := setupWorkflowRunRouter(reader)

	req := httptest.NewRequest(http.MethodPost, "/workflow-runs/run-123/provide-input", bytes.NewBufferString(`{"category":"bad"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusBadRequest)
	}
	if reader.resumeWithDataCalled {
		t.Fatalf("ResumeWorkflowRunWithData called = true, want false")
	}
}

func TestWorkflowRunHandler_ListReviews_OK(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reader := &stubWorkflowRunReader{
		reviewsResp: &service.ProcessingReviewList{
			Items: []*repository.ProcessingReviewItem{
				{ID: "review-1", WorkflowRunID: "run-1", Status: "pending"},
			},
			Summary: &service.ProcessingReviewSummary{Total: 1, Pending: 1},
		},
	}
	router := setupWorkflowRunRouter(reader)

	req := httptest.NewRequest(http.MethodGet, "/workflow-runs/run-1/reviews", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
	}
	var resp struct {
		Data    []map[string]any                `json:"data"`
		Summary service.ProcessingReviewSummary `json:"summary"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}
	if len(resp.Data) != 1 {
		t.Fatalf("data len = %d, want 1", len(resp.Data))
	}
	if resp.Summary.Pending != 1 {
		t.Fatalf("summary.pending = %d, want 1", resp.Summary.Pending)
	}
}

func TestWorkflowRunHandler_ApproveAndRollbackReview(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reader := &stubWorkflowRunReader{}
	router := setupWorkflowRunRouter(reader)

	approveReq := httptest.NewRequest(http.MethodPost, "/workflow-runs/run-1/reviews/review-1/approve", nil)
	approveW := httptest.NewRecorder()
	router.ServeHTTP(approveW, approveReq)
	if approveW.Code != http.StatusOK {
		t.Fatalf("approve status = %d, want %d", approveW.Code, http.StatusOK)
	}
	if reader.approveRunID != "run-1" || reader.approveReviewID != "review-1" {
		t.Fatalf("approve args = (%q,%q), want (run-1,review-1)", reader.approveRunID, reader.approveReviewID)
	}

	rollbackReq := httptest.NewRequest(http.MethodPost, "/workflow-runs/run-1/reviews/review-2/rollback", nil)
	rollbackW := httptest.NewRecorder()
	router.ServeHTTP(rollbackW, rollbackReq)
	if rollbackW.Code != http.StatusOK {
		t.Fatalf("rollback status = %d, want %d", rollbackW.Code, http.StatusOK)
	}
	if reader.rollbackRunID != "run-1" || reader.rollbackReviewID != "review-2" {
		t.Fatalf("rollback args = (%q,%q), want (run-1,review-2)", reader.rollbackRunID, reader.rollbackReviewID)
	}
}

func TestWorkflowRunHandler_ReviewNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reader := &stubWorkflowRunReader{approveErr: repository.ErrNotFound, rollbackReviewErr: repository.ErrNotFound}
	router := setupWorkflowRunRouter(reader)

	approveReq := httptest.NewRequest(http.MethodPost, "/workflow-runs/run-1/reviews/review-missing/approve", nil)
	approveW := httptest.NewRecorder()
	router.ServeHTTP(approveW, approveReq)
	if approveW.Code != http.StatusNotFound {
		t.Fatalf("approve status = %d, want %d", approveW.Code, http.StatusNotFound)
	}

	rollbackReq := httptest.NewRequest(http.MethodPost, "/workflow-runs/run-1/reviews/review-missing/rollback", nil)
	rollbackW := httptest.NewRecorder()
	router.ServeHTTP(rollbackW, rollbackReq)
	if rollbackW.Code != http.StatusNotFound {
		t.Fatalf("rollback status = %d, want %d", rollbackW.Code, http.StatusNotFound)
	}
}

func TestWorkflowRunHandler_ApproveAndRollbackAllReviews(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reader := &stubWorkflowRunReader{
		approveAllCount:  3,
		rollbackAllCount: 2,
	}
	router := setupWorkflowRunRouter(reader)

	approveReq := httptest.NewRequest(http.MethodPost, "/workflow-runs/run-1/reviews/approve-all", nil)
	approveW := httptest.NewRecorder()
	router.ServeHTTP(approveW, approveReq)
	if approveW.Code != http.StatusOK {
		t.Fatalf("approve-all status = %d, want %d", approveW.Code, http.StatusOK)
	}
	if reader.approveAllRunID != "run-1" {
		t.Fatalf("approve-all runID = %q, want run-1", reader.approveAllRunID)
	}
	var approveResp map[string]int
	if err := json.Unmarshal(approveW.Body.Bytes(), &approveResp); err != nil {
		t.Fatalf("json.Unmarshal(approve-all) error = %v", err)
	}
	if approveResp["approved"] != 3 {
		t.Fatalf("approve-all approved = %d, want 3", approveResp["approved"])
	}

	rollbackReq := httptest.NewRequest(http.MethodPost, "/workflow-runs/run-1/reviews/rollback-all", nil)
	rollbackW := httptest.NewRecorder()
	router.ServeHTTP(rollbackW, rollbackReq)
	if rollbackW.Code != http.StatusOK {
		t.Fatalf("rollback-all status = %d, want %d", rollbackW.Code, http.StatusOK)
	}
	if reader.rollbackAllRunID != "run-1" {
		t.Fatalf("rollback-all runID = %q, want run-1", reader.rollbackAllRunID)
	}
	var rollbackResp map[string]int
	if err := json.Unmarshal(rollbackW.Body.Bytes(), &rollbackResp); err != nil {
		t.Fatalf("json.Unmarshal(rollback-all) error = %v", err)
	}
	if rollbackResp["rolled_back"] != 2 {
		t.Fatalf("rollback-all rolled_back = %d, want 2", rollbackResp["rolled_back"])
	}
}

func TestWorkflowRunHandler_ApproveAndRollbackAllReviewsRunNotFound(t *testing.T) {
	gin.SetMode(gin.TestMode)

	reader := &stubWorkflowRunReader{
		approveAllErr:  repository.ErrNotFound,
		rollbackAllErr: repository.ErrNotFound,
	}
	router := setupWorkflowRunRouter(reader)

	approveReq := httptest.NewRequest(http.MethodPost, "/workflow-runs/run-missing/reviews/approve-all", nil)
	approveW := httptest.NewRecorder()
	router.ServeHTTP(approveW, approveReq)
	if approveW.Code != http.StatusNotFound {
		t.Fatalf("approve-all status = %d, want %d", approveW.Code, http.StatusNotFound)
	}

	rollbackReq := httptest.NewRequest(http.MethodPost, "/workflow-runs/run-missing/reviews/rollback-all", nil)
	rollbackW := httptest.NewRecorder()
	router.ServeHTTP(rollbackW, rollbackReq)
	if rollbackW.Code != http.StatusNotFound {
		t.Fatalf("rollback-all status = %d, want %d", rollbackW.Code, http.StatusNotFound)
	}
}
