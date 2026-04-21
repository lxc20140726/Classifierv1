package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/service"
)

type stubJobRepository struct {
	listFn func(ctx context.Context, filter repository.JobListFilter) ([]*repository.Job, int, error)
	getFn  func(ctx context.Context, id string) (*repository.Job, error)
}

func (s *stubJobRepository) Create(_ context.Context, _ *repository.Job) error {
	return nil
}

func (s *stubJobRepository) GetByID(ctx context.Context, id string) (*repository.Job, error) {
	if s.getFn != nil {
		return s.getFn(ctx, id)
	}
	return nil, repository.ErrNotFound
}

func (s *stubJobRepository) List(ctx context.Context, filter repository.JobListFilter) ([]*repository.Job, int, error) {
	if s.listFn != nil {
		return s.listFn(ctx, filter)
	}
	return []*repository.Job{}, 0, nil
}

func (s *stubJobRepository) UpdateTotal(_ context.Context, _ string, _ int) error {
	return nil
}

func (s *stubJobRepository) UpdateStatus(_ context.Context, _ string, _ string, _ string) error {
	return nil
}

func (s *stubJobRepository) IncrementProgress(_ context.Context, _ string, _, _ int) error {
	return nil
}

type stubWorkflowJobStarter struct {
	jobID  string
	err    error
	inputs []service.StartWorkflowJobInput
}

func (s *stubWorkflowJobStarter) StartJob(_ context.Context, input service.StartWorkflowJobInput) (string, error) {
	s.inputs = append(s.inputs, input)
	if s.err != nil {
		return "", s.err
	}
	if s.jobID != "" {
		return s.jobID, nil
	}
	return "job-started", nil
}

type stubConfigRepository struct {
	getFn func(ctx context.Context, key string) (string, error)
}

func (s *stubConfigRepository) Set(_ context.Context, _, _ string) error {
	return nil
}

func (s *stubConfigRepository) Get(ctx context.Context, key string) (string, error) {
	if s.getFn != nil {
		return s.getFn(ctx, key)
	}
	return "", repository.ErrNotFound
}

func (s *stubConfigRepository) GetAll(_ context.Context) (map[string]string, error) {
	return map[string]string{}, nil
}

func (s *stubConfigRepository) GetAppConfig(_ context.Context) (*repository.AppConfig, error) {
	return &repository.AppConfig{}, nil
}

func (s *stubConfigRepository) SaveAppConfig(_ context.Context, _ *repository.AppConfig) error {
	return nil
}

func (s *stubConfigRepository) EnsureAppConfig(_ context.Context) error {
	return nil
}

func setupJobRouter(h *JobHandler) *gin.Engine {
	r := gin.New()
	r.GET("/jobs", h.List)
	r.GET("/jobs/:id", h.Get)
	r.GET("/jobs/:id/progress", h.Progress)
	r.POST("/jobs", h.StartWorkflow)
	return r
}

func TestJobHandler_ListGetProgressAndErrors(t *testing.T) {
	gin.SetMode(gin.TestMode)

	repo := &stubJobRepository{
		listFn: func(_ context.Context, filter repository.JobListFilter) ([]*repository.Job, int, error) {
			if filter.Status != "running" {
				t.Fatalf("filter.Status = %q, want running", filter.Status)
			}
			if filter.Page != 2 || filter.Limit != 5 {
				t.Fatalf("filter page/limit = %d/%d, want 2/5", filter.Page, filter.Limit)
			}
			return []*repository.Job{{
				ID:            "job-1",
				Type:          "workflow",
				WorkflowDefID: "wf-1",
				Status:        "running",
				FolderIDs:     `["f1","f2"]`,
				Total:         10,
				Done:          3,
				Failed:        1,
			}}, 1, nil
		},
		getFn: func(_ context.Context, id string) (*repository.Job, error) {
			switch id {
			case "job-1":
				return &repository.Job{ID: "job-1", Status: "running", Done: 4, Total: 10, Failed: 1, FolderIDs: `[]`}, nil
			case "missing":
				return nil, repository.ErrNotFound
			default:
				return nil, errors.New("boom")
			}
		},
	}
	router := setupJobRouter(NewJobHandler(repo))

	t.Run("list success with contract fields", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/jobs?page=2&limit=5&status=running", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d body=%s", resp.Code, http.StatusOK, resp.Body.String())
		}

		var payload struct {
			Data  []map[string]any `json:"data"`
			Total int              `json:"total"`
			Page  int              `json:"page"`
			Limit int              `json:"limit"`
		}
		if err := json.Unmarshal(resp.Body.Bytes(), &payload); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		if payload.Total != 1 || payload.Page != 2 || payload.Limit != 5 {
			t.Fatalf("total/page/limit = %d/%d/%d, want 1/2/5", payload.Total, payload.Page, payload.Limit)
		}
		if len(payload.Data) != 1 {
			t.Fatalf("len(data) = %d, want 1", len(payload.Data))
		}
		if payload.Data[0]["workflow_def_id"] != "wf-1" {
			t.Fatalf("workflow_def_id = %#v, want wf-1", payload.Data[0]["workflow_def_id"])
		}
	})

	t.Run("list invalid params", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/jobs?page=0", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", resp.Code, http.StatusBadRequest)
		}

		req = httptest.NewRequest(http.MethodGet, "/jobs?limit=-1", nil)
		resp = httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", resp.Code, http.StatusBadRequest)
		}
	})

	t.Run("get and progress branches", func(t *testing.T) {
		okReq := httptest.NewRequest(http.MethodGet, "/jobs/job-1", nil)
		okResp := httptest.NewRecorder()
		router.ServeHTTP(okResp, okReq)
		if okResp.Code != http.StatusOK {
			t.Fatalf("get status = %d, want %d", okResp.Code, http.StatusOK)
		}

		missingReq := httptest.NewRequest(http.MethodGet, "/jobs/missing", nil)
		missingResp := httptest.NewRecorder()
		router.ServeHTTP(missingResp, missingReq)
		if missingResp.Code != http.StatusNotFound {
			t.Fatalf("missing get status = %d, want %d", missingResp.Code, http.StatusNotFound)
		}

		errReq := httptest.NewRequest(http.MethodGet, "/jobs/err", nil)
		errResp := httptest.NewRecorder()
		router.ServeHTTP(errResp, errReq)
		if errResp.Code != http.StatusInternalServerError {
			t.Fatalf("error get status = %d, want %d", errResp.Code, http.StatusInternalServerError)
		}

		progressReq := httptest.NewRequest(http.MethodGet, "/jobs/job-1/progress", nil)
		progressResp := httptest.NewRecorder()
		router.ServeHTTP(progressResp, progressReq)
		if progressResp.Code != http.StatusOK {
			t.Fatalf("progress status = %d, want %d", progressResp.Code, http.StatusOK)
		}
	})
}

func TestJobHandler_StartWorkflow(t *testing.T) {
	gin.SetMode(gin.TestMode)

	t.Run("not configured", func(t *testing.T) {
		router := setupJobRouter(NewJobHandler(&stubJobRepository{}))
		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(`{"workflow_def_id":"wf-1"}`))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusNotImplemented {
			t.Fatalf("status = %d, want %d body=%s", resp.Code, http.StatusNotImplemented, resp.Body.String())
		}
	})

	t.Run("invalid body and required field", func(t *testing.T) {
		starter := &stubWorkflowJobStarter{jobID: "job-1"}
		router := setupJobRouter(NewJobHandlerWithWorkflow(&stubJobRepository{}, nil, starter, nil, "/default/source"))

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString("{"))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", resp.Code, http.StatusBadRequest)
		}

		req = httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(`{"workflow_def_id":""}`))
		req.Header.Set("Content-Type", "application/json")
		resp = httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", resp.Code, http.StatusBadRequest)
		}
	})

	t.Run("source dir resolution precedence and starter error", func(t *testing.T) {
		starter := &stubWorkflowJobStarter{jobID: "job-accepted"}
		configRepo := &stubConfigRepository{getFn: func(_ context.Context, key string) (string, error) {
			if key != "source_dir" {
				t.Fatalf("config key = %q, want source_dir", key)
			}
			return "  /from-config  ", nil
		}}
		router := setupJobRouter(NewJobHandlerWithWorkflow(&stubJobRepository{}, nil, starter, configRepo, "/from-default"))

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(`{"workflow_def_id":"wf-1"}`))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d body=%s", resp.Code, http.StatusAccepted, resp.Body.String())
		}
		if len(starter.inputs) != 1 {
			t.Fatalf("starter inputs = %d, want 1", len(starter.inputs))
		}
		if starter.inputs[0].SourceDir != "/from-config" {
			t.Fatalf("source dir = %q, want /from-config", starter.inputs[0].SourceDir)
		}

		starter.jobID = ""
		starter.err = errors.New("start failed")
		req = httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(`{"workflow_def_id":"wf-2", "source_dir":" /from-req "}`))
		req.Header.Set("Content-Type", "application/json")
		resp = httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d", resp.Code, http.StatusInternalServerError)
		}
		if got := starter.inputs[len(starter.inputs)-1].SourceDir; got != "/from-req" {
			t.Fatalf("request source dir = %q, want /from-req", got)
		}
	})

	t.Run("accepts folder ids", func(t *testing.T) {
		starter := &stubWorkflowJobStarter{jobID: "job-folder-ids"}
		router := setupJobRouter(NewJobHandlerWithWorkflow(&stubJobRepository{}, nil, starter, nil, "/default/source"))

		req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(`{"workflow_def_id":"wf-3","folder_ids":["f1","f2"]}`))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d body=%s", resp.Code, http.StatusAccepted, resp.Body.String())
		}
		if len(starter.inputs) != 1 {
			t.Fatalf("starter inputs = %d, want 1", len(starter.inputs))
		}
		if len(starter.inputs[0].FolderIDs) != 2 {
			t.Fatalf("folder ids len = %d, want 2", len(starter.inputs[0].FolderIDs))
		}
	})
}
