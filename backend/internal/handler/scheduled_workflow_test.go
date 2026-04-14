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

type stubScheduledWorkflowRunner struct {
	jobID string
	err   error
	calls int
}

func (s *stubScheduledWorkflowRunner) StartJob(_ context.Context, _ service.StartWorkflowJobInput) (string, error) {
	s.calls++
	if s.err != nil {
		return "", s.err
	}
	if s.jobID != "" {
		return s.jobID, nil
	}
	return "job-run-now", nil
}

type stubScheduledScanRunner struct {
	jobID string
	err   error
}

func (s *stubScheduledScanRunner) StartScheduledJob(_ context.Context, _ []string) (string, bool, error) {
	if s.err != nil {
		return "", false, s.err
	}
	if s.jobID == "" {
		s.jobID = "scan-job-now"
	}
	return s.jobID, true, nil
}

type stubScheduledSyncer struct {
	err   error
	calls int
}

func (s *stubScheduledSyncer) Sync(_ context.Context) error {
	s.calls++
	return s.err
}

func setupScheduledWorkflowRouter(h *ScheduledWorkflowHandler) *gin.Engine {
	r := gin.New()
	r.GET("/scheduled-workflows", h.List)
	r.GET("/scheduled-workflows/:id", h.Get)
	r.POST("/scheduled-workflows", h.Create)
	r.PUT("/scheduled-workflows/:id", h.Update)
	r.DELETE("/scheduled-workflows/:id", h.Delete)
	r.POST("/scheduled-workflows/:id/run-now", h.RunNow)
	return r
}

func TestScheduledWorkflowHandler_CRUDAndRunNow(t *testing.T) {
	gin.SetMode(gin.TestMode)

	database := newHandlerTestDB(t)
	repo := repository.NewScheduledWorkflowRepository(database)
	runner := &stubScheduledWorkflowRunner{jobID: "job-run-now"}
	svc := service.NewScheduledWorkflowService(repo, runner, &stubScheduledScanRunner{})
	syncer := &stubScheduledSyncer{}
	router := setupScheduledWorkflowRouter(NewScheduledWorkflowHandler(repo, svc, syncer))

	t.Run("list invalid paging", func(t *testing.T) {
		req := httptest.NewRequest(http.MethodGet, "/scheduled-workflows?page=0", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", resp.Code, http.StatusBadRequest)
		}

		req = httptest.NewRequest(http.MethodGet, "/scheduled-workflows?limit=-2", nil)
		resp = httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", resp.Code, http.StatusBadRequest)
		}
	})

	t.Run("create and list/get with response contract", func(t *testing.T) {
		createReq := httptest.NewRequest(http.MethodPost, "/scheduled-workflows", bytes.NewBufferString(`{"name":"每日整理","job_type":"workflow","workflow_def_id":"wf-1","cron_spec":"0 0 * * *","enabled":true,"folder_ids":["f-1"],"source_dirs":[]}`))
		createReq.Header.Set("Content-Type", "application/json")
		createResp := httptest.NewRecorder()
		router.ServeHTTP(createResp, createReq)
		if createResp.Code != http.StatusCreated {
			t.Fatalf("status = %d, want %d body=%s", createResp.Code, http.StatusCreated, createResp.Body.String())
		}

		var created struct {
			Data map[string]any `json:"data"`
		}
		if err := json.Unmarshal(createResp.Body.Bytes(), &created); err != nil {
			t.Fatalf("json.Unmarshal() error = %v", err)
		}
		id, _ := created.Data["id"].(string)
		if id == "" {
			t.Fatalf("created id = empty")
		}
		if created.Data["name"] != "每日整理" {
			t.Fatalf("name = %#v, want 每日整理", created.Data["name"])
		}

		listReq := httptest.NewRequest(http.MethodGet, "/scheduled-workflows?page=1&limit=10", nil)
		listResp := httptest.NewRecorder()
		router.ServeHTTP(listResp, listReq)
		if listResp.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", listResp.Code, http.StatusOK)
		}

		getReq := httptest.NewRequest(http.MethodGet, "/scheduled-workflows/"+id, nil)
		getResp := httptest.NewRecorder()
		router.ServeHTTP(getResp, getReq)
		if getResp.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", getResp.Code, http.StatusOK)
		}

		runNowReq := httptest.NewRequest(http.MethodPost, "/scheduled-workflows/"+id+"/run-now", nil)
		runNowResp := httptest.NewRecorder()
		router.ServeHTTP(runNowResp, runNowReq)
		if runNowResp.Code != http.StatusAccepted {
			t.Fatalf("status = %d, want %d body=%s", runNowResp.Code, http.StatusAccepted, runNowResp.Body.String())
		}
	})

	t.Run("create/update/delete bad request and not found", func(t *testing.T) {
		badJSONReq := httptest.NewRequest(http.MethodPost, "/scheduled-workflows", bytes.NewBufferString("{"))
		badJSONReq.Header.Set("Content-Type", "application/json")
		badJSONResp := httptest.NewRecorder()
		router.ServeHTTP(badJSONResp, badJSONReq)
		if badJSONResp.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", badJSONResp.Code, http.StatusBadRequest)
		}

		invalidReq := httptest.NewRequest(http.MethodPost, "/scheduled-workflows", bytes.NewBufferString(`{"name":"x","job_type":"workflow","workflow_def_id":"wf-1","cron_spec":"bad cron","enabled":true}`))
		invalidReq.Header.Set("Content-Type", "application/json")
		invalidResp := httptest.NewRecorder()
		router.ServeHTTP(invalidResp, invalidReq)
		if invalidResp.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want %d", invalidResp.Code, http.StatusBadRequest)
		}

		updateReq := httptest.NewRequest(http.MethodPut, "/scheduled-workflows/missing", bytes.NewBufferString(`{"name":"x","job_type":"workflow","workflow_def_id":"wf-1","cron_spec":"0 * * * *","enabled":true}`))
		updateReq.Header.Set("Content-Type", "application/json")
		updateResp := httptest.NewRecorder()
		router.ServeHTTP(updateResp, updateReq)
		if updateResp.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d body=%s", updateResp.Code, http.StatusNotFound, updateResp.Body.String())
		}

		deleteReq := httptest.NewRequest(http.MethodDelete, "/scheduled-workflows/missing", nil)
		deleteResp := httptest.NewRecorder()
		router.ServeHTTP(deleteResp, deleteReq)
		if deleteResp.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", deleteResp.Code, http.StatusNotFound)
		}

		runNowReq := httptest.NewRequest(http.MethodPost, "/scheduled-workflows/missing/run-now", nil)
		runNowResp := httptest.NewRecorder()
		router.ServeHTTP(runNowResp, runNowReq)
		if runNowResp.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want %d", runNowResp.Code, http.StatusNotFound)
		}
	})

	t.Run("sync failure returns 500", func(t *testing.T) {
		syncer.err = errors.New("sync failed")
		defer func() {
			syncer.err = nil
		}()

		req := httptest.NewRequest(http.MethodPost, "/scheduled-workflows", bytes.NewBufferString(`{"name":"同步失败用例","job_type":"workflow","workflow_def_id":"wf-1","cron_spec":"0 1 * * *","enabled":true}`))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		if resp.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d body=%s", resp.Code, http.StatusInternalServerError, resp.Body.String())
		}
	})

	t.Run("run now internal error when runner fails", func(t *testing.T) {
		runner.err = errors.New("runner failed")
		defer func() {
			runner.err = nil
		}()

		createReq := httptest.NewRequest(http.MethodPost, "/scheduled-workflows", bytes.NewBufferString(`{"name":"运行失败用例","job_type":"workflow","workflow_def_id":"wf-run-fail","cron_spec":"0 2 * * *","enabled":true}`))
		createReq.Header.Set("Content-Type", "application/json")
		createResp := httptest.NewRecorder()
		router.ServeHTTP(createResp, createReq)
		if createResp.Code != http.StatusCreated {
			t.Fatalf("create status = %d, want %d body=%s", createResp.Code, http.StatusCreated, createResp.Body.String())
		}

		var created struct {
			Data struct {
				ID string `json:"id"`
			} `json:"data"`
		}
		if err := json.Unmarshal(createResp.Body.Bytes(), &created); err != nil {
			t.Fatalf("json.Unmarshal(create) error = %v", err)
		}

		runReq := httptest.NewRequest(http.MethodPost, "/scheduled-workflows/"+created.Data.ID+"/run-now", nil)
		runResp := httptest.NewRecorder()
		router.ServeHTTP(runResp, runReq)
		if runResp.Code != http.StatusInternalServerError {
			t.Fatalf("status = %d, want %d body=%s", runResp.Code, http.StatusInternalServerError, runResp.Body.String())
		}
	})
}
