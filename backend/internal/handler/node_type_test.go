package handler

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"

	"github.com/liqiye/classifier/internal/service"
)

type stubNodeTypeReader struct {
	schemas []service.NodeSchema
}

func (s *stubNodeTypeReader) ListNodeSchemas() []service.NodeSchema {
	return append([]service.NodeSchema(nil), s.schemas...)
}

func setupNodeTypeRouter(reader NodeTypeReader) *gin.Engine {
	r := gin.New()
	h := NewNodeTypeHandler(reader)
	r.GET("/node-types", h.List)
	return r
}

func TestNodeTypeHandlerList(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := setupNodeTypeRouter(&stubNodeTypeReader{schemas: []service.NodeSchema{
		{Type: "trigger", Label: "Trigger", Description: "Trigger node"},
		{Type: "move", Label: "Move", Description: "Move node"},
	}})

	req := httptest.NewRequest(http.MethodGet, "/node-types", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d, body=%s", w.Code, http.StatusOK, w.Body.String())
	}

	var payload struct {
		Data []service.NodeSchema `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &payload); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	if len(payload.Data) != 2 {
		t.Fatalf("len(data) = %d, want 2", len(payload.Data))
	}
	if payload.Data[0].Type != "trigger" {
		t.Fatalf("data[0].type = %q, want trigger", payload.Data[0].Type)
	}
}

func TestNodeTypeHandlerListRunnerNotConfigured(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router := setupNodeTypeRouter(nil)

	req := httptest.NewRequest(http.MethodGet, "/node-types", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", w.Code, http.StatusServiceUnavailable)
	}
}
