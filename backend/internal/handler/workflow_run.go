package handler

import (
	"context"
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/service"
)

type WorkflowRunReader interface {
	ListWorkflowRuns(ctx context.Context, jobID string, page, limit int) ([]*repository.WorkflowRun, int, error)
	GetWorkflowRunDetail(ctx context.Context, workflowRunID string) (*service.WorkflowRunDetail, error)
	ListProcessingReviews(ctx context.Context, workflowRunID string) (*service.ProcessingReviewList, error)
	ApproveProcessingReview(ctx context.Context, workflowRunID, reviewID string) error
	RollbackProcessingReview(ctx context.Context, workflowRunID, reviewID string) error
	ApproveAllPendingProcessingReviews(ctx context.Context, workflowRunID string) (int, error)
	RollbackAllPendingProcessingReviews(ctx context.Context, workflowRunID string) (int, error)
	ResumeWorkflowRun(ctx context.Context, workflowRunID string) error
	ResumeWorkflowRunWithData(ctx context.Context, workflowRunID string, resumeData map[string]any) error
	RollbackWorkflowRun(ctx context.Context, workflowRunID string) error
}

type provideInputRequest struct {
	Category string `json:"category"`
}

type WorkflowRunHandler struct {
	runner WorkflowRunReader
}

func NewWorkflowRunHandler(runner WorkflowRunReader) *WorkflowRunHandler {
	return &WorkflowRunHandler{runner: runner}
}

func (h *WorkflowRunHandler) ListByJob(c *gin.Context) {
	page := 1
	if rawPage := c.Query("page"); rawPage != "" {
		parsedPage, err := strconv.Atoi(rawPage)
		if err != nil || parsedPage <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid page"})
			return
		}
		page = parsedPage
	}

	limit := 20
	if rawLimit := c.Query("limit"); rawLimit != "" {
		parsedLimit, err := strconv.Atoi(rawLimit)
		if err != nil || parsedLimit <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit"})
			return
		}
		limit = parsedLimit
	}

	items, total, err := h.runner.ListWorkflowRuns(c.Request.Context(), c.Param("id"), page, limit)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list workflow runs"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": items, "total": total, "page": page, "limit": limit})
}

func (h *WorkflowRunHandler) Get(c *gin.Context) {
	detail, err := h.runner.GetWorkflowRunDetail(c.Request.Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "workflow run not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get workflow run"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": detail.Run, "node_runs": detail.NodeRuns, "review_summary": detail.ReviewSummary})
}

func (h *WorkflowRunHandler) Resume(c *gin.Context) {
	if err := h.runner.ResumeWorkflowRun(c.Request.Context(), c.Param("id")); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "workflow run not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to resume workflow run"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"resumed": true})
}

func (h *WorkflowRunHandler) Rollback(c *gin.Context) {
	if err := h.runner.RollbackWorkflowRun(c.Request.Context(), c.Param("id")); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "workflow run not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to rollback workflow run"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"rolled_back": true})
}

func (h *WorkflowRunHandler) ProvideInput(c *gin.Context) {
	id := c.Param("id")

	var req provideInputRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	validCategories := map[string]struct{}{
		"photo": {},
		"video": {},
		"manga": {},
		"mixed": {},
		"other": {},
	}
	if _, ok := validCategories[req.Category]; !ok {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid category"})
		return
	}

	if err := h.runner.ResumeWorkflowRunWithData(c.Request.Context(), id, map[string]any{"category": req.Category}); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.Status(http.StatusNoContent)
}

func (h *WorkflowRunHandler) ListReviews(c *gin.Context) {
	resp, err := h.runner.ListProcessingReviews(c.Request.Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "workflow run not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list processing reviews"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": resp.Items, "summary": resp.Summary})
}

func (h *WorkflowRunHandler) ApproveReview(c *gin.Context) {
	if err := h.runner.ApproveProcessingReview(c.Request.Context(), c.Param("id"), c.Param("reviewId")); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "review item not found"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"approved": true})
}

func (h *WorkflowRunHandler) RollbackReview(c *gin.Context) {
	if err := h.runner.RollbackProcessingReview(c.Request.Context(), c.Param("id"), c.Param("reviewId")); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "review item not found"})
			return
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"rolled_back": true})
}

func (h *WorkflowRunHandler) ApproveAllReviews(c *gin.Context) {
	approved, err := h.runner.ApproveAllPendingProcessingReviews(c.Request.Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "workflow run not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to approve pending reviews"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"approved": approved})
}

func (h *WorkflowRunHandler) RollbackAllReviews(c *gin.Context) {
	rolledBack, err := h.runner.RollbackAllPendingProcessingReviews(c.Request.Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "workflow run not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to rollback pending reviews"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"rolled_back": rolledBack})
}
