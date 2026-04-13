package handler

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/liqiye/classifier/internal/repository"
)

type WorkflowDefHandler struct {
	repo       repository.WorkflowDefinitionRepository
	validator  GraphValidator
	normalizer GraphDefinitionNormalizer
}

type GraphValidator interface {
	ValidateWorkflowGraph(graphJSON string) error
}

type GraphDefinitionNormalizer interface {
	NormalizeWorkflowDefinitionGraph(name, graphJSON string) (string, error)
}

func NewWorkflowDefHandler(repo repository.WorkflowDefinitionRepository, validator GraphValidator) *WorkflowDefHandler {
	return &WorkflowDefHandler{repo: repo, validator: validator}
}

func (h *WorkflowDefHandler) SetGraphDefinitionNormalizer(normalizer GraphDefinitionNormalizer) {
	h.normalizer = normalizer
}

func (h *WorkflowDefHandler) List(c *gin.Context) {
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

	items, total, err := h.repo.List(c.Request.Context(), repository.WorkflowDefListFilter{Page: page, Limit: limit})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list workflow definitions"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": items, "total": total, "page": page, "limit": limit})
}

func (h *WorkflowDefHandler) Create(c *gin.Context) {
	var req struct {
		Name      string `json:"name"`
		GraphJSON string `json:"graph_json"`
		IsActive  *bool  `json:"is_active"`
		Version   int    `json:"version"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	if req.Name == "" || req.GraphJSON == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "name and graph_json are required"})
		return
	}
	graphJSON := req.GraphJSON
	if h.normalizer != nil {
		normalized, err := h.normalizer.NormalizeWorkflowDefinitionGraph(req.Name, graphJSON)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		graphJSON = normalized
	}
	if h.validator != nil {
		if err := h.validator.ValidateWorkflowGraph(graphJSON); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
	}

	isActive := true
	if req.IsActive != nil {
		isActive = *req.IsActive
	}
	version := req.Version
	if version <= 0 {
		version = 1
	}

	item := &repository.WorkflowDefinition{
		ID:        uuid.NewString(),
		Name:      req.Name,
		GraphJSON: graphJSON,
		IsActive:  isActive,
		Version:   version,
	}

	if err := h.repo.Create(c.Request.Context(), item); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create workflow definition"})
		return
	}

	created, err := h.repo.GetByID(c.Request.Context(), item.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load workflow definition"})
		return
	}

	c.JSON(http.StatusCreated, gin.H{"data": created})
}

func (h *WorkflowDefHandler) Get(c *gin.Context) {
	item, err := h.repo.GetByID(c.Request.Context(), c.Param("id"))
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "workflow definition not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get workflow definition"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": item})
}

func (h *WorkflowDefHandler) Update(c *gin.Context) {
	id := c.Param("id")
	item, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "workflow definition not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get workflow definition"})
		return
	}

	var req struct {
		Name      string `json:"name"`
		GraphJSON string `json:"graph_json"`
		IsActive  *bool  `json:"is_active"`
		Version   int    `json:"version"`
	}
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}

	if req.Name != "" {
		item.Name = req.Name
	}
	if req.GraphJSON != "" {
		graphJSON := req.GraphJSON
		if h.normalizer != nil {
			normalized, normalizeErr := h.normalizer.NormalizeWorkflowDefinitionGraph(item.Name, graphJSON)
			if normalizeErr != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": normalizeErr.Error()})
				return
			}
			graphJSON = normalized
		}
		if h.validator != nil {
			if err := h.validator.ValidateWorkflowGraph(graphJSON); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
		}
		item.GraphJSON = graphJSON
	}
	if req.IsActive != nil {
		item.IsActive = *req.IsActive
	}
	if req.Version > 0 {
		item.Version = req.Version
	}

	if err := h.repo.Update(c.Request.Context(), item); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update workflow definition"})
		return
	}

	updated, err := h.repo.GetByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get workflow definition"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": updated})
}

func (h *WorkflowDefHandler) Delete(c *gin.Context) {
	if err := h.repo.Delete(c.Request.Context(), c.Param("id")); err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "workflow definition not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete workflow definition"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"deleted": true})
}
