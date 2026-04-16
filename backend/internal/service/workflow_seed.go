package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"

	"github.com/liqiye/classifier/internal/repository"
)

const workflowSeedInitializedKey = "workflow_seed_initialized"

func SeedBuiltinWorkflows(
	ctx context.Context,
	workflowRepo repository.WorkflowDefinitionRepository,
	configRepo repository.ConfigRepository,
) error {
	seedMarker, err := configRepo.Get(ctx, workflowSeedInitializedKey)
	if err != nil && !errors.Is(err, repository.ErrNotFound) {
		return fmt.Errorf("seedBuiltinWorkflows read seed marker: %w", err)
	}
	if strings.TrimSpace(seedMarker) == "1" {
		return nil
	}

	items, _, err := workflowRepo.List(ctx, repository.WorkflowDefListFilter{Page: 1, Limit: 1})
	if err != nil {
		return fmt.Errorf("seedBuiltinWorkflows list definitions: %w", err)
	}
	if len(items) == 0 {
		if err := SeedDefaultWorkflow(ctx, workflowRepo); err != nil {
			return fmt.Errorf("seedBuiltinWorkflows seed default workflow: %w", err)
		}
		if err := SeedDefaultProcessingWorkflow(ctx, workflowRepo); err != nil {
			return fmt.Errorf("seedBuiltinWorkflows seed default processing workflow: %w", err)
		}
		if err := SeedGenericProcessingWorkflow(ctx, workflowRepo); err != nil {
			return fmt.Errorf("seedBuiltinWorkflows seed generic processing workflow: %w", err)
		}
	}

	if err := configRepo.Set(ctx, workflowSeedInitializedKey, "1"); err != nil {
		return fmt.Errorf("seedBuiltinWorkflows persist seed marker: %w", err)
	}

	return nil
}

func SeedDefaultWorkflow(ctx context.Context, repo repository.WorkflowDefinitionRepository) error {
	items, _, err := repo.List(ctx, repository.WorkflowDefListFilter{Limit: 1})
	if err != nil {
		return fmt.Errorf("seedDefaultWorkflow list definitions: %w", err)
	}
	if len(items) > 0 {
		return nil
	}

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{
				ID:      "n-trigger",
				Type:    "trigger",
				Config:  map[string]any{},
				Inputs:  map[string]repository.NodeInputSpec{},
				Enabled: true,
			},
			{
				ID:   "n-scanner",
				Type: "folder-tree-scanner",
				Config: map[string]any{
					"source_dir": "",
				},
				Inputs: map[string]repository.NodeInputSpec{
					"source_dir": {
						LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-trigger", SourcePort: "folder"},
					},
				},
				Enabled: true,
			},
			{
				ID:     "n-kw",
				Type:   "name-keyword-classifier",
				Config: map[string]any{},
				Inputs: map[string]repository.NodeInputSpec{
					"trees": {
						LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-scanner", SourcePort: "tree"},
					},
				},
				Enabled: true,
			},
			{
				ID:     "n-ft",
				Type:   "file-tree-classifier",
				Config: map[string]any{},
				Inputs: map[string]repository.NodeInputSpec{
					"trees": {
						LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-scanner", SourcePort: "tree"},
					},
				},
				Enabled: true,
			},
			{
				ID:     "n-ext",
				Type:   "ext-ratio-classifier",
				Config: map[string]any{},
				Inputs: map[string]repository.NodeInputSpec{
					"trees": {
						LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-scanner", SourcePort: "tree"},
					},
				},
				Enabled: true,
			},
			{
				ID:   "n-cc",
				Type: "confidence-check",
				Config: map[string]any{
					"threshold": 0.75,
				},
				Inputs: map[string]repository.NodeInputSpec{
					"signals": {
						LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-ext", SourcePort: "signal"},
					},
				},
				Enabled: true,
			},
			{
				ID:     "n-agg",
				Type:   "signal-aggregator",
				Config: map[string]any{},
				Inputs: map[string]repository.NodeInputSpec{
					"trees": {
						LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-scanner", SourcePort: "tree"},
					},
					"signal_kw": {
						LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-kw", SourcePort: "signal"},
					},
					"signal_ft": {
						LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-ft", SourcePort: "signal"},
					},
					"signal_ext": {
						LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-cc", SourcePort: "high"},
					},
				},
				Enabled: true,
			},
			{
				ID:     "n-writer",
				Type:   "classification-writer",
				Config: map[string]any{},
				Inputs: map[string]repository.NodeInputSpec{
					"entries": {
						LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-agg", SourcePort: "entries"},
					},
				},
				Enabled: true,
			},
		},
	}

	graphBytes, err := json.Marshal(graph)
	if err != nil {
		return fmt.Errorf("seedDefaultWorkflow marshal graph: %w", err)
	}

	if err := repo.Create(ctx, &repository.WorkflowDefinition{
		ID:        uuid.NewString(),
		Name:      "默认分类流程",
		GraphJSON: string(graphBytes),
		IsActive:  true,
		Version:   1,
	}); err != nil {
		return fmt.Errorf("seedDefaultWorkflow create workflow: %w", err)
	}

	return nil
}

func SeedDefaultProcessingWorkflow(ctx context.Context, repo repository.WorkflowDefinitionRepository) error {
	exists, err := workflowDefinitionExistsByName(ctx, repo, "default-processing")
	if err != nil {
		return fmt.Errorf("seedDefaultProcessingWorkflow check existing: %w", err)
	}
	if exists {
		return nil
	}

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{
				ID:      "p-reader",
				Type:    "db-subtree-reader",
				Config:  map[string]any{},
				Inputs:  map[string]repository.NodeInputSpec{},
				Enabled: true,
			},
			{
				ID:   "p-split",
				Type: "folder-splitter",
				Config: map[string]any{
					"split_mixed": true,
					"split_depth": 1,
				},
				Inputs: map[string]repository.NodeInputSpec{
					"entry": {
						LinkSource: &repository.NodeLinkSource{SourceNodeID: "p-reader", SourcePort: "entry"},
					},
				},
				Enabled: true,
			},
			{
				ID:      "p-router",
				Type:    "category-router",
				Config:  map[string]any{},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "p-split", SourcePort: "items"}}},
				Enabled: true,
			},
			{
				ID:   "p-rename",
				Type: "rename-node",
				Config: map[string]any{
					"strategy": "template",
					"template": "{name}",
				},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "p-router", SourcePort: "video"}}},
				Enabled: true,
			},
			{
				ID:   "p-compress",
				Type: "compress-node",
				Config: map[string]any{
					"scope":         "folder",
					"format":        "cbz",
					"path_ref_type": workflowPathRefTypeOutput,
					"path_ref_key":  "mixed",
					"path_suffix":   ".archives",
				},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "p-rename", SourcePort: "items"}}},
				Enabled: true,
			},
			{
				ID:   "p-move",
				Type: "move-node",
				Config: map[string]any{
					"path_ref_type":   workflowPathRefTypeOutput,
					"path_ref_key":    "mixed",
					"path_suffix":     ".processed",
					"move_unit":       "folder",
					"conflict_policy": "auto_rename",
				},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "p-compress", SourcePort: "items"}}},
				Enabled: true,
			},
			{
				ID:      "p-thumbnail",
				Type:    "thumbnail-node",
				Config:  map[string]any{},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "p-move", SourcePort: "items"}}},
				Enabled: true,
			},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e-reader-split", Source: "p-reader", SourcePort: "entry", Target: "p-split", TargetPort: "entry"},
			{ID: "e-split-router", Source: "p-split", SourcePort: "items", Target: "p-router", TargetPort: "items"},
			{ID: "e-router-rename", Source: "p-router", SourcePort: "video", Target: "p-rename", TargetPort: "items"},
			{ID: "e-rename-compress", Source: "p-rename", SourcePort: "items", Target: "p-compress", TargetPort: "items"},
			{ID: "e-compress-move", Source: "p-compress", SourcePort: "items", Target: "p-move", TargetPort: "items"},
			{ID: "e-move-thumbnail", Source: "p-move", SourcePort: "items", Target: "p-thumbnail", TargetPort: "items"},
		},
	}

	graphBytes, err := json.Marshal(graph)
	if err != nil {
		return fmt.Errorf("seedDefaultProcessingWorkflow marshal graph: %w", err)
	}

	if err := repo.Create(ctx, &repository.WorkflowDefinition{
		ID:          uuid.NewString(),
		Name:        "default-processing",
		Description: "Phase 4 processing workflow",
		GraphJSON:   string(graphBytes),
		IsActive:    true,
		Version:     1,
	}); err != nil {
		return fmt.Errorf("seedDefaultProcessingWorkflow create workflow: %w", err)
	}

	return nil
}

func SeedGenericProcessingWorkflow(ctx context.Context, repo repository.WorkflowDefinitionRepository) error {
	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{
				ID:      "g-reader",
				Type:    "db-subtree-reader",
				Config:  map[string]any{},
				Inputs:  map[string]repository.NodeInputSpec{},
				Enabled: true,
			},
			{
				ID:   "g-split",
				Type: "folder-splitter",
				Config: map[string]any{
					"split_mixed": true,
					"split_depth": 1,
				},
				Inputs: map[string]repository.NodeInputSpec{
					"entry": {
						LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-reader", SourcePort: "entry"},
					},
				},
				Enabled: true,
			},
			{
				ID:      "g-router",
				Type:    "category-router",
				Config:  map[string]any{},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-split", SourcePort: "items"}}},
				Enabled: true,
			},
			{
				ID:   "g-rename-video",
				Type: "rename-node",
				Config: map[string]any{
					"strategy": "template",
					"template": "{name}",
				},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-router", SourcePort: "video"}}},
				Enabled: true,
			},
			{
				ID:   "g-rename-manga",
				Type: "rename-node",
				Config: map[string]any{
					"strategy": "template",
					"template": "{name}",
				},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-router", SourcePort: "manga"}}},
				Enabled: true,
			},
			{
				ID:   "g-rename-photo",
				Type: "rename-node",
				Config: map[string]any{
					"strategy": "template",
					"template": "{name}",
				},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-router", SourcePort: "photo"}}},
				Enabled: true,
			},
			{
				ID:   "g-rename-other",
				Type: "rename-node",
				Config: map[string]any{
					"strategy": "template",
					"template": "{name}",
				},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-router", SourcePort: "other"}}},
				Enabled: true,
			},
			{
				ID:      "g-mixed-router",
				Type:    "mixed-leaf-router",
				Config:  map[string]any{},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-router", SourcePort: "mixed_leaf"}}},
				Enabled: true,
			},
			{
				ID:   "g-rename-mixed-video",
				Type: "rename-node",
				Config: map[string]any{
					"strategy": "template",
					"template": "{name}",
				},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-mixed-router", SourcePort: "video"}}},
				Enabled: true,
			},
			{
				ID:   "g-rename-mixed-photo",
				Type: "rename-node",
				Config: map[string]any{
					"strategy": "template",
					"template": "{name}",
				},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-mixed-router", SourcePort: "photo"}}},
				Enabled: true,
			},
			{
				ID:   "g-rename-mixed-other",
				Type: "rename-node",
				Config: map[string]any{
					"strategy": "template",
					"template": "{name}",
				},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-mixed-router", SourcePort: "unsupported"}}},
				Enabled: true,
			},
			{
				ID:     "g-collect",
				Type:   "collect-node",
				Config: map[string]any{},
				Inputs: map[string]repository.NodeInputSpec{
					"items_1": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-rename-video", SourcePort: "items"}},
					"items_2": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-rename-manga", SourcePort: "items"}},
					"items_3": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-rename-photo", SourcePort: "items"}},
					"items_4": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-rename-other", SourcePort: "items"}},
					"items_5": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-rename-mixed-video", SourcePort: "items"}},
					"items_6": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-rename-mixed-photo", SourcePort: "items"}},
					"items_7": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-rename-mixed-other", SourcePort: "items"}},
				},
				Enabled: true,
			},
			{
				ID:   "g-move",
				Type: "move-node",
				Config: map[string]any{
					"path_ref_type":   workflowPathRefTypeOutput,
					"path_ref_key":    "mixed",
					"path_suffix":     ".processed",
					"move_unit":       "folder",
					"conflict_policy": "auto_rename",
				},
				Inputs:  map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-collect", SourcePort: "items"}}},
				Enabled: true,
			},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e-reader-split", Source: "g-reader", SourcePort: "entry", Target: "g-split", TargetPort: "entry"},
			{ID: "e-split-router", Source: "g-split", SourcePort: "items", Target: "g-router", TargetPort: "items"},
			{ID: "e-router-rename-video", Source: "g-router", SourcePort: "video", Target: "g-rename-video", TargetPort: "items"},
			{ID: "e-router-rename-manga", Source: "g-router", SourcePort: "manga", Target: "g-rename-manga", TargetPort: "items"},
			{ID: "e-router-rename-photo", Source: "g-router", SourcePort: "photo", Target: "g-rename-photo", TargetPort: "items"},
			{ID: "e-router-rename-other", Source: "g-router", SourcePort: "other", Target: "g-rename-other", TargetPort: "items"},
			{ID: "e-router-mixed-router", Source: "g-router", SourcePort: "mixed_leaf", Target: "g-mixed-router", TargetPort: "items"},
			{ID: "e-mixed-router-rename-video", Source: "g-mixed-router", SourcePort: "video", Target: "g-rename-mixed-video", TargetPort: "items"},
			{ID: "e-mixed-router-rename-photo", Source: "g-mixed-router", SourcePort: "photo", Target: "g-rename-mixed-photo", TargetPort: "items"},
			{ID: "e-mixed-router-rename-other", Source: "g-mixed-router", SourcePort: "unsupported", Target: "g-rename-mixed-other", TargetPort: "items"},
			{ID: "e-rename-video-collect", Source: "g-rename-video", SourcePort: "items", Target: "g-collect", TargetPort: "items_1"},
			{ID: "e-rename-manga-collect", Source: "g-rename-manga", SourcePort: "items", Target: "g-collect", TargetPort: "items_2"},
			{ID: "e-rename-photo-collect", Source: "g-rename-photo", SourcePort: "items", Target: "g-collect", TargetPort: "items_3"},
			{ID: "e-rename-other-collect", Source: "g-rename-other", SourcePort: "items", Target: "g-collect", TargetPort: "items_4"},
			{ID: "e-rename-mixed-video-collect", Source: "g-rename-mixed-video", SourcePort: "items", Target: "g-collect", TargetPort: "items_5"},
			{ID: "e-rename-mixed-photo-collect", Source: "g-rename-mixed-photo", SourcePort: "items", Target: "g-collect", TargetPort: "items_6"},
			{ID: "e-rename-mixed-other-collect", Source: "g-rename-mixed-other", SourcePort: "items", Target: "g-collect", TargetPort: "items_7"},
			{ID: "e-collect-move", Source: "g-collect", SourcePort: "items", Target: "g-move", TargetPort: "items"},
		},
	}

	graphBytes, err := json.Marshal(graph)
	if err != nil {
		return fmt.Errorf("seedGenericProcessingWorkflow marshal graph: %w", err)
	}

	existing, err := workflowDefinitionByName(ctx, repo, "通用处理流程")
	if err != nil {
		return fmt.Errorf("seedGenericProcessingWorkflow get existing workflow: %w", err)
	}
	if existing != nil {
		// Keep user-edited workflow definitions intact across restarts.
		return nil
	}

	if err := repo.Create(ctx, &repository.WorkflowDefinition{
		ID:          uuid.NewString(),
		Name:        "通用处理流程",
		Description: "按类别路由并统一收集合并后执行重命名与移动",
		GraphJSON:   string(graphBytes),
		IsActive:    false,
		Version:     1,
	}); err != nil {
		return fmt.Errorf("seedGenericProcessingWorkflow create workflow: %w", err)
	}

	return nil
}

func workflowDefinitionExistsByName(ctx context.Context, repo repository.WorkflowDefinitionRepository, name string) (bool, error) {
	item, err := workflowDefinitionByName(ctx, repo, name)
	if err != nil {
		return false, err
	}
	return item != nil, nil
}

func workflowDefinitionByName(ctx context.Context, repo repository.WorkflowDefinitionRepository, name string) (*repository.WorkflowDefinition, error) {
	page := 1
	for {
		items, total, err := repo.List(ctx, repository.WorkflowDefListFilter{Page: page, Limit: 100})
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			if strings.EqualFold(strings.TrimSpace(item.Name), strings.TrimSpace(name)) {
				return item, nil
			}
		}

		if len(items) == 0 || page*100 >= total {
			return nil, nil
		}
		page++
	}
}
