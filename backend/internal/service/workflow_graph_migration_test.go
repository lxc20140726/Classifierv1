package service

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/liqiye/classifier/internal/repository"
)

func TestNormalizeWorkflowDefinitionGraphs_RemovesAuditAndStepResults(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "n-rename", Type: "rename-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-reader", SourcePort: "items"}},
			}},
			{ID: "n-collect", Type: "collect-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items_1":        {LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-rename", SourcePort: "items"}},
				"step_results_1": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-rename", SourcePort: "step_results"}},
			}},
			{ID: "n-move", Type: "move-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items":        {LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-collect", SourcePort: "items"}},
				"step_results": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-collect", SourcePort: "step_results"}},
			}},
			{ID: "n-audit", Type: "audit-log", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "n-move", SourcePort: "items"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e1", Source: "n-rename", SourcePort: "items", Target: "n-collect", TargetPort: "items_1"},
			{ID: "e2", Source: "n-rename", SourcePort: "step_results", Target: "n-collect", TargetPort: "step_results_1"},
			{ID: "e3", Source: "n-collect", SourcePort: "items", Target: "n-move", TargetPort: "items"},
			{ID: "e4", Source: "n-collect", SourcePort: "step_results", Target: "n-move", TargetPort: "step_results"},
			{ID: "e5", Source: "n-move", SourcePort: "items", Target: "n-audit", TargetPort: "items"},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{
		ID:        "wf-graph-migration",
		Name:      "wf-graph-migration",
		GraphJSON: string(graphJSON),
		IsActive:  true,
		Version:   1,
	}
	if err := repo.Create(ctx, def); err != nil {
		t.Fatalf("repo.Create() error = %v", err)
	}

	if err := NormalizeWorkflowDefinitionGraphs(ctx, repo); err != nil {
		t.Fatalf("NormalizeWorkflowDefinitionGraphs() error = %v", err)
	}

	updated, err := repo.GetByID(ctx, def.ID)
	if err != nil {
		t.Fatalf("repo.GetByID() error = %v", err)
	}

	var normalized repository.WorkflowGraph
	if err := json.Unmarshal([]byte(updated.GraphJSON), &normalized); err != nil {
		t.Fatalf("json.Unmarshal(normalized) error = %v", err)
	}

	for _, node := range normalized.Nodes {
		if node.Type == "audit-log" {
			t.Fatalf("normalized graph still has audit-log node")
		}
		for inputName, spec := range node.Inputs {
			if strings.HasPrefix(inputName, "step_results") {
				t.Fatalf("input %q should be removed", inputName)
			}
			if spec.LinkSource != nil && strings.HasPrefix(spec.LinkSource.SourcePort, "step_results") {
				t.Fatalf("link source port %q should be removed", spec.LinkSource.SourcePort)
			}
		}
	}
	for _, edge := range normalized.Edges {
		if edge.Source == "n-audit" || edge.Target == "n-audit" {
			t.Fatalf("edge connected to removed audit node still exists: %+v", edge)
		}
		if strings.HasPrefix(edge.SourcePort, "step_results") || strings.HasPrefix(edge.TargetPort, "step_results") {
			t.Fatalf("step_results edge should be removed: %+v", edge)
		}
	}
}

func TestNormalizeWorkflowDefinitionGraphs_IsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "n1", Type: "collect-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items_1": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "n0", SourcePort: "items"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e1", Source: "n0", SourcePort: "items", Target: "n1", TargetPort: "items_1"},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}
	def := &repository.WorkflowDefinition{ID: "wf-graph-idempotent", Name: "wf-graph-idempotent", GraphJSON: string(graphJSON), IsActive: true, Version: 1}
	if err := repo.Create(ctx, def); err != nil {
		t.Fatalf("repo.Create() error = %v", err)
	}

	if err := NormalizeWorkflowDefinitionGraphs(ctx, repo); err != nil {
		t.Fatalf("first NormalizeWorkflowDefinitionGraphs() error = %v", err)
	}
	first, err := repo.GetByID(ctx, def.ID)
	if err != nil {
		t.Fatalf("repo.GetByID(first) error = %v", err)
	}
	firstJSON := first.GraphJSON

	if err := NormalizeWorkflowDefinitionGraphs(ctx, repo); err != nil {
		t.Fatalf("second NormalizeWorkflowDefinitionGraphs() error = %v", err)
	}
	second, err := repo.GetByID(ctx, def.ID)
	if err != nil {
		t.Fatalf("repo.GetByID(second) error = %v", err)
	}

	if second.GraphJSON != firstJSON {
		t.Fatalf("graph json changed after second normalization")
	}
}

func TestNormalizeWorkflowDefinitionGraphs_NormalizesBuiltinDefaultProcessingFlow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "p-router", Type: "category-router", Enabled: true},
			{ID: "p-rename", Type: "rename-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "p-thumbnail", SourcePort: "items"}},
			}},
			{ID: "p-compress", Type: "compress-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "p-rename", SourcePort: "items"}},
			}},
			{ID: "p-move", Type: "move-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "p-compress", SourcePort: "items"}},
			}},
			{ID: "p-thumbnail", Type: "thumbnail-node", Enabled: true, Config: map[string]any{
				"path_ref_type": "output",
				"path_ref_key":  "video",
				"path_suffix":   ".thumbnails",
			}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "p-router", SourcePort: "video"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e-router-thumbnail", Source: "p-router", SourcePort: "video", Target: "p-thumbnail", TargetPort: "items"},
			{ID: "e-thumbnail-rename", Source: "p-thumbnail", SourcePort: "items", Target: "p-rename", TargetPort: "items"},
			{ID: "e-rename-compress", Source: "p-rename", SourcePort: "items", Target: "p-compress", TargetPort: "items"},
			{ID: "e-compress-move", Source: "p-compress", SourcePort: "items", Target: "p-move", TargetPort: "items"},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{
		ID:        "wf-default-processing-migration",
		Name:      "default-processing",
		GraphJSON: string(graphJSON),
		IsActive:  true,
		Version:   1,
	}
	if err := repo.Create(ctx, def); err != nil {
		t.Fatalf("repo.Create() error = %v", err)
	}

	if err := NormalizeWorkflowDefinitionGraphs(ctx, repo); err != nil {
		t.Fatalf("NormalizeWorkflowDefinitionGraphs() error = %v", err)
	}

	updated, err := repo.GetByID(ctx, def.ID)
	if err != nil {
		t.Fatalf("repo.GetByID() error = %v", err)
	}
	var normalized repository.WorkflowGraph
	if err := json.Unmarshal([]byte(updated.GraphJSON), &normalized); err != nil {
		t.Fatalf("json.Unmarshal(updated.GraphJSON) error = %v", err)
	}

	nodeByID := map[string]repository.WorkflowGraphNode{}
	for _, node := range normalized.Nodes {
		nodeByID[node.ID] = node
	}
	thumbnailNode := nodeByID["p-thumbnail"]
	if len(thumbnailNode.Config) != 0 {
		t.Fatalf("thumbnail config = %#v, want empty for built-in default flow", thumbnailNode.Config)
	}
	if link := thumbnailNode.Inputs["items"].LinkSource; link == nil || link.SourceNodeID != "p-move" || link.SourcePort != "items" {
		t.Fatalf("thumbnail items link = %#v, want p-move.items", link)
	}
	if link := nodeByID["p-rename"].Inputs["items"].LinkSource; link == nil || link.SourceNodeID != "p-router" || link.SourcePort != "video" {
		t.Fatalf("rename items link = %#v, want p-router.video", link)
	}
}

func TestNormalizeWorkflowDefinitionGraphs_DoesNotForceBuiltinRulesOnCustomWorkflow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "p-router", Type: "category-router", Enabled: true},
			{ID: "p-thumbnail", Type: "thumbnail-node", Enabled: true, Config: map[string]any{
				"output_dir": "/custom/thumbs",
			}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "p-router", SourcePort: "video"}},
			}},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{
		ID:        "wf-custom-thumb-path",
		Name:      "custom-processing",
		GraphJSON: string(graphJSON),
		IsActive:  true,
		Version:   1,
	}
	if err := repo.Create(ctx, def); err != nil {
		t.Fatalf("repo.Create() error = %v", err)
	}

	if err := NormalizeWorkflowDefinitionGraphs(ctx, repo); err != nil {
		t.Fatalf("NormalizeWorkflowDefinitionGraphs() error = %v", err)
	}

	updated, err := repo.GetByID(ctx, def.ID)
	if err != nil {
		t.Fatalf("repo.GetByID() error = %v", err)
	}
	var normalized repository.WorkflowGraph
	if err := json.Unmarshal([]byte(updated.GraphJSON), &normalized); err != nil {
		t.Fatalf("json.Unmarshal(updated.GraphJSON) error = %v", err)
	}

	nodeByID := map[string]repository.WorkflowGraphNode{}
	for _, node := range normalized.Nodes {
		nodeByID[node.ID] = node
	}
	thumbnailNode := nodeByID["p-thumbnail"]
	if link := thumbnailNode.Inputs["items"].LinkSource; link == nil || link.SourceNodeID != "p-router" || link.SourcePort != "video" {
		t.Fatalf("thumbnail items link = %#v, want unchanged p-router.video", link)
	}
	if got := strings.TrimSpace(stringConfig(thumbnailNode.Config, "path_ref_type")); got != workflowPathRefTypeCustom {
		t.Fatalf("thumbnail path_ref_type = %q, want %q", got, workflowPathRefTypeCustom)
	}
	if got := normalizeWorkflowPath(stringConfig(thumbnailNode.Config, "path_ref_key")); got != "/custom/thumbs" {
		t.Fatalf("thumbnail path_ref_key = %q, want %q", got, "/custom/thumbs")
	}
}

func TestNormalizeWorkflowDefinitionGraphs_LeavesBlankThumbnailConfigUnchanged(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "p-router", Type: "category-router", Enabled: true},
			{ID: "p-thumbnail", Type: "thumbnail-node", Enabled: true, Config: map[string]any{}, Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "p-router", SourcePort: "video"}},
			}},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{
		ID:        "wf-blank-thumb-config",
		Name:      "custom-processing-no-thumb-path",
		GraphJSON: string(graphJSON),
		IsActive:  true,
		Version:   1,
	}
	if err := repo.Create(ctx, def); err != nil {
		t.Fatalf("repo.Create() error = %v", err)
	}

	if err := NormalizeWorkflowDefinitionGraphs(ctx, repo); err != nil {
		t.Fatalf("NormalizeWorkflowDefinitionGraphs() error = %v", err)
	}

	updated, err := repo.GetByID(ctx, def.ID)
	if err != nil {
		t.Fatalf("repo.GetByID() error = %v", err)
	}
	var normalized repository.WorkflowGraph
	if err := json.Unmarshal([]byte(updated.GraphJSON), &normalized); err != nil {
		t.Fatalf("json.Unmarshal(updated.GraphJSON) error = %v", err)
	}

	nodeByID := map[string]repository.WorkflowGraphNode{}
	for _, node := range normalized.Nodes {
		nodeByID[node.ID] = node
	}
	thumbnailNode := nodeByID["p-thumbnail"]
	if len(thumbnailNode.Config) != 0 {
		t.Fatalf("thumbnail config = %#v, want empty", thumbnailNode.Config)
	}
}

func TestNormalizeWorkflowDefinitionGraphs_MigratesLegacyMovePathOption(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{
				ID:      "move",
				Type:    "move-node",
				Enabled: true,
				Config: map[string]any{
					"target_dir_source":    "output",
					"target_dir_option_id": "photo:1",
				},
			},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{
		ID:        "wf-migrate-move-legacy-option",
		Name:      "wf-migrate-move-legacy-option",
		GraphJSON: string(graphJSON),
		IsActive:  true,
		Version:   1,
	}
	if err := repo.Create(ctx, def); err != nil {
		t.Fatalf("repo.Create() error = %v", err)
	}

	if err := NormalizeWorkflowDefinitionGraphs(ctx, repo); err != nil {
		t.Fatalf("NormalizeWorkflowDefinitionGraphs() error = %v", err)
	}

	updated, err := repo.GetByID(ctx, def.ID)
	if err != nil {
		t.Fatalf("repo.GetByID() error = %v", err)
	}
	var normalized repository.WorkflowGraph
	if err := json.Unmarshal([]byte(updated.GraphJSON), &normalized); err != nil {
		t.Fatalf("json.Unmarshal(updated.GraphJSON) error = %v", err)
	}
	if len(normalized.Nodes) != 1 {
		t.Fatalf("len(normalized.Nodes) = %d, want 1", len(normalized.Nodes))
	}

	moveNode := normalized.Nodes[0]
	if got := strings.TrimSpace(stringConfig(moveNode.Config, "path_ref_type")); got != workflowPathRefTypeOutput {
		t.Fatalf("move path_ref_type = %q, want %q", got, workflowPathRefTypeOutput)
	}
	if got := strings.TrimSpace(stringConfig(moveNode.Config, "path_ref_key")); got != "photo:1" {
		t.Fatalf("move path_ref_key = %q, want %q", got, "photo:1")
	}
	if _, ok := moveNode.Config["target_dir_source"]; ok {
		t.Fatalf("legacy key target_dir_source should be removed")
	}
	if _, ok := moveNode.Config["target_dir_option_id"]; ok {
		t.Fatalf("legacy key target_dir_option_id should be removed")
	}
}

func TestNormalizeWorkflowDefinitionGraphs_ModernMovePathRefWinsOverLegacyPath(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{
				ID:      "move",
				Type:    "move-node",
				Enabled: true,
				Config: map[string]any{
					"path_ref_type": workflowPathRefTypeOutput,
					"path_ref_key":  "video:0",
					"target_dir":    "/target/mixed",
				},
			},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{
		ID:        "wf-modern-path-ref-wins",
		Name:      "wf-modern-path-ref-wins",
		GraphJSON: string(graphJSON),
		IsActive:  true,
		Version:   1,
	}
	if err := repo.Create(ctx, def); err != nil {
		t.Fatalf("repo.Create() error = %v", err)
	}

	if err := NormalizeWorkflowDefinitionGraphs(ctx, repo); err != nil {
		t.Fatalf("NormalizeWorkflowDefinitionGraphs() error = %v", err)
	}

	updated, err := repo.GetByID(ctx, def.ID)
	if err != nil {
		t.Fatalf("repo.GetByID() error = %v", err)
	}
	var normalized repository.WorkflowGraph
	if err := json.Unmarshal([]byte(updated.GraphJSON), &normalized); err != nil {
		t.Fatalf("json.Unmarshal(updated.GraphJSON) error = %v", err)
	}

	moveNode := normalized.Nodes[0]
	if got := strings.TrimSpace(stringConfig(moveNode.Config, "path_ref_type")); got != workflowPathRefTypeOutput {
		t.Fatalf("move path_ref_type = %q, want %q", got, workflowPathRefTypeOutput)
	}
	if got := strings.TrimSpace(stringConfig(moveNode.Config, "path_ref_key")); got != "video:0" {
		t.Fatalf("move path_ref_key = %q, want video:0", got)
	}
	if _, ok := moveNode.Config["target_dir"]; ok {
		t.Fatalf("legacy key target_dir should be removed")
	}
}

func TestNormalizeWorkflowDefinitionGraphs_AddsGenericMixedOtherBranch(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	graph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "g-mixed-router", Type: "mixed-leaf-router", Enabled: true},
			{ID: "g-rename-mixed-video", Type: "rename-node", Enabled: true},
			{ID: "g-rename-mixed-photo", Type: "rename-node", Enabled: true},
			{ID: "g-collect", Type: "collect-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
				"items_5": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-rename-mixed-video", SourcePort: "items"}},
				"items_6": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-rename-mixed-photo", SourcePort: "items"}},
			}},
		},
		Edges: []repository.WorkflowGraphEdge{
			{ID: "e-mixed-router-rename-video", Source: "g-mixed-router", SourcePort: "video", Target: "g-rename-mixed-video", TargetPort: "items"},
			{ID: "e-mixed-router-rename-photo", Source: "g-mixed-router", SourcePort: "photo", Target: "g-rename-mixed-photo", TargetPort: "items"},
			{ID: "e-rename-mixed-video-collect", Source: "g-rename-mixed-video", SourcePort: "items", Target: "g-collect", TargetPort: "items_5"},
			{ID: "e-rename-mixed-photo-collect", Source: "g-rename-mixed-photo", SourcePort: "items", Target: "g-collect", TargetPort: "items_6"},
		},
	}
	graphJSON, err := json.Marshal(graph)
	if err != nil {
		t.Fatalf("json.Marshal(graph) error = %v", err)
	}

	def := &repository.WorkflowDefinition{
		ID:        "wf-generic-mixed-other",
		Name:      "通用处理流程",
		GraphJSON: string(graphJSON),
		IsActive:  false,
		Version:   1,
	}
	if err := repo.Create(ctx, def); err != nil {
		t.Fatalf("repo.Create() error = %v", err)
	}

	if err := NormalizeWorkflowDefinitionGraphs(ctx, repo); err != nil {
		t.Fatalf("NormalizeWorkflowDefinitionGraphs() error = %v", err)
	}

	updated, err := repo.GetByID(ctx, def.ID)
	if err != nil {
		t.Fatalf("repo.GetByID() error = %v", err)
	}
	var normalized repository.WorkflowGraph
	if err := json.Unmarshal([]byte(updated.GraphJSON), &normalized); err != nil {
		t.Fatalf("json.Unmarshal(normalized) error = %v", err)
	}

	nodeByID := map[string]repository.WorkflowGraphNode{}
	for _, node := range normalized.Nodes {
		nodeByID[node.ID] = node
	}
	otherNode, ok := nodeByID["g-rename-mixed-other"]
	if !ok {
		t.Fatalf("g-rename-mixed-other node missing")
	}
	if otherNode.Inputs["items"].LinkSource == nil ||
		otherNode.Inputs["items"].LinkSource.SourceNodeID != "g-mixed-router" ||
		otherNode.Inputs["items"].LinkSource.SourcePort != "unsupported" {
		t.Fatalf("g-rename-mixed-other items input = %#v", otherNode.Inputs["items"])
	}
	collectNode := nodeByID["g-collect"]
	if collectNode.Inputs["items_7"].LinkSource == nil ||
		collectNode.Inputs["items_7"].LinkSource.SourceNodeID != "g-rename-mixed-other" {
		t.Fatalf("g-collect items_7 input = %#v", collectNode.Inputs["items_7"])
	}

	hasUnsupportedEdge := false
	hasCollectEdge := false
	for _, edge := range normalized.Edges {
		if edge.Source == "g-mixed-router" && edge.SourcePort == "unsupported" && edge.Target == "g-rename-mixed-other" {
			hasUnsupportedEdge = true
		}
		if edge.Source == "g-rename-mixed-other" && edge.Target == "g-collect" && edge.TargetPort == "items_7" {
			hasCollectEdge = true
		}
	}
	if !hasUnsupportedEdge || !hasCollectEdge {
		t.Fatalf("missing mixed other edges: unsupported=%v collect=%v edges=%#v", hasUnsupportedEdge, hasCollectEdge, normalized.Edges)
	}
}
