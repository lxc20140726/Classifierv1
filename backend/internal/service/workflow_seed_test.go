package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/liqiye/classifier/internal/repository"
)

func TestSeedDefaultWorkflow_CreatesExpectedGraphAndIsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	if err := SeedDefaultWorkflow(ctx, repo); err != nil {
		t.Fatalf("SeedDefaultWorkflow() error = %v", err)
	}

	items, total, err := repo.List(ctx, repository.WorkflowDefListFilter{Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("repo.List() error = %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("workflow count = total:%d len:%d, want 1", total, len(items))
	}

	seeded := items[0]
	if seeded.Name != "默认分类流程" {
		t.Fatalf("seeded.Name = %q, want 默认分类流程", seeded.Name)
	}
	if !seeded.IsActive {
		t.Fatalf("seeded.IsActive = false, want true")
	}
	if seeded.Version != 1 {
		t.Fatalf("seeded.Version = %d, want 1", seeded.Version)
	}

	var graph repository.WorkflowGraph
	if err := json.Unmarshal([]byte(seeded.GraphJSON), &graph); err != nil {
		t.Fatalf("json.Unmarshal(GraphJSON) error = %v", err)
	}

	gotTypes := make([]string, 0, len(graph.Nodes))
	for _, node := range graph.Nodes {
		gotTypes = append(gotTypes, node.Type)
	}
	wantTypes := []string{
		"trigger",
		"folder-tree-scanner",
		"name-keyword-classifier",
		"file-tree-classifier",
		"ext-ratio-classifier",
		"confidence-check",
		"signal-aggregator",
		"classification-writer",
	}
	if len(gotTypes) != len(wantTypes) {
		t.Fatalf("node count = %d, want %d; got %v", len(gotTypes), len(wantTypes), gotTypes)
	}
	for i, want := range wantTypes {
		if gotTypes[i] != want {
			t.Fatalf("node[%d].Type = %q, want %q; full=%v", i, gotTypes[i], want, gotTypes)
		}
	}
	for _, node := range graph.Nodes {
		for inputName, spec := range node.Inputs {
			if spec.LinkSource == nil {
				continue
			}
			if spec.LinkSource.SourcePort == "" {
				t.Fatalf("default graph node %s input %s source_port is empty", node.ID, inputName)
			}
			if spec.LinkSource.OutputPortIndex != 0 {
				t.Fatalf("default graph node %s input %s still uses output_port_index=%d", node.ID, inputName, spec.LinkSource.OutputPortIndex)
			}
		}
	}

	if err := SeedDefaultWorkflow(ctx, repo); err != nil {
		t.Fatalf("second SeedDefaultWorkflow() error = %v", err)
	}
	items, total, err = repo.List(ctx, repository.WorkflowDefListFilter{Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("repo.List() after second seed error = %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("after second seed workflow count = total:%d len:%d, want 1", total, len(items))
	}
	if items[0].ID != seeded.ID {
		t.Fatalf("seeded workflow id changed from %q to %q on second seed", seeded.ID, items[0].ID)
	}
}

func TestSeedDefaultProcessingWorkflow_CreatesExpectedGraphAndIsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	if err := SeedDefaultProcessingWorkflow(ctx, repo); err != nil {
		t.Fatalf("SeedDefaultProcessingWorkflow() error = %v", err)
	}

	items, total, err := repo.List(ctx, repository.WorkflowDefListFilter{Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("repo.List() error = %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("workflow count = total:%d len:%d, want 1", total, len(items))
	}

	seeded := items[0]
	if seeded.Name != "default-processing" {
		t.Fatalf("seeded.Name = %q, want default-processing", seeded.Name)
	}
	if !seeded.IsActive {
		t.Fatalf("seeded.IsActive = false, want true")
	}
	if seeded.Version != 1 {
		t.Fatalf("seeded.Version = %d, want 1", seeded.Version)
	}

	var graph repository.WorkflowGraph
	if err := json.Unmarshal([]byte(seeded.GraphJSON), &graph); err != nil {
		t.Fatalf("json.Unmarshal(GraphJSON) error = %v", err)
	}

	gotTypes := make([]string, 0, len(graph.Nodes))
	for _, node := range graph.Nodes {
		gotTypes = append(gotTypes, node.Type)
	}
	wantTypes := []string{
		"db-subtree-reader",
		"folder-splitter",
		"category-router",
		"rename-node",
		"compress-node",
		"move-node",
		"thumbnail-node",
	}
	if len(gotTypes) != len(wantTypes) {
		t.Fatalf("node count = %d, want %d; got %v", len(gotTypes), len(wantTypes), gotTypes)
	}
	for i, want := range wantTypes {
		if gotTypes[i] != want {
			t.Fatalf("node[%d].Type = %q, want %q; full=%v", i, gotTypes[i], want, gotTypes)
		}
	}
	nodeByID := map[string]repository.WorkflowGraphNode{}
	for _, node := range graph.Nodes {
		nodeByID[node.ID] = node
	}
	thumbnailNode, ok := nodeByID["p-thumbnail"]
	if !ok {
		t.Fatalf("graph missing node p-thumbnail")
	}
	if len(thumbnailNode.Config) != 0 {
		t.Fatalf("p-thumbnail config = %#v, want empty config for default same-directory output", thumbnailNode.Config)
	}
	thumbnailInput := thumbnailNode.Inputs["items"]
	if thumbnailInput.LinkSource == nil || thumbnailInput.LinkSource.SourceNodeID != "p-move" || thumbnailInput.LinkSource.SourcePort != "items" {
		t.Fatalf("p-thumbnail items input = %#v, want link from p-move.items", thumbnailInput.LinkSource)
	}

	if len(graph.Edges) == 0 {
		t.Fatalf("edges count = 0, want > 0")
	}
	for _, node := range graph.Nodes {
		for inputName, spec := range node.Inputs {
			if spec.LinkSource == nil {
				continue
			}
			if spec.LinkSource.SourcePort == "" {
				t.Fatalf("node %s input %s source_port is empty", node.ID, inputName)
			}
			if spec.LinkSource.OutputPortIndex != 0 {
				t.Fatalf("node %s input %s still uses output_port_index=%d", node.ID, inputName, spec.LinkSource.OutputPortIndex)
			}
		}
	}
	for _, edge := range graph.Edges {
		if edge.SourcePort == "" {
			t.Fatalf("edge %s source_port is empty", edge.ID)
		}
		if edge.TargetPort == "" {
			t.Fatalf("edge %s target_port is empty", edge.ID)
		}
		if edge.SourcePortIndex != 0 || edge.TargetPortIndex != 0 {
			t.Fatalf("edge %s still uses numeric port indexes: source=%d target=%d", edge.ID, edge.SourcePortIndex, edge.TargetPortIndex)
		}
	}

	if err := SeedDefaultProcessingWorkflow(ctx, repo); err != nil {
		t.Fatalf("second SeedDefaultProcessingWorkflow() error = %v", err)
	}
	items, total, err = repo.List(ctx, repository.WorkflowDefListFilter{Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("repo.List() after second seed error = %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("after second seed workflow count = total:%d len:%d, want 1", total, len(items))
	}
	if items[0].ID != seeded.ID {
		t.Fatalf("seeded workflow id changed from %q to %q on second seed", seeded.ID, items[0].ID)
	}
}

func TestSeedDefaultProcessingWorkflow_DoesNotDuplicateWhenDefaultWorkflowExists(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	if err := SeedDefaultWorkflow(ctx, repo); err != nil {
		t.Fatalf("SeedDefaultWorkflow() error = %v", err)
	}
	if err := SeedDefaultProcessingWorkflow(ctx, repo); err != nil {
		t.Fatalf("SeedDefaultProcessingWorkflow() error = %v", err)
	}
	if err := SeedDefaultProcessingWorkflow(ctx, repo); err != nil {
		t.Fatalf("second SeedDefaultProcessingWorkflow() error = %v", err)
	}

	items, total, err := repo.List(ctx, repository.WorkflowDefListFilter{Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("repo.List() error = %v", err)
	}
	if total != 2 || len(items) != 2 {
		t.Fatalf("workflow count = total:%d len:%d, want 2", total, len(items))
	}
}

func TestSeedGenericProcessingWorkflow_CreatesExpectedGraphAndIsIdempotent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	if err := SeedGenericProcessingWorkflow(ctx, repo); err != nil {
		t.Fatalf("SeedGenericProcessingWorkflow() error = %v", err)
	}

	items, total, err := repo.List(ctx, repository.WorkflowDefListFilter{Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("repo.List() error = %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("workflow count = total:%d len:%d, want 1", total, len(items))
	}

	seeded := items[0]
	if seeded.Name != "通用处理流程" {
		t.Fatalf("seeded.Name = %q, want 通用处理流程", seeded.Name)
	}
	if seeded.IsActive {
		t.Fatalf("seeded.IsActive = true, want false")
	}
	if seeded.Version != 1 {
		t.Fatalf("seeded.Version = %d, want 1", seeded.Version)
	}

	var graph repository.WorkflowGraph
	if err := json.Unmarshal([]byte(seeded.GraphJSON), &graph); err != nil {
		t.Fatalf("json.Unmarshal(GraphJSON) error = %v", err)
	}

	gotTypes := make([]string, 0, len(graph.Nodes))
	for _, node := range graph.Nodes {
		gotTypes = append(gotTypes, node.Type)
	}
	wantTypes := []string{
		"db-subtree-reader",
		"folder-splitter",
		"category-router",
		"rename-node",
		"rename-node",
		"rename-node",
		"rename-node",
		"mixed-leaf-router",
		"rename-node",
		"rename-node",
		"collect-node",
		"move-node",
	}
	if len(gotTypes) != len(wantTypes) {
		t.Fatalf("node count = %d, want %d; got %v", len(gotTypes), len(wantTypes), gotTypes)
	}
	for i, want := range wantTypes {
		if gotTypes[i] != want {
			t.Fatalf("node[%d].Type = %q, want %q; full=%v", i, gotTypes[i], want, gotTypes)
		}
	}

	if len(graph.Edges) == 0 {
		t.Fatalf("edges count = 0, want > 0")
	}
	for _, node := range graph.Nodes {
		for inputName, spec := range node.Inputs {
			if spec.LinkSource == nil {
				continue
			}
			if spec.LinkSource.SourcePort == "" {
				t.Fatalf("node %s input %s source_port is empty", node.ID, inputName)
			}
			if spec.LinkSource.OutputPortIndex != 0 {
				t.Fatalf("node %s input %s still uses output_port_index=%d", node.ID, inputName, spec.LinkSource.OutputPortIndex)
			}
		}
	}
	for _, edge := range graph.Edges {
		if edge.SourcePort == "" {
			t.Fatalf("edge %s source_port is empty", edge.ID)
		}
		if edge.TargetPort == "" {
			t.Fatalf("edge %s target_port is empty", edge.ID)
		}
		if edge.SourcePortIndex != 0 || edge.TargetPortIndex != 0 {
			t.Fatalf("edge %s still uses numeric port indexes: source=%d target=%d", edge.ID, edge.SourcePortIndex, edge.TargetPortIndex)
		}
	}

	if err := SeedGenericProcessingWorkflow(ctx, repo); err != nil {
		t.Fatalf("second SeedGenericProcessingWorkflow() error = %v", err)
	}
	items, total, err = repo.List(ctx, repository.WorkflowDefListFilter{Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("repo.List() after second seed error = %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("after second seed workflow count = total:%d len:%d, want 1", total, len(items))
	}
	if items[0].ID != seeded.ID {
		t.Fatalf("seeded workflow id changed from %q to %q on second seed", seeded.ID, items[0].ID)
	}
}

func TestSeedGenericProcessingWorkflow_DoesNotDuplicateWithOtherSeededFlows(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	if err := SeedDefaultWorkflow(ctx, repo); err != nil {
		t.Fatalf("SeedDefaultWorkflow() error = %v", err)
	}
	if err := SeedDefaultProcessingWorkflow(ctx, repo); err != nil {
		t.Fatalf("SeedDefaultProcessingWorkflow() error = %v", err)
	}
	if err := SeedGenericProcessingWorkflow(ctx, repo); err != nil {
		t.Fatalf("SeedGenericProcessingWorkflow() error = %v", err)
	}
	if err := SeedGenericProcessingWorkflow(ctx, repo); err != nil {
		t.Fatalf("second SeedGenericProcessingWorkflow() error = %v", err)
	}

	items, total, err := repo.List(ctx, repository.WorkflowDefListFilter{Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("repo.List() error = %v", err)
	}
	if total != 3 || len(items) != 3 {
		t.Fatalf("workflow count = total:%d len:%d, want 3", total, len(items))
	}
}

func TestSeedGenericProcessingWorkflow_DoesNotOverwriteExistingGraph(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	repo := repository.NewWorkflowDefinitionRepository(database)

	customGraph := repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{
				ID:      "custom-trigger",
				Type:    "trigger",
				Config:  map[string]any{},
				Inputs:  map[string]repository.NodeInputSpec{},
				Enabled: true,
			},
		},
		Edges: []repository.WorkflowGraphEdge{},
	}
	customGraphBytes, err := json.Marshal(customGraph)
	if err != nil {
		t.Fatalf("json.Marshal(customGraph) error = %v", err)
	}

	if err := repo.Create(ctx, &repository.WorkflowDefinition{
		ID:          "wf-custom-generic",
		Name:        "通用处理流程",
		Description: "用户自定义版本",
		GraphJSON:   string(customGraphBytes),
		IsActive:    true,
		Version:     7,
	}); err != nil {
		t.Fatalf("repo.Create() error = %v", err)
	}

	if err := SeedGenericProcessingWorkflow(ctx, repo); err != nil {
		t.Fatalf("SeedGenericProcessingWorkflow() error = %v", err)
	}

	item, err := workflowDefinitionByName(ctx, repo, "通用处理流程")
	if err != nil {
		t.Fatalf("workflowDefinitionByName() error = %v", err)
	}
	if item == nil {
		t.Fatalf("workflowDefinitionByName() = nil, want existing item")
	}
	if item.ID != "wf-custom-generic" {
		t.Fatalf("item.ID = %q, want wf-custom-generic", item.ID)
	}
	if item.Description != "用户自定义版本" {
		t.Fatalf("item.Description = %q, want 用户自定义版本", item.Description)
	}
	if item.GraphJSON != string(customGraphBytes) {
		t.Fatalf("item.GraphJSON was overwritten")
	}
	if !item.IsActive {
		t.Fatalf("item.IsActive = false, want true")
	}
	if item.Version != 7 {
		t.Fatalf("item.Version = %d, want 7", item.Version)
	}
}

func TestSeedBuiltinWorkflows_DoesNotRecreateDeletedDefinitionsAfterInitialized(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	workflowRepo := repository.NewWorkflowDefinitionRepository(database)
	configRepo := repository.NewConfigRepository(database)

	if err := configRepo.Set(ctx, workflowSeedInitializedKey, "1"); err != nil {
		t.Fatalf("configRepo.Set(seed marker) error = %v", err)
	}
	if err := SeedBuiltinWorkflows(ctx, workflowRepo, configRepo); err != nil {
		t.Fatalf("SeedBuiltinWorkflows() error = %v", err)
	}

	items, total, err := workflowRepo.List(ctx, repository.WorkflowDefListFilter{Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("workflowRepo.List() error = %v", err)
	}
	if total != 0 || len(items) != 0 {
		t.Fatalf("workflow count = total:%d len:%d, want 0", total, len(items))
	}
}

func TestSeedBuiltinWorkflows_SetsMarkerWithoutSeedingWhenDefinitionsExist(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	workflowRepo := repository.NewWorkflowDefinitionRepository(database)
	configRepo := repository.NewConfigRepository(database)

	if err := workflowRepo.Create(ctx, &repository.WorkflowDefinition{
		ID:          "wf-manual",
		Name:        "用户自定义流程",
		Description: "manual",
		GraphJSON:   `{"nodes":[],"edges":[]}`,
		IsActive:    true,
		Version:     1,
	}); err != nil {
		t.Fatalf("workflowRepo.Create() error = %v", err)
	}

	if err := SeedBuiltinWorkflows(ctx, workflowRepo, configRepo); err != nil {
		t.Fatalf("SeedBuiltinWorkflows() error = %v", err)
	}

	items, total, err := workflowRepo.List(ctx, repository.WorkflowDefListFilter{Page: 1, Limit: 10})
	if err != nil {
		t.Fatalf("workflowRepo.List() error = %v", err)
	}
	if total != 1 || len(items) != 1 {
		t.Fatalf("workflow count = total:%d len:%d, want 1", total, len(items))
	}
	if items[0].Name != "用户自定义流程" {
		t.Fatalf("items[0].Name = %q, want 用户自定义流程", items[0].Name)
	}

	marker, err := configRepo.Get(ctx, workflowSeedInitializedKey)
	if err != nil {
		t.Fatalf("configRepo.Get(seed marker) error = %v", err)
	}
	if marker != "1" {
		t.Fatalf("seed marker = %q, want 1", marker)
	}
}
