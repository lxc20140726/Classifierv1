package service

import (
	"strings"
	"testing"

	"github.com/liqiye/classifier/internal/repository"
)

func TestValidateGraph(t *testing.T) {
	t.Parallel()

	schemas := map[string]NodeSchema{
		"scan": {
			Type:    "scan",
			Outputs: []PortDef{{Name: "trees", Type: PortTypeFolderTreeList}},
		},
		"move-node": {
			Type:   "move-node",
			Inputs: []PortDef{{Name: "items", Type: PortTypeProcessingItemList, Required: true}},
		},
		"rename-node": {
			Type:    "rename-node",
			Inputs:  []PortDef{{Name: "items", Type: PortTypeProcessingItemList, Required: true}},
			Outputs: []PortDef{{Name: "items", Type: PortTypeProcessingItemList}},
		},
		"bad-port": {
			Type:   "bad-port",
			Inputs: []PortDef{{Name: "items", Type: PortTypeProcessingItemList, Required: true, Lazy: true}},
		},
	}

	itemValue := any("literal")
	tests := []struct {
		name    string
		graph   repository.WorkflowGraph
		wantErr string
	}{
		{
			name: "type mismatch",
			graph: repository.WorkflowGraph{
				Nodes: []repository.WorkflowGraphNode{
					{ID: "scan", Type: "scan", Enabled: true},
					{ID: "move", Type: "move-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
						"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scan", SourcePort: "trees"}},
					}},
				},
				Edges: []repository.WorkflowGraphEdge{{ID: "e1", Source: "scan", SourcePort: "trees", Target: "move", TargetPort: "items"}},
			},
			wantErr: "port type mismatch",
		},
		{
			name:    "missing required input",
			graph:   repository.WorkflowGraph{Nodes: []repository.WorkflowGraphNode{{ID: "move", Type: "move-node", Enabled: true}}},
			wantErr: "required input port \"items\" is not connected",
		},
		{
			name:    "required and lazy conflict",
			graph:   repository.WorkflowGraph{Nodes: []repository.WorkflowGraphNode{{ID: "bad", Type: "bad-port", Enabled: true}}},
			wantErr: "cannot be both required and lazy",
		},
		{
			name: "const and link source conflict",
			graph: repository.WorkflowGraph{Nodes: []repository.WorkflowGraphNode{
				{ID: "scan", Type: "scan", Enabled: true},
				{ID: "move", Type: "move-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
					"items": {ConstValue: &itemValue, LinkSource: &repository.NodeLinkSource{SourceNodeID: "scan", SourcePort: "trees"}},
				}},
			}},
			wantErr: "both const_value and link_source",
		},
		{
			name:    "unknown node type",
			graph:   repository.WorkflowGraph{Nodes: []repository.WorkflowGraphNode{{ID: "ghost", Type: "ghost", Enabled: true}}},
			wantErr: "unknown node type",
		},
		{
			name: "edge missing mirrored input link source",
			graph: repository.WorkflowGraph{
				Nodes: []repository.WorkflowGraphNode{
					{ID: "scan", Type: "scan", Enabled: true},
					{ID: "move", Type: "move-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{}},
				},
				Edges: []repository.WorkflowGraphEdge{{ID: "e1", Source: "scan", SourcePort: "trees", Target: "move", TargetPort: "items"}},
			},
			wantErr: "not mirrored by node input",
		},
		{
			name: "input link source missing matching edge",
			graph: repository.WorkflowGraph{
				Nodes: []repository.WorkflowGraphNode{
					{ID: "scan", Type: "scan", Enabled: true},
					{ID: "move", Type: "move-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
						"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scan", SourcePort: "trees"}},
					}},
				},
			},
			wantErr: "has no matching edge",
		},
		{
			name: "duplicate target edge binding",
			graph: repository.WorkflowGraph{
				Nodes: []repository.WorkflowGraphNode{
					{ID: "scan-a", Type: "scan", Enabled: true},
					{ID: "scan-b", Type: "scan", Enabled: true},
					{ID: "move", Type: "move-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
						"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scan-a", SourcePort: "trees"}},
					}},
				},
				Edges: []repository.WorkflowGraphEdge{
					{ID: "e1", Source: "scan-a", SourcePort: "trees", Target: "move", TargetPort: "items"},
					{ID: "e2", Source: "scan-b", SourcePort: "trees", Target: "move", TargetPort: "items"},
				},
			},
			wantErr: "multiple edges target the same input port",
		},
		{
			name:  "disabled node skips required connectivity",
			graph: repository.WorkflowGraph{Nodes: []repository.WorkflowGraphNode{{ID: "move", Type: "move-node", Enabled: false}}},
		},
		{
			name: "duplicate edge id",
			graph: repository.WorkflowGraph{
				Nodes: []repository.WorkflowGraphNode{
					{ID: "scan", Type: "scan", Enabled: true},
					{ID: "move-a", Type: "move-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scan", SourcePort: "trees"}}}},
					{ID: "move-b", Type: "move-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "scan", SourcePort: "trees"}}}},
				},
				Edges: []repository.WorkflowGraphEdge{
					{ID: "dup", Source: "scan", SourcePort: "trees", Target: "move-a", TargetPort: "items"},
					{ID: "dup", Source: "scan", SourcePort: "trees", Target: "move-b", TargetPort: "items"},
				},
			},
			wantErr: "duplicate edge id",
		},
		{
			name: "cycle detected",
			graph: repository.WorkflowGraph{
				Nodes: []repository.WorkflowGraphNode{
					{ID: "a", Type: "rename-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "b", SourcePort: "items"}}}},
					{ID: "b", Type: "rename-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "a", SourcePort: "items"}}}},
				},
				Edges: []repository.WorkflowGraphEdge{{ID: "e1", Source: "a", SourcePort: "items", Target: "b", TargetPort: "items"}, {ID: "e2", Source: "b", SourcePort: "items", Target: "a", TargetPort: "items"}},
			},
			wantErr: "cycle detected",
		},
		{
			name:  "const value satisfies required input",
			graph: repository.WorkflowGraph{Nodes: []repository.WorkflowGraphNode{{ID: "move", Type: "move-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{"items": {ConstValue: &itemValue}}}}},
		},
		{
			name: "valid edge and input consistency",
			graph: repository.WorkflowGraph{
				Nodes: []repository.WorkflowGraphNode{
					{ID: "scan", Type: "scan", Enabled: true},
					{ID: "rename-a", Type: "rename-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
						"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "rename-b", SourcePort: "items"}},
					}},
					{ID: "rename-b", Type: "rename-node", Enabled: true, Inputs: map[string]repository.NodeInputSpec{
						"items": {ConstValue: &itemValue},
					}},
				},
				Edges: []repository.WorkflowGraphEdge{{ID: "e1", Source: "rename-b", SourcePort: "items", Target: "rename-a", TargetPort: "items"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateGraph(tt.graph, schemas)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateGraph() error = %v", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("ValidateGraph() error = nil, want %q", tt.wantErr)
			}
			if !strings.Contains(strings.ToLower(err.Error()), strings.ToLower(tt.wantErr)) {
				t.Fatalf("ValidateGraph() error = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}
