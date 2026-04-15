package service

import (
	"testing"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

func TestTopologicalLevelsInfersDependenciesFromNodeInputs(t *testing.T) {
	graph := &repository.WorkflowGraph{
		Nodes: []repository.WorkflowGraphNode{
			{ID: "trigger", Type: "trigger", Enabled: true},
			{
				ID:      "scanner",
				Type:    "folder-tree-scanner",
				Enabled: true,
				Inputs: map[string]repository.NodeInputSpec{
					"source_dir": {
						LinkSource: &repository.NodeLinkSource{SourceNodeID: "trigger", SourcePort: "folder"},
					},
				},
			},
		},
		Edges: []repository.WorkflowGraphEdge{},
	}

	levels, err := topologicalLevels(graph)
	if err != nil {
		t.Fatalf("topologicalLevels() error = %v", err)
	}
	if len(levels) != 2 {
		t.Fatalf("len(levels) = %d, want 2", len(levels))
	}
	if len(levels[0]) != 1 || levels[0][0].ID != "trigger" {
		t.Fatalf("level0 = %#v, want [trigger]", levels[0])
	}
	if len(levels[1]) != 1 || levels[1][0].ID != "scanner" {
		t.Fatalf("level1 = %#v, want [scanner]", levels[1])
	}
}

func TestResolveNodeInputsFallsBackToRunSourceDirWhenUpstreamNil(t *testing.T) {
	svc := &WorkflowRunnerService{
		executors: map[string]WorkflowNodeExecutor{
			"trigger":              &triggerNodeExecutor{},
			folderTreeScannerExecutorType: newFolderTreeScannerExecutor(fs.NewMockAdapter()),
		},
	}

	node := repository.WorkflowGraphNode{
		ID:   "scanner",
		Type: folderTreeScannerExecutorType,
		Inputs: map[string]repository.NodeInputSpec{
			"source_dir": {
				LinkSource: &repository.NodeLinkSource{
					SourceNodeID: "trigger",
					SourcePort:   "folder",
				},
			},
		},
	}

	inputs, _ := svc.resolveNodeInputs(
		node,
		map[string]map[string]TypedValue{
			"trigger": {
				"folder": {Type: PortTypeJSON, Value: nil},
			},
		},
		map[string]string{"trigger": "succeeded"},
		"/tmp/source",
	)

	got, ok := inputs["source_dir"]
	if !ok || got == nil {
		t.Fatalf("source_dir input = nil, want fallback source dir")
	}
	value, ok := got.Value.(string)
	if !ok {
		t.Fatalf("source_dir input type = %T, want string", got.Value)
	}
	if value != "/tmp/source" {
		t.Fatalf("source_dir input value = %q, want /tmp/source", value)
	}
}
