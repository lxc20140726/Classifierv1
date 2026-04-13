package service

import (
	"testing"

	"github.com/liqiye/classifier/internal/repository"
)

func TestClassifyNodeInputsSkipOnEmpty(t *testing.T) {
	t.Parallel()

	node := repository.WorkflowGraphNode{
		ID:   "rename",
		Type: "rename-node",
		Inputs: map[string]repository.NodeInputSpec{
			"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "router", SourcePort: "manga"}},
		},
	}
	schema := NodeSchema{
		Inputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, Required: true, SkipOnEmpty: true},
		},
	}
	inputs := map[string]*TypedValue{
		"items": {Type: PortTypeProcessingItemList, Value: []ProcessingItem{}},
	}
	sources := map[string]InputValueSource{
		"items": InputValueSourceEmptyOutput,
	}

	skip, fail, _, _, reason := classifyNodeInputs(node, inputs, sources, schema)
	if !skip {
		t.Fatalf("skip = false, want true")
	}
	if fail {
		t.Fatalf("fail = true, want false")
	}
	if reason != "empty_input" {
		t.Fatalf("reason = %q, want empty_input", reason)
	}
}

func TestClassifyNodeInputsSkipOnDefaultFromSkippedUpstream(t *testing.T) {
	t.Parallel()

	node := repository.WorkflowGraphNode{
		ID:   "move",
		Type: "move-node",
		Inputs: map[string]repository.NodeInputSpec{
			"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "rename", SourcePort: "items"}},
		},
	}
	schema := NodeSchema{
		Inputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, Required: true, AcceptDefault: true},
		},
	}
	inputs := map[string]*TypedValue{
		"items": {Type: PortTypeProcessingItemList, Value: []ProcessingItem{}},
	}
	sources := map[string]InputValueSource{
		"items": InputValueSourceDefaultFromSkippedUpstream,
	}

	skip, fail, _, _, reason := classifyNodeInputs(node, inputs, sources, schema)
	if !skip {
		t.Fatalf("skip = false, want true")
	}
	if fail {
		t.Fatalf("fail = true, want false")
	}
	if reason != "default_from_skipped_upstream" {
		t.Fatalf("reason = %q, want default_from_skipped_upstream", reason)
	}
}

func TestClassifyNodeInputsEmptyListFailsWithoutSkipOnEmpty(t *testing.T) {
	t.Parallel()

	node := repository.WorkflowGraphNode{
		ID:   "strict",
		Type: "strict-node",
		Inputs: map[string]repository.NodeInputSpec{
			"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "router", SourcePort: "other"}},
		},
	}
	schema := NodeSchema{
		Inputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, Required: true},
		},
	}
	inputs := map[string]*TypedValue{
		"items": {Type: PortTypeProcessingItemList, Value: []ProcessingItem{}},
	}
	sources := map[string]InputValueSource{
		"items": InputValueSourceEmptyOutput,
	}

	skip, fail, code, _, reason := classifyNodeInputs(node, inputs, sources, schema)
	if skip {
		t.Fatalf("skip = true, want false")
	}
	if !fail {
		t.Fatalf("fail = false, want true")
	}
	if code != "NODE_INPUT_EMPTY" {
		t.Fatalf("code = %q, want NODE_INPUT_EMPTY", code)
	}
	if reason != "" {
		t.Fatalf("reason = %q, want empty string", reason)
	}
}

func TestClassifyNodeInputsMixedLeafRouterSkipsEmptyList(t *testing.T) {
	t.Parallel()

	node := repository.WorkflowGraphNode{
		ID:   "mixed-router",
		Type: mixedLeafRouterExecutorType,
		Inputs: map[string]repository.NodeInputSpec{
			"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "router", SourcePort: "mixed_leaf"}},
		},
	}
	schema := newMixedLeafRouterExecutor(nil).Schema()
	inputs := map[string]*TypedValue{
		"items": {Type: PortTypeProcessingItemList, Value: []ProcessingItem{}},
	}
	sources := map[string]InputValueSource{
		"items": InputValueSourceEmptyOutput,
	}

	skip, fail, _, _, reason := classifyNodeInputs(node, inputs, sources, schema)
	if !skip {
		t.Fatalf("skip = false, want true")
	}
	if fail {
		t.Fatalf("fail = true, want false")
	}
	if reason != "empty_input" {
		t.Fatalf("reason = %q, want empty_input", reason)
	}
}
