package service

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/liqiye/classifier/internal/repository"
)

const auditLogNodeType = "audit-log"

func NormalizeWorkflowDefinitionGraphs(ctx context.Context, repo repository.WorkflowDefinitionRepository) error {
	if repo == nil {
		return nil
	}

	page := 1
	for {
		items, total, err := repo.List(ctx, repository.WorkflowDefListFilter{Page: page, Limit: 100})
		if err != nil {
			return fmt.Errorf("normalizeWorkflowDefinitionGraphs list page %d: %w", page, err)
		}
		if len(items) == 0 {
			return nil
		}

		for _, item := range items {
			if item == nil || strings.TrimSpace(item.GraphJSON) == "" {
				continue
			}
			normalized, changed, err := normalizeWorkflowGraphJSON(item)
			if err != nil {
				return fmt.Errorf("normalizeWorkflowDefinitionGraphs normalize %q: %w", item.ID, err)
			}
			if !changed {
				continue
			}
			item.GraphJSON = normalized
			if err := repo.Update(ctx, item); err != nil {
				return fmt.Errorf("normalizeWorkflowDefinitionGraphs update %q: %w", item.ID, err)
			}
		}

		if page*100 >= total {
			return nil
		}
		page++
	}
}

func (s *WorkflowRunnerService) NormalizeWorkflowDefinitionGraph(name, graphJSON string) (string, error) {
	def := &repository.WorkflowDefinition{
		Name:      strings.TrimSpace(name),
		GraphJSON: graphJSON,
	}
	normalized, _, err := normalizeWorkflowGraphJSON(def)
	if err != nil {
		return "", fmt.Errorf("normalize workflow definition graph: %w", err)
	}
	return normalized, nil
}

func normalizeWorkflowGraphJSON(def *repository.WorkflowDefinition) (string, bool, error) {
	if def == nil {
		return "", false, fmt.Errorf("workflow definition is nil")
	}

	raw := strings.TrimSpace(def.GraphJSON)
	if raw == "" {
		return "", false, nil
	}

	graph := repository.WorkflowGraph{}
	if err := json.Unmarshal([]byte(raw), &graph); err != nil {
		return "", false, fmt.Errorf("unmarshal graph json: %w", err)
	}

	removedNodeIDs := map[string]struct{}{}
	changed := false

	filteredNodes := make([]repository.WorkflowGraphNode, 0, len(graph.Nodes))
	for _, node := range graph.Nodes {
		if strings.TrimSpace(node.Type) == auditLogNodeType {
			removedNodeIDs[node.ID] = struct{}{}
			changed = true
			continue
		}
		if migrateNodePathConfig(&node) {
			changed = true
		}

		if len(node.Inputs) > 0 {
			cleanInputs := make(map[string]repository.NodeInputSpec, len(node.Inputs))
			for key, spec := range node.Inputs {
				if isStepResultsPort(key) {
					changed = true
					continue
				}
				if spec.LinkSource != nil {
					if _, removed := removedNodeIDs[spec.LinkSource.SourceNodeID]; removed {
						changed = true
						continue
					}
					if isStepResultsPort(spec.LinkSource.SourcePort) {
						changed = true
						continue
					}
				}
				cleanInputs[key] = spec
			}
			node.Inputs = cleanInputs
		}

		filteredNodes = append(filteredNodes, node)
	}
	graph.Nodes = filteredNodes

	filteredEdges := make([]repository.WorkflowGraphEdge, 0, len(graph.Edges))
	for _, edge := range graph.Edges {
		if _, removed := removedNodeIDs[edge.Source]; removed {
			changed = true
			continue
		}
		if _, removed := removedNodeIDs[edge.Target]; removed {
			changed = true
			continue
		}
		if isStepResultsPort(edge.SourcePort) || isStepResultsPort(edge.TargetPort) {
			changed = true
			continue
		}
		filteredEdges = append(filteredEdges, edge)
	}
	graph.Edges = filteredEdges

	remainingNodeIDs := make(map[string]struct{}, len(graph.Nodes))
	for _, node := range graph.Nodes {
		remainingNodeIDs[node.ID] = struct{}{}
	}
	for index := range graph.Nodes {
		node := &graph.Nodes[index]
		if len(node.Inputs) == 0 {
			continue
		}
		cleanInputs := make(map[string]repository.NodeInputSpec, len(node.Inputs))
		for key, spec := range node.Inputs {
			if spec.LinkSource == nil {
				cleanInputs[key] = spec
				continue
			}
			if _, exists := remainingNodeIDs[spec.LinkSource.SourceNodeID]; !exists {
				changed = true
				continue
			}
			if isStepResultsPort(spec.LinkSource.SourcePort) {
				changed = true
				continue
			}
			cleanInputs[key] = spec
		}
		node.Inputs = cleanInputs
	}

	if normalizeBuiltinDefaultProcessingGraph(def, &graph) {
		changed = true
	}
	if normalizeBuiltinGenericProcessingGraph(def, &graph) {
		changed = true
	}

	if !changed {
		return raw, false, nil
	}

	data, err := json.Marshal(graph)
	if err != nil {
		return "", false, fmt.Errorf("marshal normalized graph json: %w", err)
	}
	return string(data), true, nil
}

func normalizeBuiltinDefaultProcessingGraph(def *repository.WorkflowDefinition, graph *repository.WorkflowGraph) bool {
	if def == nil || graph == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(def.Name), "default-processing") {
		return false
	}

	nodeIndexByID := map[string]int{}
	for index := range graph.Nodes {
		nodeIndexByID[graph.Nodes[index].ID] = index
	}

	requiredNodeIDs := []string{"p-router", "p-rename", "p-compress", "p-move", "p-thumbnail"}
	for _, nodeID := range requiredNodeIDs {
		if _, ok := nodeIndexByID[nodeID]; !ok {
			return false
		}
	}

	changed := false
	if setWorkflowNodeInputLink(&graph.Nodes[nodeIndexByID["p-rename"]], "items", "p-router", "video") {
		changed = true
	}
	if setWorkflowNodeInputLink(&graph.Nodes[nodeIndexByID["p-compress"]], "items", "p-rename", "items") {
		changed = true
	}
	if setWorkflowNodeInputLink(&graph.Nodes[nodeIndexByID["p-move"]], "items", "p-compress", "items") {
		changed = true
	}
	if setWorkflowNodeInputLink(&graph.Nodes[nodeIndexByID["p-thumbnail"]], "items", "p-move", "items") {
		changed = true
	}

	thumbnailNode := &graph.Nodes[nodeIndexByID["p-thumbnail"]]
	if thumbnailNode.Config == nil {
		thumbnailNode.Config = map[string]any{}
	}
	for _, key := range []string{
		"path_ref_type",
		"path_ref_key",
		"path_suffix",
		"target_dir",
		"targetDir",
		"output_dir",
		"target_dir_source",
		"output_dir_source",
		"target_dir_option_id",
		"output_dir_option_id",
	} {
		if _, ok := thumbnailNode.Config[key]; ok {
			delete(thumbnailNode.Config, key)
			changed = true
		}
	}

	if ensureWorkflowGraphEdge(graph, "e-router-rename", "p-router", "video", "p-rename", "items") {
		changed = true
	}
	if ensureWorkflowGraphEdge(graph, "e-rename-compress", "p-rename", "items", "p-compress", "items") {
		changed = true
	}
	if ensureWorkflowGraphEdge(graph, "e-compress-move", "p-compress", "items", "p-move", "items") {
		changed = true
	}
	if ensureWorkflowGraphEdge(graph, "e-move-thumbnail", "p-move", "items", "p-thumbnail", "items") {
		changed = true
	}
	if removeWorkflowGraphEdge(graph, "p-router", "video", "p-thumbnail", "items") {
		changed = true
	}
	if removeWorkflowGraphEdge(graph, "p-thumbnail", "items", "p-rename", "items") {
		changed = true
	}

	return changed
}

func normalizeBuiltinGenericProcessingGraph(def *repository.WorkflowDefinition, graph *repository.WorkflowGraph) bool {
	if def == nil || graph == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(def.Name), "通用处理流程") {
		return false
	}

	nodeIndexByID := map[string]int{}
	for index := range graph.Nodes {
		nodeIndexByID[graph.Nodes[index].ID] = index
	}

	if _, ok := nodeIndexByID["g-mixed-router"]; !ok {
		return false
	}
	if _, ok := nodeIndexByID["g-collect"]; !ok {
		return false
	}

	changed := false
	if index, ok := nodeIndexByID["compress-node-12"]; ok {
		if setCompressNodeMixedPathRefByCategory(&graph.Nodes[index], "manga:0") {
			changed = true
		}
	}
	if index, ok := nodeIndexByID["compress-node-13"]; ok {
		if setCompressNodeMixedPathRefByCategory(&graph.Nodes[index], "photo:0") {
			changed = true
		}
	}

	otherIndex, ok := nodeIndexByID["g-rename-mixed-other"]
	if !ok {
		graph.Nodes = append(graph.Nodes, repository.WorkflowGraphNode{
			ID:   "g-rename-mixed-other",
			Type: "rename-node",
			Config: map[string]any{
				"strategy": "template",
				"template": "{name}",
			},
			Inputs: map[string]repository.NodeInputSpec{
				"items": {LinkSource: &repository.NodeLinkSource{SourceNodeID: "g-mixed-router", SourcePort: "unsupported"}},
			},
			Enabled: true,
		})
		otherIndex = len(graph.Nodes) - 1
		nodeIndexByID["g-rename-mixed-other"] = otherIndex
		changed = true
	}

	if setWorkflowNodeInputLink(&graph.Nodes[otherIndex], "items", "g-mixed-router", "unsupported") {
		changed = true
	}
	if setWorkflowNodeInputLink(&graph.Nodes[nodeIndexByID["g-collect"]], "items_7", "g-rename-mixed-other", "items") {
		changed = true
	}
	if ensureWorkflowGraphEdge(graph, "e-mixed-router-rename-other", "g-mixed-router", "unsupported", "g-rename-mixed-other", "items") {
		changed = true
	}
	if ensureWorkflowGraphEdge(graph, "e-rename-mixed-other-collect", "g-rename-mixed-other", "items", "g-collect", "items_7") {
		changed = true
	}

	return changed
}

func setWorkflowNodeInputLink(node *repository.WorkflowGraphNode, inputName, sourceNodeID, sourcePort string) bool {
	if node == nil {
		return false
	}
	if node.Inputs == nil {
		node.Inputs = map[string]repository.NodeInputSpec{}
	}
	spec := node.Inputs[inputName]
	if spec.LinkSource != nil &&
		strings.TrimSpace(spec.LinkSource.SourceNodeID) == strings.TrimSpace(sourceNodeID) &&
		strings.TrimSpace(spec.LinkSource.SourcePort) == strings.TrimSpace(sourcePort) {
		return false
	}

	spec.LinkSource = &repository.NodeLinkSource{
		SourceNodeID: strings.TrimSpace(sourceNodeID),
		SourcePort:   strings.TrimSpace(sourcePort),
	}
	spec.ConstValue = nil
	node.Inputs[inputName] = spec
	return true
}

func ensureWorkflowGraphEdge(graph *repository.WorkflowGraph, edgeID, source, sourcePort, target, targetPort string) bool {
	if graph == nil {
		return false
	}

	normalizedEdgeID := strings.TrimSpace(edgeID)
	normalizedSource := strings.TrimSpace(source)
	normalizedSourcePort := strings.TrimSpace(sourcePort)
	normalizedTarget := strings.TrimSpace(target)
	normalizedTargetPort := strings.TrimSpace(targetPort)

	for index := range graph.Edges {
		edge := &graph.Edges[index]
		if strings.TrimSpace(edge.Source) != normalizedSource ||
			strings.TrimSpace(edge.SourcePort) != normalizedSourcePort ||
			strings.TrimSpace(edge.Target) != normalizedTarget ||
			strings.TrimSpace(edge.TargetPort) != normalizedTargetPort {
			continue
		}
		changed := false
		if strings.TrimSpace(edge.ID) == "" && normalizedEdgeID != "" {
			edge.ID = normalizedEdgeID
			changed = true
		}
		if edge.SourcePortIndex != 0 {
			edge.SourcePortIndex = 0
			changed = true
		}
		if edge.TargetPortIndex != 0 {
			edge.TargetPortIndex = 0
			changed = true
		}
		return changed
	}

	graph.Edges = append(graph.Edges, repository.WorkflowGraphEdge{
		ID:         normalizedEdgeID,
		Source:     normalizedSource,
		SourcePort: normalizedSourcePort,
		Target:     normalizedTarget,
		TargetPort: normalizedTargetPort,
	})
	return true
}

func removeWorkflowGraphEdge(graph *repository.WorkflowGraph, source, sourcePort, target, targetPort string) bool {
	if graph == nil || len(graph.Edges) == 0 {
		return false
	}

	normalizedSource := strings.TrimSpace(source)
	normalizedSourcePort := strings.TrimSpace(sourcePort)
	normalizedTarget := strings.TrimSpace(target)
	normalizedTargetPort := strings.TrimSpace(targetPort)

	filtered := make([]repository.WorkflowGraphEdge, 0, len(graph.Edges))
	removed := false
	for _, edge := range graph.Edges {
		if strings.TrimSpace(edge.Source) == normalizedSource &&
			strings.TrimSpace(edge.SourcePort) == normalizedSourcePort &&
			strings.TrimSpace(edge.Target) == normalizedTarget &&
			strings.TrimSpace(edge.TargetPort) == normalizedTargetPort {
			removed = true
			continue
		}
		filtered = append(filtered, edge)
	}
	if removed {
		graph.Edges = filtered
	}
	return removed
}

func isStepResultsPort(name string) bool {
	return strings.HasPrefix(strings.TrimSpace(name), "step_results")
}

func migrateNodePathConfig(node *repository.WorkflowGraphNode) bool {
	if node == nil || node.Config == nil {
		return false
	}

	changed := false
	legacyPath := ""
	if value := normalizeWorkflowPath(stringConfig(node.Config, "target_dir")); value != "" {
		legacyPath = value
	}
	if legacyPath == "" {
		if value := normalizeWorkflowPath(stringConfig(node.Config, "output_dir")); value != "" {
			legacyPath = value
		}
	}
	legacyRefType, legacyRefKey := legacyPathRefFromConfig(node.Config)
	hasModernPathRef := strings.TrimSpace(stringConfig(node.Config, "path_ref_type")) != "" ||
		strings.TrimSpace(stringConfig(node.Config, "path_ref_key")) != ""

	if strings.TrimSpace(stringConfig(node.Config, "path_ref_type")) == "" {
		if legacyRefType != "" {
			node.Config["path_ref_type"] = legacyRefType
			changed = true
		}
		switch strings.TrimSpace(node.Type) {
		case "move-node":
			if strings.TrimSpace(stringConfig(node.Config, "path_ref_type")) == "" {
				node.Config["path_ref_type"] = workflowPathRefTypeOutput
				changed = true
			}
			if strings.TrimSpace(stringConfig(node.Config, "path_ref_key")) == "" && legacyRefKey == "" {
				node.Config["path_ref_key"] = "mixed"
				changed = true
			}
		case "compress-node":
			if strings.TrimSpace(stringConfig(node.Config, "path_ref_type")) == "" {
				node.Config["path_ref_type"] = workflowPathRefTypeOutput
				changed = true
			}
			if strings.TrimSpace(stringConfig(node.Config, "path_ref_key")) == "" && legacyRefKey == "" {
				node.Config["path_ref_key"] = "mixed"
				changed = true
			}
		}
	}
	if strings.TrimSpace(stringConfig(node.Config, "path_ref_key")) == "" && legacyRefKey != "" {
		node.Config["path_ref_key"] = legacyRefKey
		changed = true
	}

	if legacyPath != "" && !hasModernPathRef {
		node.Config["path_ref_type"] = workflowPathRefTypeCustom
		node.Config["path_ref_key"] = legacyPath
		changed = true
	}

	legacyKeys := []string{
		"target_dir",
		"targetDir",
		"output_dir",
		"target_dir_source",
		"output_dir_source",
		"target_dir_option_id",
		"output_dir_option_id",
	}
	for _, key := range legacyKeys {
		if _, ok := node.Config[key]; ok {
			delete(node.Config, key)
			changed = true
		}
	}

	return changed
}

func setCompressNodeMixedPathRefByCategory(node *repository.WorkflowGraphNode, expectedRef string) bool {
	if node == nil || strings.TrimSpace(node.Type) != "compress-node" {
		return false
	}
	if node.Config == nil {
		node.Config = map[string]any{}
	}

	refType := strings.ToLower(strings.TrimSpace(stringConfig(node.Config, "path_ref_type")))
	refKey := strings.TrimSpace(stringConfig(node.Config, "path_ref_key"))
	legacyRefType, legacyRefKey := legacyPathRefFromConfig(node.Config)
	if refType == "" {
		refType = legacyRefType
	}
	if refKey == "" {
		refKey = legacyRefKey
	}
	if refType == "" {
		refType = workflowPathRefTypeOutput
	}

	if refType != workflowPathRefTypeOutput {
		return false
	}
	category, index := parseOutputDirRef(refKey)
	if category != "mixed" || index != 0 {
		return false
	}

	changed := false
	if strings.TrimSpace(stringConfig(node.Config, "path_ref_type")) != workflowPathRefTypeOutput {
		node.Config["path_ref_type"] = workflowPathRefTypeOutput
		changed = true
	}
	if strings.TrimSpace(stringConfig(node.Config, "path_ref_key")) != expectedRef {
		node.Config["path_ref_key"] = expectedRef
		changed = true
	}
	return changed
}
