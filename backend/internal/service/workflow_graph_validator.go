package service

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/liqiye/classifier/internal/repository"
)

type ValidationErrors struct {
	Issues []string
}

func (e *ValidationErrors) Error() string {
	return strings.Join(e.Issues, "; ")
}

func (e *ValidationErrors) add(format string, args ...any) {
	e.Issues = append(e.Issues, fmt.Sprintf(format, args...))
}

func (e *ValidationErrors) hasIssues() bool {
	return len(e.Issues) > 0
}

func ValidateGraph(graph repository.WorkflowGraph, schemas map[string]NodeSchema) error {
	issues := &ValidationErrors{}
	nodeByID := make(map[string]repository.WorkflowGraphNode, len(graph.Nodes))
	edgeByTargetPort := make(map[string]repository.WorkflowGraphEdge, len(graph.Edges))
	edgeByInputBinding := make(map[string]repository.WorkflowGraphEdge, len(graph.Edges))
	seenEdgeIDs := make(map[string]struct{}, len(graph.Edges))

	for _, node := range graph.Nodes {
		if strings.TrimSpace(node.ID) == "" {
			issues.add("node id is empty")
			continue
		}
		if _, exists := nodeByID[node.ID]; exists {
			issues.add("duplicate node id %q", node.ID)
			continue
		}
		nodeByID[node.ID] = node

		schema, ok := schemas[node.Type]
		if !ok {
			issues.add("unknown node type %q for node %q", node.Type, node.ID)
			continue
		}

		for _, port := range schema.InputDefs() {
			if port.Required && port.Lazy {
				issues.add("node type %q port %q cannot be both required and lazy", node.Type, port.Name)
			}
		}
	}

	for _, node := range graph.Nodes {
		schema, ok := schemas[node.Type]
		if !ok {
			continue
		}
		for inputName, spec := range node.Inputs {
			targetPort := schema.InputPort(inputName)
			if targetPort == nil {
				issues.add("node %q has unknown input port %q", node.ID, inputName)
				continue
			}
			if spec.ConstValue != nil && spec.LinkSource != nil {
				issues.add("node %q input %q cannot define both const_value and link_source", node.ID, inputName)
				continue
			}
			if spec.LinkSource == nil {
				continue
			}

			sourceNode, exists := nodeByID[spec.LinkSource.SourceNodeID]
			if !exists {
				issues.add("node %q input %q references missing source node %q", node.ID, inputName, spec.LinkSource.SourceNodeID)
				continue
			}

			sourceSchema, ok := schemas[sourceNode.Type]
			if !ok {
				issues.add("node %q input %q references unknown source node type %q", node.ID, inputName, sourceNode.Type)
				continue
			}

			sourcePortName := strings.TrimSpace(spec.LinkSource.SourcePort)
			if sourcePortName == "" {
				issues.add("node %q input %q link source port is empty", node.ID, inputName)
				continue
			}

			sourcePort := sourceSchema.OutputPort(sourcePortName)
			if sourcePort == nil {
				issues.add("node %q input %q references missing source port %q on node %q", node.ID, inputName, sourcePortName, sourceNode.ID)
				continue
			}

			if sourcePort.Type != "" && targetPort.Type != "" && sourcePort.Type != targetPort.Type {
				issues.add("port type mismatch: %q.%q(%s) -> %q.%q(%s)", sourceNode.ID, sourcePortName, sourcePort.Type, node.ID, inputName, targetPort.Type)
			}

			bindingKey := edgeBindingKey(sourceNode.ID, sourcePortName, node.ID, inputName)
			edgeByInputBinding[bindingKey] = repository.WorkflowGraphEdge{
				Source:     sourceNode.ID,
				SourcePort: sourcePortName,
				Target:     node.ID,
				TargetPort: inputName,
			}
		}
	}

	for _, edge := range graph.Edges {
		if strings.TrimSpace(edge.ID) != "" {
			if _, exists := seenEdgeIDs[edge.ID]; exists {
				issues.add("duplicate edge id %q", edge.ID)
				continue
			}
			seenEdgeIDs[edge.ID] = struct{}{}
		}

		sourceNode, sourceExists := nodeByID[edge.Source]
		if !sourceExists {
			issues.add("edge %q references missing source node %q", edge.ID, edge.Source)
			continue
		}
		targetNode, targetExists := nodeByID[edge.Target]
		if !targetExists {
			issues.add("edge %q references missing target node %q", edge.ID, edge.Target)
			continue
		}

		sourceSchema, sourceOK := schemas[sourceNode.Type]
		targetSchema, targetOK := schemas[targetNode.Type]
		if !sourceOK || !targetOK {
			continue
		}

		sourcePortName := strings.TrimSpace(edge.SourcePort)
		if sourcePortName == "" {
			issues.add("edge %q source port is empty", edge.ID)
			continue
		}
		targetPortName := strings.TrimSpace(edge.TargetPort)
		if targetPortName == "" {
			issues.add("edge %q target port is empty", edge.ID)
			continue
		}

		sourcePort := sourceSchema.OutputPort(sourcePortName)
		if sourcePort == nil {
			issues.add("node %q has no output port %q", edge.Source, sourcePortName)
			continue
		}
		targetPort := targetSchema.InputPort(targetPortName)
		if targetPort == nil {
			issues.add("node %q has no input port %q", edge.Target, targetPortName)
			continue
		}

		targetBindingKey := nodeInputKey(edge.Target, targetPortName)
		if existing, exists := edgeByTargetPort[targetBindingKey]; exists {
			issues.add("multiple edges target the same input port %q.%q: %q and %q", edge.Target, targetPortName, existing.ID, edge.ID)
			continue
		}
		edgeByTargetPort[targetBindingKey] = edge

		if sourcePort.Type != "" && targetPort.Type != "" && sourcePort.Type != targetPort.Type {
			issues.add("port type mismatch: %q.%q(%s) -> %q.%q(%s)", edge.Source, sourcePortName, sourcePort.Type, edge.Target, targetPortName, targetPort.Type)
		}

		spec, exists := targetNode.Inputs[targetPortName]
		if !exists || spec.LinkSource == nil {
			issues.add("edge %q is not mirrored by node input %q.%q link_source", edge.ID, edge.Target, targetPortName)
			continue
		}
		if spec.ConstValue != nil {
			issues.add("edge %q conflicts with const_value on node %q input %q", edge.ID, edge.Target, targetPortName)
			continue
		}
		if spec.LinkSource.SourceNodeID != edge.Source || strings.TrimSpace(spec.LinkSource.SourcePort) != sourcePortName {
			issues.add("edge %q does not match node input binding for %q.%q", edge.ID, edge.Target, targetPortName)
		}
	}

	for _, node := range graph.Nodes {
		if !node.Enabled {
			continue
		}
		schema, ok := schemas[node.Type]
		if !ok {
			continue
		}
		for _, port := range schema.InputDefs() {
			if !port.Required || port.Lazy {
				continue
			}
			spec, exists := node.Inputs[port.Name]
			if !exists || (spec.ConstValue == nil && spec.LinkSource == nil) {
				issues.add("node %q required input port %q is not connected", node.ID, port.Name)
			}
		}

		for inputName, spec := range node.Inputs {
			if spec.LinkSource == nil {
				continue
			}
			sourcePortName := strings.TrimSpace(spec.LinkSource.SourcePort)
			bindingKey := edgeBindingKey(spec.LinkSource.SourceNodeID, sourcePortName, node.ID, inputName)
			if _, exists := edgeByTargetPort[nodeInputKey(node.ID, inputName)]; !exists {
				issues.add("node %q input %q link_source has no matching edge", node.ID, inputName)
				continue
			}
			if _, exists := edgeByInputBinding[bindingKey]; !exists {
				issues.add("node %q input %q link_source is inconsistent with edge definitions", node.ID, inputName)
			}
		}
	}

	if _, err := topologicalNodes(&graph); err != nil {
		issues.add("%s", err.Error())
	}

	if issues.hasIssues() {
		return issues
	}

	return nil
}

func nodeInputKey(nodeID, portName string) string {
	return nodeID + "::" + portName
}

func edgeBindingKey(sourceNodeID, sourcePortName, targetNodeID, targetPortName string) string {
	return sourceNodeID + "::" + sourcePortName + "->" + targetNodeID + "::" + targetPortName
}

func (s *WorkflowRunnerService) schemaMap() map[string]NodeSchema {
	out := make(map[string]NodeSchema, len(s.executors))
	for nodeType, executor := range s.executors {
		out[nodeType] = executor.Schema()
	}
	return out
}

func (s *WorkflowRunnerService) ValidateWorkflowGraph(graphJSON string) error {
	graph, err := parseWorkflowGraphForValidation(graphJSON)
	if err != nil {
		return err
	}

	s.normalizeGraphPortReferences(graph)
	for edgeIndex := range graph.Edges {
		edge := &graph.Edges[edgeIndex]
		if strings.TrimSpace(edge.SourcePort) == "" && edge.SourcePortIndex > 0 {
			sourceType := ""
			for _, node := range graph.Nodes {
				if node.ID == edge.Source {
					sourceType = node.Type
					break
				}
			}
			sourceSchema := s.schemaForNode(sourceType)
			if edge.SourcePortIndex >= 0 && edge.SourcePortIndex < len(sourceSchema.OutputDefs()) {
				edge.SourcePort = sourceSchema.OutputDefs()[edge.SourcePortIndex].Name
				edge.SourcePortIndex = 0
			}
		}
		if strings.TrimSpace(edge.TargetPort) == "" && edge.TargetPortIndex > 0 {
			targetType := ""
			for _, node := range graph.Nodes {
				if node.ID == edge.Target {
					targetType = node.Type
					break
				}
			}
			targetSchema := s.schemaForNode(targetType)
			if edge.TargetPortIndex >= 0 && edge.TargetPortIndex < len(targetSchema.InputDefs()) {
				edge.TargetPort = targetSchema.InputDefs()[edge.TargetPortIndex].Name
				edge.TargetPortIndex = 0
			}
		}
	}

	return ValidateGraph(*graph, s.schemaMap())
}

func marshalGraphJSON(graph repository.WorkflowGraph) string {
	encoded, _ := json.Marshal(graph)
	return string(encoded)
}
