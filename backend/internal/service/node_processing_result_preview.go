package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

const processingResultPreviewExecutorType = "processing-result-preview"

type processingResultPreviewSummary struct {
	TotalDirs   int                    `json:"total_dirs"`
	TotalSteps  int                    `json:"total_steps"`
	Succeeded   int                    `json:"succeeded"`
	Failed      int                    `json:"failed"`
	ByDirectory []dirProcessingSummary `json:"by_directory"`
}

type dirProcessingSummary struct {
	SourcePath string           `json:"source_path"`
	Steps      []dirStepSummary `json:"steps"`
	Succeeded  int              `json:"succeeded"`
	Failed     int              `json:"failed"`
}

type dirStepSummary struct {
	NodeType  string `json:"node_type"`
	NodeLabel string `json:"node_label"`
	Status    string `json:"status"`
	Error     string `json:"error,omitempty"`
}

type processingResultPreviewNodeExecutor struct{}

func newProcessingResultPreviewExecutor() *processingResultPreviewNodeExecutor {
	return &processingResultPreviewNodeExecutor{}
}

func (e *processingResultPreviewNodeExecutor) Type() string {
	return processingResultPreviewExecutorType
}

func (e *processingResultPreviewNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "处理结果预览",
		Description: "仅在节点内预览处理节点结果（不提供下游输出端口）",
		Inputs: []PortDef{
			{Name: "step_results", Type: PortTypeProcessingStepResultList, Required: true, Description: "处理步骤结果列表"},
		},
	}
}

func (e *processingResultPreviewNodeExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	rawResults, ok := firstPresentTyped(input.Inputs, "step_results")
	if !ok {
		return NodeExecutionOutput{
			Status:        ExecutionFailure,
			ErrorCode:     "NODE_INPUT_MISSING",
			PendingReason: "step_results input is required",
		}, nil
	}
	if !isSupportedStepResultInput(rawResults) {
		return NodeExecutionOutput{
			Status:        ExecutionFailure,
			ErrorCode:     "NODE_INPUT_TYPE",
			PendingReason: fmt.Sprintf("step_results input has unsupported type %T", rawResults),
		}, nil
	}

	stepResults := processingStepResultsFromAny(rawResults)
	if len(stepResults) == 0 {
		return NodeExecutionOutput{
			Status:        ExecutionFailure,
			ErrorCode:     "NODE_INPUT_EMPTY",
			PendingReason: "step_results input is empty",
		}, nil
	}

	dirMap := make(map[string]*dirProcessingSummary)
	dirOrder := make([]string, 0, len(stepResults))
	for _, result := range stepResults {
		sourcePath := strings.TrimSpace(result.SourcePath)
		if sourcePath == "" {
			sourcePath = "unknown"
		}
		dirSummary, exists := dirMap[sourcePath]
		if !exists {
			dirSummary = &dirProcessingSummary{
				SourcePath: sourcePath,
				Steps:      make([]dirStepSummary, 0, 2),
			}
			dirMap[sourcePath] = dirSummary
			dirOrder = append(dirOrder, sourcePath)
		}
		step := dirStepSummary{
			NodeType:  strings.TrimSpace(result.NodeType),
			NodeLabel: strings.TrimSpace(result.NodeLabel),
			Status:    strings.TrimSpace(result.Status),
			Error:     strings.TrimSpace(result.Error),
		}
		dirSummary.Steps = append(dirSummary.Steps, step)
		if isStepSucceeded(step.Status) {
			dirSummary.Succeeded++
			continue
		}
		dirSummary.Failed++
	}

	byDirectory := make([]dirProcessingSummary, 0, len(dirOrder))
	for _, path := range dirOrder {
		if item, ok := dirMap[path]; ok {
			byDirectory = append(byDirectory, *item)
		}
	}
	sort.SliceStable(byDirectory, func(i, j int) bool {
		if byDirectory[i].Failed == byDirectory[j].Failed {
			return byDirectory[i].SourcePath < byDirectory[j].SourcePath
		}
		return byDirectory[i].Failed > byDirectory[j].Failed
	})

	summary := processingResultPreviewSummary{
		TotalDirs:   len(byDirectory),
		TotalSteps:  len(stepResults),
		ByDirectory: byDirectory,
	}
	for _, item := range byDirectory {
		if item.Failed == 0 {
			summary.Succeeded++
			continue
		}
		summary.Failed++
	}

	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{
			"summary": {Type: PortTypeJSON, Value: summary},
		},
		Status: ExecutionSuccess,
	}, nil
}

func (e *processingResultPreviewNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *processingResultPreviewNodeExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func isSupportedStepResultInput(raw any) bool {
	switch raw.(type) {
	case ProcessingStepResult, *ProcessingStepResult, []ProcessingStepResult, []*ProcessingStepResult, []map[string]any, []any:
		return true
	default:
		return false
	}
}

func isStepSucceeded(status string) bool {
	normalized := strings.ToLower(strings.TrimSpace(status))
	return normalized == "moved" || normalized == "succeeded" || normalized == "success"
}
