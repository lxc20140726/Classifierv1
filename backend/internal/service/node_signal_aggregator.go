package service

import (
	"context"
	"fmt"
	"strings"
)

const signalAggregatorExecutorType = "signal-aggregator"

type signalAggregatorNodeExecutor struct{}

func newSignalAggregatorExecutor() *signalAggregatorNodeExecutor {
	return &signalAggregatorNodeExecutor{}
}

func (e *signalAggregatorNodeExecutor) Type() string {
	return signalAggregatorExecutorType
}

func (e *signalAggregatorNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "信号聚合器",
		Description: "聚合多路分类信号并输出分类条目",
		Inputs: []PortDef{
			{Name: "trees", Type: PortTypeFolderTreeList, Required: true, Description: "目录树列表"},
			{Name: "signal_kw", Type: PortTypeClassificationSignalList, Lazy: true, Description: "关键词分类器信号"},
			{Name: "signal_ft", Type: PortTypeClassificationSignalList, Lazy: true, Description: "文件树分类器信号"},
			{Name: "signal_ext", Type: PortTypeClassificationSignalList, Lazy: true, Description: "扩展名分类器信号"},
			{Name: "signal_high", Type: PortTypeClassificationSignalList, Lazy: true, Description: "置信度高信号"},
		},
		Outputs: []PortDef{
			{Name: "entries", Type: PortTypeClassifiedEntryList, RequiredOutput: true, Description: "聚合后的分类条目"},
		},
	}
}

func (e *signalAggregatorNodeExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	rawTrees, ok := firstPresentTyped(input.Inputs, "trees")
	if !ok {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: trees input is required", e.Type())
	}

	trees, found, err := parseFolderTreesInput(rawTrees)
	if err != nil {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute parse trees: %w", e.Type(), err)
	}
	if !found || len(trees) == 0 {
		return NodeExecutionOutput{
			Outputs: map[string]TypedValue{"entries": {Type: PortTypeClassifiedEntryList, Value: []ClassifiedEntry{}}},
			Status:  ExecutionSuccess,
		}, nil
	}

	rawInputs := typedInputsToAny(input.Inputs)
	signalsBySource, err := buildSignalIndex(rawInputs)
	if err != nil {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute build signal index: %w", e.Type(), err)
	}

	entries := make([]ClassifiedEntry, 0, len(trees))
	for _, tree := range trees {
		entry := buildClassifiedEntryFromTree(tree, signalsBySource)
		entries = append(entries, entry)
	}

	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{"entries": {Type: PortTypeClassifiedEntryList, Value: entries}},
		Status:  ExecutionSuccess,
	}, nil
}

func buildClassifiedEntryFromTree(tree FolderTree, signalsBySource map[string]map[string]ClassificationSignal) ClassifiedEntry {
	subtreeEntries := make([]ClassifiedEntry, 0, len(tree.Subdirs))
	for _, subdir := range tree.Subdirs {
		subtreeEntries = append(subtreeEntries, buildClassifiedEntryFromTree(subdir, signalsBySource))
	}

	bestSignal := pickBestSignalByPath(tree.Path, signalsBySource)
	summary := summarizeFolderTreeMedia(tree)
	directSummary := summarizeCurrentFolderMedia(tree)
	finalCategory, confidence, reason := aggregateTreeCategory(bestSignal, summary, directSummary)
	if strings.TrimSpace(finalCategory) == "" {
		finalCategory = "other"
	}

	entry := ClassifiedEntry{
		Path:          strings.TrimSpace(tree.Path),
		Name:          strings.TrimSpace(tree.Name),
		Category:      finalCategory,
		Confidence:    confidence,
		Reason:        reason,
		Classifier:    signalAggregatorExecutorType,
		HasOtherFiles: summary.hasOtherFiles,
		Files:         append([]FileEntry(nil), tree.Files...),
		Subtree:       subtreeEntries,
	}
	if entry.Name == "" && entry.Path != "" {
		entry.Name = entry.Path
	}
	return entry
}

func (e *signalAggregatorNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *signalAggregatorNodeExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}
