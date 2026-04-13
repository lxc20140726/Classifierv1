package service

import (
	"context"
	"fmt"
)

const classificationReaderExecutorType = "classification-reader"

type classificationReaderNodeExecutor struct{}

func newClassificationReaderExecutor() *classificationReaderNodeExecutor {
	return &classificationReaderNodeExecutor{}
}

func NewClassificationReaderExecutor() WorkflowNodeExecutor {
	return newClassificationReaderExecutor()
}

func (e *classificationReaderNodeExecutor) Type() string {
	return classificationReaderExecutorType
}

func (e *classificationReaderNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "分类读取器",
		Description: "分类管道到处理管道的桥接节点，读取 subtree-aggregator 的分类结果供处理链使用",
		Inputs: []PortDef{
			{Name: "entry", Type: PortTypeJSON, Description: "已分类条目（可选）", Required: false},
			{Name: "job_id", Type: PortTypeString, Description: "任务 ID（可选）", Required: false},
		},
		Outputs: []PortDef{
			{Name: "entry", Type: PortTypeJSON, Description: "已分类条目"},
		},
	}
}

func (e *classificationReaderNodeExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	rawInputs := typedInputsToAny(input.Inputs)
	entry, ok := classificationReaderResolveInputEntry(rawInputs)
	if !ok {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: entry input is required", e.Type())
	}

	if entry.Category == "" {
		entry.Category = "other"
	}
	if entry.Classifier == "" {
		entry.Classifier = e.Type()
	}

	return NodeExecutionOutput{Outputs: map[string]TypedValue{"entry": {Type: PortTypeJSON, Value: entry}}, Status: ExecutionSuccess}, nil
}

func (e *classificationReaderNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *classificationReaderNodeExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func classificationReaderResolveInputEntry(inputs map[string]any) (ClassifiedEntry, bool) {
	for _, key := range []string{"entry", "classified_entry"} {
		raw, ok := inputs[key]
		if !ok {
			continue
		}

		entry, ok := classificationReaderToEntry(raw)
		if !ok {
			continue
		}

		return entry, true
	}

	return ClassifiedEntry{}, false
}

func classificationReaderToEntry(raw any) (ClassifiedEntry, bool) {
	switch value := raw.(type) {
	case ClassifiedEntry:
		return value, true
	case *ClassifiedEntry:
		if value == nil {
			return ClassifiedEntry{}, false
		}
		return *value, true
	case map[string]any:
		entry := ClassifiedEntry{
			FolderID:      anyString(value["folder_id"]),
			Path:          anyString(value["path"]),
			Name:          anyString(value["name"]),
			Category:      anyString(value["category"]),
			Confidence:    asFloat64(value["confidence"]),
			Reason:        anyString(value["reason"]),
			Classifier:    anyString(value["classifier"]),
			HasOtherFiles: asBool(value["has_other_files"]),
		}
		return entry, true
	default:
		return ClassifiedEntry{}, false
	}
}
