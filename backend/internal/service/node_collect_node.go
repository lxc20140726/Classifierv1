package service

import (
	"context"
	"fmt"
)

const collectNodeExecutorType = "collect-node"

type collectNodeExecutor struct{}

func newCollectNodeExecutor() *collectNodeExecutor {
	return &collectNodeExecutor{}
}

func (e *collectNodeExecutor) Type() string {
	return collectNodeExecutorType
}

func (e *collectNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "收集节点",
		Description: "将多条并行分支的处理项合并为一个列表",
		Inputs: []PortDef{
			{Name: "items_1", Type: PortTypeProcessingItemList, Lazy: true, Description: "并行分支1"},
			{Name: "items_2", Type: PortTypeProcessingItemList, Lazy: true, Description: "并行分支2"},
			{Name: "items_3", Type: PortTypeProcessingItemList, Lazy: true, Description: "并行分支3"},
			{Name: "items_4", Type: PortTypeProcessingItemList, Lazy: true, Description: "并行分支4"},
			{Name: "items_5", Type: PortTypeProcessingItemList, Lazy: true, Description: "并行分支5"},
		},
		Outputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, RequiredOutput: true, Description: "合并后的处理项列表"},
		},
	}
}

func (e *collectNodeExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	merged := make([]ProcessingItem, 0)
	for index := 1; index <= 5; index++ {
		key := fmt.Sprintf("items_%d", index)
		value, ok := input.Inputs[key]
		if !ok || value == nil || value.Value == nil {
			continue
		}
		items, ok := categoryRouterToItems(value.Value)
		if !ok {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: %s must be processing item list", e.Type(), key)
		}
		merged = append(merged, items...)
	}

	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{
			"items": {Type: PortTypeProcessingItemList, Value: merged},
		},
		Status: ExecutionSuccess,
	}, nil
}

func (e *collectNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *collectNodeExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}
