package service

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
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
	inputs := make([]PortDef, 0, 8)
	for index := 1; index <= 8; index++ {
		inputs = append(inputs, PortDef{
			Name:        fmt.Sprintf("items_%d", index),
			Type:        PortTypeProcessingItemList,
			Lazy:        true,
			Description: fmt.Sprintf("并行分支%d", index),
		})
	}

	return NodeSchema{
		Type:        e.Type(),
		Label:       "收集节点",
		Description: "将多条并行分支的处理项合并为一个列表",
		Inputs:      inputs,
		Outputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, RequiredOutput: true, Description: "合并后的处理项列表"},
		},
	}
}

func (e *collectNodeExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	merged := make([]ProcessingItem, 0)
	for _, key := range collectNodeInputKeys(input.Inputs) {
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

func collectNodeInputKeys(inputs map[string]*TypedValue) []string {
	type numberedInput struct {
		key   string
		index int
	}

	numbered := make([]numberedInput, 0, len(inputs))
	for key := range inputs {
		if !strings.HasPrefix(key, "items_") {
			continue
		}
		index, err := strconv.Atoi(strings.TrimPrefix(key, "items_"))
		if err != nil || index <= 0 {
			continue
		}
		numbered = append(numbered, numberedInput{key: key, index: index})
	}

	sort.Slice(numbered, func(i, j int) bool {
		if numbered[i].index == numbered[j].index {
			return numbered[i].key < numbered[j].key
		}
		return numbered[i].index < numbered[j].index
	})

	keys := make([]string, 0, len(numbered))
	for _, item := range numbered {
		keys = append(keys, item.key)
	}
	return keys
}
