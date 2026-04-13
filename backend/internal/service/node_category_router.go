package service

import (
	"context"
	"fmt"
	"strings"
)

const categoryRouterExecutorType = "category-router"

type categoryRouterNodeExecutor struct{}

func newCategoryRouterExecutor() *categoryRouterNodeExecutor {
	return &categoryRouterNodeExecutor{}
}

func NewCategoryRouterExecutor() WorkflowNodeExecutor {
	return newCategoryRouterExecutor()
}

func (e *categoryRouterNodeExecutor) Type() string {
	return categoryRouterExecutorType
}

func (e *categoryRouterNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "类别路由器",
		Description: "按分类类别将处理项路由至对应输出端口（video / manga / photo / other / mixed_leaf）",
		Inputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, Description: "待路由的处理项列表", Required: true},
		},
		Outputs: []PortDef{
			{Name: "video", Type: PortTypeProcessingItemList, AllowEmpty: true, Description: "视频类处理项"},
			{Name: "manga", Type: PortTypeProcessingItemList, AllowEmpty: true, Description: "漫画类处理项"},
			{Name: "photo", Type: PortTypeProcessingItemList, AllowEmpty: true, Description: "图片类处理项"},
			{Name: "other", Type: PortTypeProcessingItemList, AllowEmpty: true, Description: "其他类处理项"},
			{Name: "mixed_leaf", Type: PortTypeProcessingItemList, AllowEmpty: true, Description: "混合叶节点处理项"},
		},
	}
}

func (e *categoryRouterNodeExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	items, ok := categoryRouterExtractItems(input.Inputs)
	if !ok {
		return NodeExecutionOutput{Outputs: emptyCategoryRouterOutputs(), Status: ExecutionSuccess}, nil
	}

	ports := make([][]ProcessingItem, 5)
	for _, item := range items {
		idx := categoryRouterPortIndex(item.Category)
		ports[idx] = append(ports[idx], item)
	}

	return NodeExecutionOutput{Outputs: map[string]TypedValue{
		"video":      {Type: PortTypeProcessingItemList, Value: append([]ProcessingItem(nil), ports[0]...)},
		"manga":      {Type: PortTypeProcessingItemList, Value: append([]ProcessingItem(nil), ports[1]...)},
		"photo":      {Type: PortTypeProcessingItemList, Value: append([]ProcessingItem(nil), ports[2]...)},
		"other":      {Type: PortTypeProcessingItemList, Value: append([]ProcessingItem(nil), ports[3]...)},
		"mixed_leaf": {Type: PortTypeProcessingItemList, Value: append([]ProcessingItem(nil), ports[4]...)},
	}, Status: ExecutionSuccess}, nil
}

func emptyCategoryRouterOutputs() map[string]TypedValue {
	return map[string]TypedValue{
		"video":      {Type: PortTypeProcessingItemList, Value: []ProcessingItem{}},
		"manga":      {Type: PortTypeProcessingItemList, Value: []ProcessingItem{}},
		"photo":      {Type: PortTypeProcessingItemList, Value: []ProcessingItem{}},
		"other":      {Type: PortTypeProcessingItemList, Value: []ProcessingItem{}},
		"mixed_leaf": {Type: PortTypeProcessingItemList, Value: []ProcessingItem{}},
	}
}

func (e *categoryRouterNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *categoryRouterNodeExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func categoryRouterExtractItems(inputs map[string]*TypedValue) ([]ProcessingItem, bool) {
	for _, key := range []string{"items", "item"} {
		typed, ok := inputs[key]
		if !ok || typed == nil {
			continue
		}

		items, ok := categoryRouterToItems(typed.Value)
		if !ok {
			continue
		}

		return items, true
	}

	return nil, false
}

func categoryRouterToItems(raw any) ([]ProcessingItem, bool) {
	switch value := raw.(type) {
	case ProcessingItem:
		return []ProcessingItem{processingItemNormalize(value)}, true
	case *ProcessingItem:
		if value == nil {
			return nil, false
		}
		return []ProcessingItem{processingItemNormalize(*value)}, true
	case []ProcessingItem:
		out := make([]ProcessingItem, 0, len(value))
		for _, item := range value {
			out = append(out, processingItemNormalize(item))
		}
		return out, true
	case []*ProcessingItem:
		out := make([]ProcessingItem, 0, len(value))
		for _, item := range value {
			if item == nil {
				continue
			}
			out = append(out, processingItemNormalize(*item))
		}
		return out, true
	case []any:
		out := make([]ProcessingItem, 0, len(value))
		for _, item := range value {
			parsed, ok := categoryRouterToItem(item)
			if !ok {
				continue
			}
			out = append(out, parsed)
		}
		return out, true
	default:
		parsed, ok := categoryRouterToItem(value)
		if !ok {
			return nil, false
		}
		return []ProcessingItem{parsed}, true
	}
}

func categoryRouterToItem(raw any) (ProcessingItem, bool) {
	switch value := raw.(type) {
	case ProcessingItem:
		return processingItemNormalize(value), true
	case *ProcessingItem:
		if value == nil {
			return ProcessingItem{}, false
		}
		return processingItemNormalize(*value), true
	case map[string]any:
		item := ProcessingItem{
			SourcePath:         anyString(value["source_path"]),
			CurrentPath:        anyString(value["current_path"]),
			FolderID:           anyString(value["folder_id"]),
			FolderName:         anyString(value["folder_name"]),
			TargetName:         anyString(value["target_name"]),
			Category:           anyString(value["category"]),
			ParentPath:         anyString(value["parent_path"]),
			RootPath:           anyString(value["root_path"]),
			RelativePath:       anyString(value["relative_path"]),
			SourceKind:         anyString(value["source_kind"]),
			OriginalSourcePath: anyString(value["original_source_path"]),
		}
		return processingItemNormalize(item), true
	default:
		return ProcessingItem{}, false
	}
}

func categoryRouterPortIndex(category string) int {
	switch strings.ToLower(strings.TrimSpace(category)) {
	case "video":
		return 0
	case "manga":
		return 1
	case "photo":
		return 2
	case "mixed":
		return 4
	default:
		return 3
	}
}
