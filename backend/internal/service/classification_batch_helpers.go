package service

import (
	"encoding/json"
	"fmt"
	"strings"
)

func parseFolderTreesInput(raw any) ([]FolderTree, bool, error) {
	if raw == nil {
		return nil, false, nil
	}

	switch value := raw.(type) {
	case []FolderTree:
		return append([]FolderTree(nil), value...), true, nil
	case []*FolderTree:
		out := make([]FolderTree, 0, len(value))
		for _, tree := range value {
			if tree == nil {
				continue
			}
			out = append(out, *tree)
		}
		return out, true, nil
	case FolderTree:
		return []FolderTree{value}, true, nil
	case *FolderTree:
		if value == nil {
			return nil, true, nil
		}
		return []FolderTree{*value}, true, nil
	case []any:
		out := make([]FolderTree, 0, len(value))
		for idx, item := range value {
			tree, ok, err := toFolderTree(item)
			if err != nil {
				return nil, true, fmt.Errorf("parseFolderTreesInput item %d: %w", idx, err)
			}
			if !ok {
				continue
			}
			out = append(out, tree)
		}
		return out, true, nil
	default:
		return nil, false, nil
	}
}

func toFolderTree(raw any) (FolderTree, bool, error) {
	switch value := raw.(type) {
	case FolderTree:
		return value, true, nil
	case *FolderTree:
		if value == nil {
			return FolderTree{}, false, nil
		}
		return *value, true, nil
	case map[string]any:
		encoded, err := json.Marshal(value)
		if err != nil {
			return FolderTree{}, false, fmt.Errorf("marshal map folder tree: %w", err)
		}
		var tree FolderTree
		if err := json.Unmarshal(encoded, &tree); err != nil {
			return FolderTree{}, false, fmt.Errorf("unmarshal map folder tree: %w", err)
		}
		return tree, true, nil
	default:
		return FolderTree{}, false, nil
	}
}

func parseSignalListInput(raw any) ([]ClassificationSignal, bool, error) {
	if raw == nil {
		return nil, false, nil
	}

	switch value := raw.(type) {
	case []ClassificationSignal:
		return append([]ClassificationSignal(nil), value...), true, nil
	case []*ClassificationSignal:
		out := make([]ClassificationSignal, 0, len(value))
		for _, signal := range value {
			if signal == nil {
				continue
			}
			out = append(out, *signal)
		}
		return out, true, nil
	case ClassificationSignal:
		return []ClassificationSignal{value}, true, nil
	case *ClassificationSignal:
		if value == nil {
			return nil, true, nil
		}
		return []ClassificationSignal{*value}, true, nil
	case []any:
		out := make([]ClassificationSignal, 0, len(value))
		for idx, item := range value {
			signal, ok, err := toClassificationSignal(item)
			if err != nil {
				return nil, true, fmt.Errorf("parseSignalListInput item %d: %w", idx, err)
			}
			if !ok {
				continue
			}
			out = append(out, signal)
		}
		return out, true, nil
	default:
		return nil, false, nil
	}
}

func toClassificationSignal(raw any) (ClassificationSignal, bool, error) {
	switch value := raw.(type) {
	case ClassificationSignal:
		return value, true, nil
	case *ClassificationSignal:
		if value == nil {
			return ClassificationSignal{}, false, nil
		}
		return *value, true, nil
	case map[string]any:
		encoded, err := json.Marshal(value)
		if err != nil {
			return ClassificationSignal{}, false, fmt.Errorf("marshal map signal: %w", err)
		}
		var signal ClassificationSignal
		if err := json.Unmarshal(encoded, &signal); err != nil {
			return ClassificationSignal{}, false, fmt.Errorf("unmarshal map signal: %w", err)
		}
		return signal, true, nil
	default:
		return ClassificationSignal{}, false, nil
	}
}

func firstPresent(input map[string]any, keys ...string) (any, bool) {
	for _, key := range keys {
		value, ok := input[key]
		if !ok {
			continue
		}
		if value == nil {
			continue
		}
		return value, true
	}

	return nil, false
}

func typedInputsToAny(inputs map[string]*TypedValue) map[string]any {
	out := make(map[string]any, len(inputs))
	for key, typed := range inputs {
		if typed == nil {
			out[key] = nil
			continue
		}
		out[key] = typed.Value
	}

	return out
}

func compactPaths(paths []string) []string {
	seen := make(map[string]struct{}, len(paths))
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		trimmed := strings.TrimSpace(path)
		if trimmed == "" {
			continue
		}
		if _, ok := seen[trimmed]; ok {
			continue
		}
		seen[trimmed] = struct{}{}
		out = append(out, trimmed)
	}

	return out
}

func asFloat64(value any) float64 {
	switch v := value.(type) {
	case float64:
		return v
	case float32:
		return float64(v)
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case int32:
		return float64(v)
	default:
		return 0
	}
}

func asBool(value any) bool {
	switch v := value.(type) {
	case bool:
		return v
	case int:
		return v != 0
	case int64:
		return v != 0
	case float64:
		return v != 0
	default:
		return false
	}
}
