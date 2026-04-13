package service

import (
	"fmt"
	"strings"
)

func processingStepResultsFromAny(raw any) []ProcessingStepResult {
	switch typed := raw.(type) {
	case ProcessingStepResult:
		return []ProcessingStepResult{typed}
	case *ProcessingStepResult:
		if typed == nil {
			return nil
		}
		return []ProcessingStepResult{*typed}
	case []ProcessingStepResult:
		return append([]ProcessingStepResult(nil), typed...)
	case []*ProcessingStepResult:
		out := make([]ProcessingStepResult, 0, len(typed))
		for _, item := range typed {
			if item == nil {
				continue
			}
			out = append(out, *item)
		}
		return out
	case []map[string]any:
		out := make([]ProcessingStepResult, 0, len(typed))
		for _, item := range typed {
			out = append(out, processingStepResultFromMap(item))
		}
		return out
	case []any:
		out := make([]ProcessingStepResult, 0, len(typed))
		for _, item := range typed {
			switch v := item.(type) {
			case ProcessingStepResult:
				out = append(out, v)
			case *ProcessingStepResult:
				if v != nil {
					out = append(out, *v)
				}
			case map[string]any:
				out = append(out, processingStepResultFromMap(v))
			}
		}
		return out
	default:
		return nil
	}
}

func processingStepResultFromMap(raw map[string]any) ProcessingStepResult {
	result := ProcessingStepResult{}
	if value, ok := raw["folder_id"]; ok {
		result.FolderID = strings.TrimSpace(anyValueToString(value))
	}
	if value, ok := raw["source_path"]; ok {
		result.SourcePath = strings.TrimSpace(anyValueToString(value))
	}
	if value, ok := raw["target_path"]; ok {
		result.TargetPath = strings.TrimSpace(anyValueToString(value))
	}
	if value, ok := raw["node_type"]; ok {
		result.NodeType = strings.TrimSpace(anyValueToString(value))
	}
	if value, ok := raw["node_label"]; ok {
		result.NodeLabel = strings.TrimSpace(anyValueToString(value))
	}
	if value, ok := raw["status"]; ok {
		result.Status = strings.TrimSpace(anyValueToString(value))
	}
	if value, ok := raw["error"]; ok {
		result.Error = strings.TrimSpace(anyValueToString(value))
	}
	return result
}

func anyValueToString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(value)
	}
}
