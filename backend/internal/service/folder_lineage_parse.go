package service

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/liqiye/classifier/internal/repository"
)

func parseRawObject(raw json.RawMessage) map[string]any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	out := map[string]any{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{}
	}
	return out
}

func parseReviewSteps(raw json.RawMessage) []FolderLineageReviewStep {
	if len(raw) == 0 {
		return []FolderLineageReviewStep{}
	}
	var payload []map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return []FolderLineageReviewStep{}
	}
	out := make([]FolderLineageReviewStep, 0, len(payload))
	for _, item := range payload {
		step := FolderLineageReviewStep{
			NodeType:   strings.TrimSpace(lineageAnyString(item["node_type"])),
			NodeLabel:  strings.TrimSpace(lineageAnyString(item["node_label"])),
			Status:     strings.TrimSpace(lineageAnyString(item["status"])),
			SourcePath: normalizeLineagePath(lineageAnyString(item["source_path"])),
			TargetPath: normalizeLineagePath(resolveStepTargetPath(lineageAnyString(item["source_path"]), lineageAnyString(item["target_path"]))),
			Error:      strings.TrimSpace(lineageAnyString(item["error"])),
		}
		if step.NodeType == "" && step.SourcePath == "" && step.TargetPath == "" {
			continue
		}
		out = append(out, step)
	}
	return out
}

func parseDiffExecutedSteps(diff map[string]any) []FolderLineageReviewStep {
	if len(diff) == 0 {
		return []FolderLineageReviewStep{}
	}
	raw, ok := diff["executed_steps"]
	if !ok {
		return []FolderLineageReviewStep{}
	}
	arrayPayload, ok := raw.([]any)
	if !ok {
		return []FolderLineageReviewStep{}
	}
	out := make([]FolderLineageReviewStep, 0, len(arrayPayload))
	for _, item := range arrayPayload {
		entry, ok := item.(map[string]any)
		if !ok {
			continue
		}
		nodeType := strings.TrimSpace(lineageAnyString(entry["node_type"]))
		if nodeType == "" {
			continue
		}
		out = append(out, FolderLineageReviewStep{
			NodeType:   nodeType,
			NodeLabel:  strings.TrimSpace(lineageAnyString(entry["node_label"])),
			Status:     strings.TrimSpace(lineageAnyString(entry["status"])),
			SourcePath: normalizeLineagePath(lineageAnyString(entry["source_path"])),
			TargetPath: normalizeLineagePath(resolveStepTargetPath(lineageAnyString(entry["source_path"]), lineageAnyString(entry["target_path"]))),
		})
	}
	return out
}

func extractArtifactPaths(after map[string]any) []string {
	if len(after) == 0 {
		return []string{}
	}
	raw := after["artifact_paths"]
	arrayPayload, ok := raw.([]any)
	if !ok {
		return []string{}
	}
	out := make([]string, 0, len(arrayPayload))
	for _, item := range arrayPayload {
		artifactPath := strings.TrimSpace(lineageAnyString(item))
		if artifactPath == "" {
			continue
		}
		out = append(out, artifactPath)
	}
	return out
}

func resolveArtifactStepType(artifactPath string, steps []FolderLineageReviewStep) string {
	for _, step := range steps {
		targetPath := normalizeLineagePath(step.TargetPath)
		if targetPath == normalizeLineagePath(artifactPath) {
			return strings.TrimSpace(step.NodeType)
		}
	}
	return ""
}

func resolveStepTargetPath(sourcePath string, targetPath string) string {
	normalizedSource := normalizeLineagePath(sourcePath)
	normalizedTarget := normalizeLineagePath(targetPath)
	if normalizedTarget == "" {
		return ""
	}
	if filepath.IsAbs(normalizedTarget) || strings.Contains(normalizedTarget, "/") {
		return normalizedTarget
	}
	if normalizedSource == "" {
		return normalizedTarget
	}
	return normalizeLineagePath(filepath.Join(filepath.Dir(normalizedSource), normalizedTarget))
}

func classifyPathTransition(fromPath string, toPath string) (string, string) {
	normalizedFrom := normalizeLineagePath(fromPath)
	normalizedTo := normalizeLineagePath(toPath)
	if normalizedFrom == "" || normalizedTo == "" {
		return folderLineageEdgeTypeMovedTo, folderLineageEventTypeMove
	}
	if filepath.Dir(normalizedFrom) == filepath.Dir(normalizedTo) && filepath.Base(normalizedFrom) != filepath.Base(normalizedTo) {
		return folderLineageEdgeTypeRenamedTo, folderLineageEventTypeRename
	}
	return folderLineageEdgeTypeMovedTo, folderLineageEventTypeMove
}

func normalizeLineagePath(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	return strings.ReplaceAll(normalizeWindowsDriveLetter(filepath.Clean(trimmed)), `\`, "/")
}

func firstNonNilObservation(items []*repository.FolderPathObservation) *repository.FolderPathObservation {
	ordered := make([]*repository.FolderPathObservation, 0, len(items))
	for _, item := range items {
		if item == nil {
			continue
		}
		ordered = append(ordered, item)
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].FirstSeenAt.Equal(ordered[j].FirstSeenAt) {
			if ordered[i].LastSeenAt.Equal(ordered[j].LastSeenAt) {
				return normalizeLineagePath(ordered[i].Path) < normalizeLineagePath(ordered[j].Path)
			}
			return ordered[i].LastSeenAt.Before(ordered[j].LastSeenAt)
		}
		return ordered[i].FirstSeenAt.Before(ordered[j].FirstSeenAt)
	})
	if len(ordered) == 0 {
		return nil
	}
	return ordered[0]
}

func sortedKeys(input map[string]struct{}) []string {
	if len(input) == 0 {
		return []string{}
	}
	out := make([]string, 0, len(input))
	for key := range input {
		if strings.TrimSpace(key) == "" {
			continue
		}
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func lineageAnyString(value any) string {
	switch typed := value.(type) {
	case string:
		return typed
	default:
		return fmt.Sprint(value)
	}
}

func isFailedAuditLog(log *repository.AuditLog) bool {
	if log == nil {
		return false
	}
	if strings.EqualFold(strings.TrimSpace(log.Result), "failed") {
		return true
	}
	if strings.EqualFold(strings.TrimSpace(log.Level), "error") {
		return true
	}
	return strings.TrimSpace(log.ErrorMsg) != ""
}
