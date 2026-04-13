package service

import (
	"strconv"
	"strings"

	"github.com/liqiye/classifier/internal/repository"
)

const (
	workflowPathRefTypeScan   = "scan"
	workflowPathRefTypeOutput = "output"
	workflowPathRefTypeCustom = "custom"
)

type workflowNodePathOptions struct {
	DefaultType      string
	DefaultOutputKey string
	LegacyKeys       []string
}

func resolveWorkflowNodePath(config map[string]any, appConfig *repository.AppConfig, options workflowNodePathOptions) string {
	refType := strings.ToLower(strings.TrimSpace(stringConfig(config, "path_ref_type")))
	refKey := strings.TrimSpace(stringConfig(config, "path_ref_key"))
	legacyRefType, legacyRefKey := legacyPathRefFromConfig(config)
	if refType == "" {
		refType = legacyRefType
	}
	if refKey == "" {
		refKey = legacyRefKey
	}
	suffix := normalizeWorkflowPath(strings.TrimSpace(stringConfig(config, "path_suffix")))
	legacyPath := firstLegacyNodePath(config, options.LegacyKeys...)

	if refType == "" {
		if legacyPath != "" {
			refType = workflowPathRefTypeCustom
			refKey = legacyPath
		} else if strings.TrimSpace(options.DefaultType) != "" {
			refType = strings.ToLower(strings.TrimSpace(options.DefaultType))
		}
	}

	var base string
	switch refType {
	case workflowPathRefTypeOutput:
		outputKey := refKey
		if outputKey == "" {
			outputKey = strings.TrimSpace(options.DefaultOutputKey)
		}
		base = resolveOutputDirByKey(appConfig, outputKey)
		if base == "" && looksLikePath(outputKey) {
			base = normalizeWorkflowPath(outputKey)
		}
	case workflowPathRefTypeScan:
		base = resolveScanDirByKey(appConfig, refKey)
		if base == "" && looksLikePath(refKey) {
			base = normalizeWorkflowPath(refKey)
		}
	default:
		base = normalizeWorkflowPath(refKey)
	}

	if base == "" {
		base = legacyPath
	}
	if base == "" {
		return normalizeWorkflowPath(suffix)
	}
	if suffix == "" {
		return normalizeWorkflowPath(base)
	}
	return joinWorkflowPath(base, suffix)
}

func legacyPathRefFromConfig(config map[string]any) (string, string) {
	source := strings.ToLower(strings.TrimSpace(
		firstNonEmptyStringConfig(config, "target_dir_source", "output_dir_source"),
	))
	optionID := strings.TrimSpace(
		firstNonEmptyStringConfig(config, "target_dir_option_id", "output_dir_option_id"),
	)
	if optionID == "" {
		return "", ""
	}
	switch source {
	case workflowPathRefTypeOutput, "target", "target_dir", "output_dir":
		return workflowPathRefTypeOutput, optionID
	case workflowPathRefTypeScan, "source", "source_dir", "scan_input":
		return workflowPathRefTypeScan, optionID
	case workflowPathRefTypeCustom:
		return workflowPathRefTypeCustom, optionID
	default:
		if looksLikePath(optionID) {
			return workflowPathRefTypeCustom, optionID
		}
		return workflowPathRefTypeOutput, optionID
	}
}

func firstNonEmptyStringConfig(config map[string]any, keys ...string) string {
	for _, key := range keys {
		value := strings.TrimSpace(stringConfig(config, key))
		if value != "" {
			return value
		}
	}
	return ""
}

func firstLegacyNodePath(config map[string]any, keys ...string) string {
	for _, key := range keys {
		value := normalizeWorkflowPath(stringConfig(config, key))
		if value != "" {
			return value
		}
	}
	return ""
}

func resolveOutputDirByKey(appConfig *repository.AppConfig, key string) string {
	if appConfig == nil {
		return ""
	}
	category, index := parseOutputDirRef(key)
	if category == "" || index < 0 {
		return ""
	}

	var dirs []string
	switch category {
	case "video":
		dirs = appConfig.OutputDirs.Video
	case "manga":
		dirs = appConfig.OutputDirs.Manga
	case "photo":
		dirs = appConfig.OutputDirs.Photo
	case "other":
		dirs = appConfig.OutputDirs.Other
	case "mixed":
		dirs = appConfig.OutputDirs.Mixed
	default:
		return ""
	}
	if index >= len(dirs) {
		return ""
	}
	return normalizeWorkflowPath(dirs[index])
}

func parseOutputDirRef(raw string) (string, int) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", -1
	}

	parts := strings.SplitN(trimmed, ":", 2)
	category := strings.ToLower(strings.TrimSpace(parts[0]))
	if category == "" {
		return "", -1
	}
	if len(parts) == 1 {
		return category, 0
	}

	index, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || index < 0 {
		return "", -1
	}
	return category, index
}

func resolveScanDirByKey(appConfig *repository.AppConfig, key string) string {
	if appConfig == nil || len(appConfig.ScanInputDirs) == 0 {
		return ""
	}
	trimmed := strings.TrimSpace(key)
	if trimmed == "" {
		return normalizeWorkflowPath(appConfig.ScanInputDirs[0])
	}
	index, err := strconv.Atoi(trimmed)
	if err == nil {
		if index >= 0 && index < len(appConfig.ScanInputDirs) {
			return normalizeWorkflowPath(appConfig.ScanInputDirs[index])
		}
		return ""
	}
	for _, item := range appConfig.ScanInputDirs {
		if normalizeWorkflowPath(item) == normalizeWorkflowPath(trimmed) {
			return normalizeWorkflowPath(item)
		}
	}
	return ""
}

func looksLikePath(value string) bool {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return false
	}
	return strings.Contains(trimmed, "/") || strings.Contains(trimmed, "\\") || strings.HasPrefix(trimmed, ".")
}
