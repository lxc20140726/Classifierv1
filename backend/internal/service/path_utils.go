package service

import (
	"path/filepath"
	"strings"
)

func normalizeWorkflowPath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}

	normalizedSeparators := strings.ReplaceAll(trimmed, "\\", "/")
	cleaned := filepath.Clean(normalizedSeparators)
	return filepath.ToSlash(cleaned)
}

func joinWorkflowPath(base, name string) string {
	return normalizeWorkflowPath(filepath.Join(strings.TrimSpace(base), strings.TrimSpace(name)))
}

func normalizeWindowsDriveLetter(path string) string {
	if len(path) < 2 {
		return path
	}
	drive := path[0]
	if path[1] != ':' || ((drive < 'A' || drive > 'Z') && (drive < 'a' || drive > 'z')) {
		return path
	}
	return strings.ToLower(path[:1]) + path[1:]
}
