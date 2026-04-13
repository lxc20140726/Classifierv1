package service

import (
	"path/filepath"
	"strings"
)

func processingItemNormalize(item ProcessingItem) ProcessingItem {
	item.SourcePath = normalizeWorkflowPath(item.SourcePath)
	item.CurrentPath = normalizeWorkflowPath(item.CurrentPath)
	item.ParentPath = normalizeWorkflowPath(item.ParentPath)
	item.RootPath = normalizeWorkflowPath(item.RootPath)
	item.RelativePath = normalizeWorkflowPath(item.RelativePath)
	item.OriginalSourcePath = normalizeWorkflowPath(item.OriginalSourcePath)
	item.SourceKind = strings.ToLower(strings.TrimSpace(item.SourceKind))

	if item.SourcePath == "" {
		item.SourcePath = item.OriginalSourcePath
	}
	if item.CurrentPath == "" {
		item.CurrentPath = item.SourcePath
	}
	if item.SourcePath == "" {
		item.SourcePath = item.CurrentPath
	}
	if item.ParentPath == "" && item.CurrentPath != "" {
		item.ParentPath = normalizeWorkflowPath(filepath.Dir(item.CurrentPath))
	}
	if strings.TrimSpace(item.FolderName) == "" && item.CurrentPath != "" {
		item.FolderName = strings.TrimSpace(filepath.Base(item.CurrentPath))
	}

	return item
}

func processingItemCurrentPath(item ProcessingItem) string {
	normalized := processingItemNormalize(item)
	if normalized.CurrentPath != "" {
		return normalized.CurrentPath
	}
	return normalized.SourcePath
}

func processingItemMediaPath(item ProcessingItem) string {
	normalized := processingItemNormalize(item)
	if normalized.CurrentPath != "" {
		return normalized.CurrentPath
	}
	return normalized.SourcePath
}

func processingItemRootName(item ProcessingItem) string {
	normalized := processingItemNormalize(item)
	rootPath := strings.TrimSpace(normalized.RootPath)
	if rootPath == "" {
		return ""
	}
	rootName := strings.TrimSpace(filepath.Base(rootPath))
	if rootName == "" || rootName == "." || rootName == string(filepath.Separator) {
		return ""
	}
	return rootName
}

func processingItemArtifactName(item ProcessingItem) string {
	if rootName := processingItemRootName(item); rootName != "" {
		return rootName
	}
	return phase4MoveItemName(item)
}
