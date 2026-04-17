package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

const phase4MoveNodeExecutorType = "move-node"

type phase4MoveNodeExecutor struct {
	fs      fs.FSAdapter
	folders repository.FolderRepository
}

func newPhase4MoveNodeExecutor(fsAdapter fs.FSAdapter, folderRepo repository.FolderRepository) *phase4MoveNodeExecutor {
	return &phase4MoveNodeExecutor{fs: fsAdapter, folders: folderRepo}
}

func (e *phase4MoveNodeExecutor) Type() string {
	return phase4MoveNodeExecutorType
}

func (e *phase4MoveNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "移动节点",
		Description: "将处理项归并到来源根目录目标产物目录，支持冲突策略和操作回滚",
		Inputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, Description: "待归并的处理项列表", Required: true, SkipOnEmpty: true, AcceptDefault: true},
		},
		Outputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, RequiredOutput: true, Description: "归并后的处理项列表（结构兼容输出）"},
		},
	}
}

func (e *phase4MoveNodeExecutor) Execute(ctx context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	items, ok := categoryRouterExtractItems(input.Inputs)
	if !ok || len(items) == 0 {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: items input is required", e.Type())
	}

	targetDir := resolveWorkflowNodePath(input.Node.Config, input.AppConfig, workflowNodePathOptions{
		DefaultType:      workflowPathRefTypeOutput,
		DefaultOutputKey: "mixed",
		LegacyKeys:       []string{"target_dir", "targetDir", "output_dir"},
	})
	if targetDir == "" {
		targetDir = phase4MoveResolveOutputFallbackTargetDir(items, input.Node.Config, input.AppConfig)
	}
	if targetDir == "" {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: target_dir is required (path_ref_type=%q, path_ref_key=%q)", e.Type(), strings.TrimSpace(stringConfig(input.Node.Config, "path_ref_type")), strings.TrimSpace(stringConfig(input.Node.Config, "path_ref_key")))
	}

	moveUnit := strings.ToLower(strings.TrimSpace(stringConfig(input.Node.Config, "move_unit")))
	if moveUnit == "" {
		moveUnit = "folder"
	}
	if moveUnit != "folder" {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: unsupported move_unit %q, only folder is supported", e.Type(), moveUnit)
	}

	if !folderSplitterBoolConfig(input.Node.Config, "preserve_substructure", true) {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: preserve_substructure=false is not supported", e.Type())
	}

	createTarget := folderSplitterBoolConfig(input.Node.Config, "create_target_if_missing", true)
	targetExists, err := e.fs.Exists(ctx, targetDir)
	if err != nil {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: check target dir %q: %w", e.Type(), targetDir, err)
	}
	if !targetExists {
		if !createTarget {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: target dir %q does not exist and create_target_if_missing is false", e.Type(), targetDir)
		}
		if err := e.fs.MkdirAll(ctx, targetDir, 0o755); err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: create target dir %q: %w", e.Type(), targetDir, err)
		}
	}

	conflictPolicy := strings.ToLower(strings.TrimSpace(stringConfig(input.Node.Config, "conflict_policy")))
	if conflictPolicy == "" {
		conflictPolicy = "auto_rename"
	}
	if conflictPolicy != "auto_rename" && conflictPolicy != "skip" && conflictPolicy != "overwrite" {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: unsupported conflict_policy %q", e.Type(), conflictPolicy)
	}

	if phase4MoveUseLegacyMode(items) {
		return e.executeLegacyMove(ctx, input, items, targetDir, conflictPolicy)
	}

	_, rootName, normalizedItems, err := e.normalizeAndValidateItems(ctx, items)
	if err != nil {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: %w", e.Type(), err)
	}

	stepResults := make([]ProcessingStepResult, 0, len(normalizedItems))
	outputItems := make([]ProcessingItem, 0, len(normalizedItems))
	for index, item := range normalizedItems {
		itemTargetDir := phase4MoveResolveItemTargetDir(item, input.Node.Config, input.AppConfig, targetDir)
		if strings.TrimSpace(itemTargetDir) == "" {
			itemTargetDir = targetDir
		}
		targetRoot := joinWorkflowPath(itemTargetDir, rootName)
		if err := e.fs.MkdirAll(ctx, targetRoot, 0o755); err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: create target root %q: %w", e.Type(), targetRoot, err)
		}

		switch item.SourceKind {
		case ProcessingItemSourceKindDirectory:
			results, processErr := e.moveDirectoryArtifacts(ctx, item, targetRoot, conflictPolicy, input)
			if processErr != nil {
				return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: %w", e.Type(), processErr)
			}
			stepResults = append(stepResults, results...)
		case ProcessingItemSourceKindArchive:
			result, processErr := e.moveArchiveArtifact(ctx, item, targetRoot, conflictPolicy, input)
			if processErr != nil {
				return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: %w", e.Type(), processErr)
			}
			stepResults = append(stepResults, result)
		default:
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: unsupported source_kind %q", e.Type(), item.SourceKind)
		}
		item.CurrentPath = normalizeWorkflowPath(targetRoot)
		item.ParentPath = itemTargetDir
		item.FolderName = rootName
		item.TargetName = rootName
		if e.folders != nil && strings.TrimSpace(item.FolderID) != "" && item.SourceKind == ProcessingItemSourceKindDirectory {
			if err := e.folders.UpdatePath(ctx, item.FolderID, item.CurrentPath, itemTargetDir, rootName); err != nil {
				return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: update folder path for %q: %w", e.Type(), item.FolderID, err)
			}
		}
		outputItems = append(outputItems, item)

		if input.ProgressFn != nil {
			percent := (index + 1) * 100 / len(normalizedItems)
			input.ProgressFn(percent, fmt.Sprintf("已完成 %d/%d 项归并", index+1, len(normalizedItems)))
		}
	}

	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{
			"items":        {Type: PortTypeProcessingItemList, Value: outputItems},
			"step_results": {Type: PortTypeProcessingStepResultList, Value: stepResults},
		},
		Status: ExecutionSuccess,
	}, nil
}

func (e *phase4MoveNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *phase4MoveNodeExecutor) Rollback(ctx context.Context, input NodeRollbackInput) error {
	entries, err := phase4MoveCollectRollbackEntries(input)
	if err != nil {
		return fmt.Errorf("%s.Rollback: %w", e.Type(), err)
	}
	entries = phase4MoveFilterRollbackEntries(entries, input.Folder)

	targetRoots := map[string]struct{}{}
	for _, entry := range entries {
		entry.TargetPath = normalizeWorkflowPath(entry.TargetPath)
		entry.SourcePath = normalizeWorkflowPath(entry.SourcePath)
		if strings.TrimSpace(entry.TargetPath) == "" || strings.TrimSpace(entry.SourcePath) == "" || entry.TargetPath == entry.SourcePath {
			continue
		}

		exists, err := e.fs.Exists(ctx, entry.TargetPath)
		if err != nil {
			return fmt.Errorf("check moved artifact %q: %w", entry.TargetPath, err)
		}
		if !exists {
			continue
		}
		info, err := e.fs.Stat(ctx, entry.TargetPath)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return fmt.Errorf("stat moved artifact %q: %w", entry.TargetPath, err)
		}

		folderID := strings.TrimSpace(entry.FolderID)
		if folderID == "" && input.Folder != nil {
			folderID = strings.TrimSpace(input.Folder.ID)
		}
		if folderID == "" && e.folders != nil {
			if folder, err := e.folders.GetCurrentByPath(ctx, entry.TargetPath); err == nil && folder != nil {
				folderID = strings.TrimSpace(folder.ID)
			}
		}

		if info.IsDir {
			if err := e.fs.MoveDir(ctx, entry.TargetPath, entry.SourcePath); err != nil {
				return fmt.Errorf("move folder back %q to %q: %w", entry.TargetPath, entry.SourcePath, err)
			}
			if e.folders != nil && folderID != "" {
				sourceDir := filepath.Dir(entry.SourcePath)
				relativePath := filepath.Base(entry.SourcePath)
				if input.Folder != nil {
					sourceDir = input.Folder.SourceDir
					relativePath = relativePathFromSourceDir(input.Folder.SourceDir, entry.SourcePath)
					if relativePath == "" || strings.HasPrefix(relativePath, "..") {
						sourceDir = filepath.Dir(entry.SourcePath)
						relativePath = filepath.Base(entry.SourcePath)
					}
				}
				if err := e.folders.UpdatePath(ctx, folderID, entry.SourcePath, sourceDir, relativePath); err != nil {
					return fmt.Errorf("update folder path for %q: %w", folderID, err)
				}
			}
		} else {
			if err := e.fs.MoveFile(ctx, entry.TargetPath, entry.SourcePath); err != nil {
				return fmt.Errorf("move artifact back %q to %q: %w", entry.TargetPath, entry.SourcePath, err)
			}
			targetRoots[normalizeWorkflowPath(filepath.Dir(entry.TargetPath))] = struct{}{}
		}
	}

	for _, root := range phase4MoveSortedPathsDesc(targetRoots) {
		if root == "" {
			continue
		}
		if err := phase4MoveRemoveDirIfEmpty(ctx, e.fs, root); err != nil {
			return err
		}
	}

	return nil
}

func phase4MoveUseLegacyMode(items []ProcessingItem) bool {
	if len(items) == 0 {
		return false
	}
	for _, item := range items {
		if strings.TrimSpace(item.RootPath) != "" || strings.TrimSpace(item.RelativePath) != "" || strings.TrimSpace(item.SourceKind) != "" || strings.TrimSpace(item.OriginalSourcePath) != "" {
			return false
		}
	}
	return true
}

func phase4MoveResolveOutputFallbackTargetDir(items []ProcessingItem, config map[string]any, appConfig *repository.AppConfig) string {
	refType := strings.ToLower(strings.TrimSpace(stringConfig(config, "path_ref_type")))
	if refType != workflowPathRefTypeOutput {
		return ""
	}

	outputKey := strings.TrimSpace(stringConfig(config, "path_ref_key"))
	if outputKey == "" {
		outputKey = "mixed"
	}
	category, index := parseOutputDirRef(outputKey)
	if category != "mixed" || index < 0 {
		return ""
	}

	inferredCategory := phase4MoveInferSingleCategory(items)
	switch inferredCategory {
	case "video", "manga", "photo", "other":
	default:
		return ""
	}

	candidateKey := inferredCategory
	if index > 0 {
		candidateKey = fmt.Sprintf("%s:%d", inferredCategory, index)
	}
	return resolveOutputDirByKey(appConfig, candidateKey)
}

func phase4MoveInferSingleCategory(items []ProcessingItem) string {
	category := ""
	for _, item := range items {
		current := strings.ToLower(strings.TrimSpace(item.Category))
		if current == "" {
			continue
		}
		if category == "" {
			category = current
			continue
		}
		if category != current {
			return ""
		}
	}
	return category
}

func phase4MoveResolveItemTargetDir(item ProcessingItem, config map[string]any, appConfig *repository.AppConfig, fallback string) string {
	refType := strings.ToLower(strings.TrimSpace(stringConfig(config, "path_ref_type")))
	if refType != workflowPathRefTypeOutput {
		return fallback
	}

	outputKey := strings.TrimSpace(stringConfig(config, "path_ref_key"))
	if outputKey == "" {
		outputKey = "mixed"
	}
	configuredCategory, index := parseOutputDirRef(outputKey)
	if configuredCategory != "mixed" || index < 0 {
		return fallback
	}

	itemCategory := strings.ToLower(strings.TrimSpace(item.Category))
	switch itemCategory {
	case "video", "manga", "photo", "other":
	default:
		return fallback
	}

	candidateKey := itemCategory
	if index > 0 {
		candidateKey = fmt.Sprintf("%s:%d", itemCategory, index)
	}
	candidateConfig := make(map[string]any, len(config)+1)
	for key, value := range config {
		candidateConfig[key] = value
	}
	candidateConfig["path_ref_key"] = candidateKey
	if targetDir := resolveWorkflowNodePath(candidateConfig, appConfig, workflowNodePathOptions{
		DefaultType:      workflowPathRefTypeOutput,
		DefaultOutputKey: "mixed",
		LegacyKeys:       []string{"target_dir", "targetDir", "output_dir"},
	}); targetDir != "" {
		return targetDir
	}

	return fallback
}

func (e *phase4MoveNodeExecutor) executeLegacyMove(
	ctx context.Context,
	input NodeExecutionInput,
	items []ProcessingItem,
	targetDir string,
	conflictPolicy string,
) (NodeExecutionOutput, error) {
	movedItems := make([]ProcessingItem, 0, len(items))
	stepResults := make([]ProcessingStepResult, 0, len(items))
	for index, item := range items {
		item = processingItemNormalize(item)
		sourcePath := processingItemCurrentPath(item)
		if sourcePath == "" {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: item current_path is required", e.Type())
		}
		targetName := phase4MoveItemName(item)
		if strings.TrimSpace(targetName) == "" {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: target item name is empty", e.Type())
		}

		destinationPath := joinWorkflowPath(targetDir, targetName)
		finalPath, skipped, err := e.resolveArtifactDestinationPath(ctx, targetDir, targetName, conflictPolicy, "", false)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: resolve destination for %q: %w", e.Type(), sourcePath, err)
		}
		if skipped {
			stepResults = append(stepResults, ProcessingStepResult{
				FolderID:   strings.TrimSpace(item.FolderID),
				SourcePath: sourcePath,
				TargetPath: destinationPath,
				NodeType:   input.Node.Type,
				NodeLabel:  strings.TrimSpace(input.Node.Label),
				Status:     "skipped",
			})
			movedItems = append(movedItems, item)
			continue
		}

		if err := e.fs.MoveDir(ctx, sourcePath, finalPath); err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: move %q to %q: %w", e.Type(), sourcePath, finalPath, err)
		}
		folderID := strings.TrimSpace(item.FolderID)
		if e.folders != nil && folderID != "" {
			sourceDir := filepath.Dir(finalPath)
			relativePath := filepath.Base(finalPath)
			if err := e.folders.UpdatePath(ctx, folderID, finalPath, sourceDir, relativePath); err != nil {
				return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: update folder path for %q: %w", e.Type(), folderID, err)
			}
		}

		item.CurrentPath = normalizeWorkflowPath(finalPath)
		item.ParentPath = normalizeWorkflowPath(filepath.Dir(finalPath))
		item.FolderName = filepath.Base(finalPath)
		item.TargetName = filepath.Base(finalPath)
		movedItems = append(movedItems, item)
		stepResults = append(stepResults, ProcessingStepResult{
			FolderID:   strings.TrimSpace(item.FolderID),
			SourcePath: sourcePath,
			TargetPath: normalizeWorkflowPath(finalPath),
			NodeType:   input.Node.Type,
			NodeLabel:  strings.TrimSpace(input.Node.Label),
			Status:     "moved",
		})

		if input.ProgressFn != nil {
			percent := (index + 1) * 100 / len(items)
			input.ProgressFn(percent, fmt.Sprintf("已完成 %d/%d 项移动", index+1, len(items)))
		}
	}

	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{
			"items":        {Type: PortTypeProcessingItemList, Value: movedItems},
			"step_results": {Type: PortTypeProcessingStepResultList, Value: stepResults},
		},
		Status: ExecutionSuccess,
	}, nil
}

func (e *phase4MoveNodeExecutor) normalizeAndValidateItems(ctx context.Context, items []ProcessingItem) (string, string, []ProcessingItem, error) {
	normalized := make([]ProcessingItem, 0, len(items))
	rootPath := ""
	for _, raw := range items {
		item := processingItemNormalize(raw)
		currentPath := processingItemCurrentPath(item)
		if currentPath == "" {
			return "", "", nil, fmt.Errorf("item current_path is required")
		}
		if item.SourceKind == "" {
			info, err := e.fs.Stat(ctx, currentPath)
			if err != nil {
				return "", "", nil, fmt.Errorf("stat current_path %q: %w", currentPath, err)
			}
			if info.IsDir {
				item.SourceKind = ProcessingItemSourceKindDirectory
			} else {
				item.SourceKind = ProcessingItemSourceKindArchive
			}
		}
		if item.SourceKind != ProcessingItemSourceKindDirectory && item.SourceKind != ProcessingItemSourceKindArchive {
			return "", "", nil, fmt.Errorf("unsupported source_kind %q", item.SourceKind)
		}

		if item.RootPath == "" {
			switch item.SourceKind {
			case ProcessingItemSourceKindDirectory:
				item.RootPath = item.SourcePath
			case ProcessingItemSourceKindArchive:
				if item.OriginalSourcePath != "" {
					item.RootPath = item.OriginalSourcePath
				} else {
					item.RootPath = item.SourcePath
				}
			}
		}
		if item.RootPath == "" {
			return "", "", nil, fmt.Errorf("item root_path is required")
		}
		if item.SourceKind == ProcessingItemSourceKindArchive && item.OriginalSourcePath == "" {
			item.OriginalSourcePath = item.SourcePath
		}
		if item.RelativePath == "" && item.RootPath != "" && item.OriginalSourcePath != "" {
			item.RelativePath = folderSplitterRelativePath(item.RootPath, item.OriginalSourcePath)
		}
		normalized = append(normalized, item)
	}

	var derived bool
	rootPath, normalized, derived = phase4MoveResolveExecutionRoot(normalized)
	if rootPath == "" {
		for _, item := range normalized {
			if rootPath == "" {
				rootPath = item.RootPath
				continue
			}
			if item.RootPath != rootPath {
				return "", "", nil, fmt.Errorf("multiple root_path detected in one execution: %q and %q", rootPath, item.RootPath)
			}
		}
	}
	if derived {
		for index := range normalized {
			originalRootPath := normalized[index].RootPath
			normalized[index].RootPath = rootPath
			relativeRootPath := folderSplitterRelativePath(rootPath, originalRootPath)
			if relativeRootPath != "" {
				normalized[index].RelativePath = relativeRootPath
				continue
			}
			if normalized[index].RelativePath == "" {
				relativeSourcePath := normalized[index].OriginalSourcePath
				if strings.TrimSpace(relativeSourcePath) == "" {
					relativeSourcePath = processingItemCurrentPath(normalized[index])
				}
				normalized[index].RelativePath = folderSplitterRelativePath(rootPath, relativeSourcePath)
			}
		}
	}

	rootName := strings.TrimSpace(filepath.Base(rootPath))
	if rootName == "" || rootName == "." || rootName == string(filepath.Separator) {
		return "", "", nil, fmt.Errorf("invalid root_path %q", rootPath)
	}
	return rootPath, rootName, normalized, nil
}

func phase4MoveResolveExecutionRoot(items []ProcessingItem) (string, []ProcessingItem, bool) {
	if len(items) == 0 {
		return "", items, false
	}

	rootPaths := map[string]struct{}{}
	commonParent := ""
	for _, item := range items {
		rootPath := normalizeWorkflowPath(item.RootPath)
		if rootPath == "" {
			return "", items, false
		}
		rootPaths[rootPath] = struct{}{}

		parent := normalizeWorkflowPath(filepath.Dir(rootPath))
		if parent == "" || parent == "." || parent == rootPath {
			return "", items, false
		}
		if commonParent == "" {
			commonParent = parent
		} else if parent != commonParent {
			return "", items, false
		}

		currentPath := processingItemCurrentPath(item)
		relativePath := strings.TrimSpace(item.RelativePath)
		if relativePath == "" && (currentPath == "" || currentPath != rootPath) && item.SourceKind != ProcessingItemSourceKindArchive {
			return "", items, false
		}
	}

	if commonParent == "" {
		return "", items, false
	}
	if len(rootPaths) <= 1 {
		return "", items, false
	}

	return commonParent, items, true
}

func (e *phase4MoveNodeExecutor) moveDirectoryArtifacts(
	ctx context.Context,
	item ProcessingItem,
	targetRoot string,
	conflictPolicy string,
	input NodeExecutionInput,
) ([]ProcessingStepResult, error) {
	sourcePath := processingItemCurrentPath(item)
	info, err := e.fs.Stat(ctx, sourcePath)
	if err != nil {
		return nil, fmt.Errorf("stat directory item %q: %w", sourcePath, err)
	}
	if !info.IsDir {
		return nil, fmt.Errorf("directory item current_path %q is not directory", sourcePath)
	}

	entries, err := e.fs.ReadDir(ctx, sourcePath)
	if err != nil {
		return nil, fmt.Errorf("read directory item %q: %w", sourcePath, err)
	}

	results := make([]ProcessingStepResult, 0, len(entries))
	for _, entry := range entries {
		sourceFilePath := joinWorkflowPath(sourcePath, entry.Name)
		if entry.IsDir {
			return nil, fmt.Errorf("directory item %q contains subdirectory %q; recursive merge is not supported", sourcePath, sourceFilePath)
		}

		targetName := entry.Name
		conflictPrefix := phase4MoveFlattenPath(item.RelativePath)
		targetPath, skipped, err := e.resolveArtifactDestinationPath(ctx, targetRoot, targetName, conflictPolicy, conflictPrefix, true)
		if err != nil {
			return nil, fmt.Errorf("resolve destination for %q: %w", sourceFilePath, err)
		}
		if skipped {
			results = append(results, ProcessingStepResult{
				FolderID:   strings.TrimSpace(item.FolderID),
				SourcePath: sourceFilePath,
				TargetPath: targetPath,
				NodeType:   input.Node.Type,
				NodeLabel:  strings.TrimSpace(input.Node.Label),
				Status:     "skipped",
			})
			continue
		}
		if err := e.fs.MoveFile(ctx, sourceFilePath, targetPath); err != nil {
			return nil, fmt.Errorf("move file %q to %q: %w", sourceFilePath, targetPath, err)
		}
		results = append(results, ProcessingStepResult{
			FolderID:   strings.TrimSpace(item.FolderID),
			SourcePath: sourceFilePath,
			TargetPath: targetPath,
			NodeType:   input.Node.Type,
			NodeLabel:  strings.TrimSpace(input.Node.Label),
			Status:     "moved",
		})
	}

	return results, nil
}

func (e *phase4MoveNodeExecutor) moveArchiveArtifact(
	ctx context.Context,
	item ProcessingItem,
	targetRoot string,
	conflictPolicy string,
	input NodeExecutionInput,
) (ProcessingStepResult, error) {
	sourcePath := processingItemCurrentPath(item)
	info, err := e.fs.Stat(ctx, sourcePath)
	if err != nil {
		return ProcessingStepResult{}, fmt.Errorf("stat archive item %q: %w", sourcePath, err)
	}
	if info.IsDir {
		return ProcessingStepResult{}, fmt.Errorf("archive item current_path %q is directory", sourcePath)
	}

	targetName := filepath.Base(sourcePath)
	flattened := phase4MoveFlattenPath(item.RelativePath)
	if flattened != "" {
		targetName = flattened + filepath.Ext(sourcePath)
	}

	targetPath, skipped, err := e.resolveArtifactDestinationPath(ctx, targetRoot, targetName, conflictPolicy, "", false)
	if err != nil {
		return ProcessingStepResult{}, fmt.Errorf("resolve destination for archive %q: %w", sourcePath, err)
	}
	if skipped {
		return ProcessingStepResult{
			FolderID:   strings.TrimSpace(item.FolderID),
			SourcePath: sourcePath,
			TargetPath: targetPath,
			NodeType:   input.Node.Type,
			NodeLabel:  strings.TrimSpace(input.Node.Label),
			Status:     "skipped",
		}, nil
	}

	if err := e.fs.MoveFile(ctx, sourcePath, targetPath); err != nil {
		return ProcessingStepResult{}, fmt.Errorf("move archive %q to %q: %w", sourcePath, targetPath, err)
	}
	return ProcessingStepResult{
		FolderID:   strings.TrimSpace(item.FolderID),
		SourcePath: sourcePath,
		TargetPath: targetPath,
		NodeType:   input.Node.Type,
		NodeLabel:  strings.TrimSpace(input.Node.Label),
		Status:     "moved",
	}, nil
}

func (e *phase4MoveNodeExecutor) resolveArtifactDestinationPath(
	ctx context.Context,
	targetRoot string,
	targetName string,
	conflictPolicy string,
	conflictPrefix string,
	allowPrefixFallback bool,
) (string, bool, error) {
	targetName = strings.TrimSpace(targetName)
	if targetName == "" {
		return "", false, fmt.Errorf("target name is empty")
	}

	defaultPath := joinWorkflowPath(targetRoot, targetName)
	exists, err := e.fs.Exists(ctx, defaultPath)
	if err != nil {
		return "", false, err
	}
	if !exists {
		return defaultPath, false, nil
	}

	switch conflictPolicy {
	case "skip":
		return defaultPath, true, nil
	case "overwrite":
		if err := e.fs.Remove(ctx, defaultPath); err != nil {
			return "", false, fmt.Errorf("overwrite remove existing destination %q: %w", defaultPath, err)
		}
		return defaultPath, false, nil
	case "auto_rename":
		if allowPrefixFallback {
			prefix := strings.TrimSpace(conflictPrefix)
			if prefix != "" {
				prefixedName := prefix + "-" + targetName
				prefixedPath := joinWorkflowPath(targetRoot, prefixedName)
				taken, err := e.fs.Exists(ctx, prefixedPath)
				if err != nil {
					return "", false, err
				}
				if !taken {
					return prefixedPath, false, nil
				}
				return e.resolveAutoRenamePath(ctx, targetRoot, prefixedName)
			}
		}
		return e.resolveAutoRenamePath(ctx, targetRoot, targetName)
	default:
		return "", false, fmt.Errorf("unsupported conflict_policy %q", conflictPolicy)
	}
}

func (e *phase4MoveNodeExecutor) resolveAutoRenamePath(ctx context.Context, targetRoot, targetName string) (string, bool, error) {
	ext := filepath.Ext(targetName)
	stem := strings.TrimSuffix(targetName, ext)
	if stem == "" {
		stem = targetName
		ext = ""
	}
	for index := 1; index <= 9999; index++ {
		candidateName := fmt.Sprintf("%s-%d%s", stem, index, ext)
		candidatePath := joinWorkflowPath(targetRoot, candidateName)
		exists, err := e.fs.Exists(ctx, candidatePath)
		if err != nil {
			return "", false, err
		}
		if !exists {
			return candidatePath, false, nil
		}
	}
	return "", false, fmt.Errorf("auto_rename exhausted candidates for %q", targetName)
}

func phase4MoveFilterRollbackEntries(entries []phase4MoveRollbackEntry, folder *repository.Folder) []phase4MoveRollbackEntry {
	if len(entries) == 0 || folder == nil {
		return entries
	}
	folderID := strings.TrimSpace(folder.ID)
	folderPath := strings.TrimSpace(folder.Path)
	if folderID == "" && folderPath == "" {
		return entries
	}

	filtered := make([]phase4MoveRollbackEntry, 0, len(entries))
	for _, entry := range entries {
		if folderID != "" && strings.TrimSpace(entry.FolderID) == folderID {
			filtered = append(filtered, entry)
			continue
		}
		sourcePath := strings.TrimSpace(entry.SourcePath)
		targetPath := strings.TrimSpace(entry.TargetPath)
		if folderPath != "" && (sourcePath == folderPath || targetPath == folderPath || strings.HasPrefix(sourcePath, folderPath+string(filepath.Separator)) || strings.HasPrefix(targetPath, folderPath+string(filepath.Separator))) {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

type phase4MoveRollbackEntry struct {
	FolderID   string
	SourcePath string
	TargetPath string
}

func phase4MoveCollectRollbackEntries(input NodeRollbackInput) ([]phase4MoveRollbackEntry, error) {
	entryByTarget := make(map[string]phase4MoveRollbackEntry)
	collect := func(raw string, source string) error {
		entries, err := phase4MoveRollbackEntriesFromOutput(raw)
		if err != nil {
			return fmt.Errorf("parse %s output: %w", source, err)
		}
		for _, entry := range entries {
			key := strings.TrimSpace(entry.TargetPath)
			if key == "" {
				continue
			}
			entryByTarget[key] = entry
		}
		return nil
	}

	if input.NodeRun != nil && strings.TrimSpace(input.NodeRun.OutputJSON) != "" {
		if err := collect(input.NodeRun.OutputJSON, fmt.Sprintf("node run %q", input.NodeRun.ID)); err != nil {
			return nil, err
		}
	}
	for _, snapshot := range input.Snapshots {
		if snapshot == nil || snapshot.Kind != "post" || strings.TrimSpace(snapshot.OutputJSON) == "" {
			continue
		}
		if err := collect(snapshot.OutputJSON, fmt.Sprintf("snapshot %q", snapshot.ID)); err != nil {
			return nil, err
		}
	}

	entries := make([]phase4MoveRollbackEntry, 0, len(entryByTarget))
	for _, entry := range entryByTarget {
		entries = append(entries, entry)
	}
	return entries, nil
}

func phase4MoveRollbackEntriesFromOutput(raw string) ([]phase4MoveRollbackEntry, error) {
	var wrapped struct {
		Outputs map[string]TypedValueJSON `json:"outputs"`
	}
	if err := json.Unmarshal([]byte(raw), &wrapped); err == nil && len(wrapped.Outputs) > 0 {
		decoded, err := typedValueMapFromJSON(wrapped.Outputs, NewTypeRegistry())
		if err != nil {
			return nil, err
		}
		return phase4MoveRollbackEntriesFromValues(decoded["items"].Value, decoded["step_results"].Value), nil
	}

	if typedOutputs, typed, err := parseTypedNodeOutputs(raw); err != nil {
		return nil, err
	} else if typed {
		return phase4MoveRollbackEntriesFromValues(typedOutputs["items"].Value, typedOutputs["step_results"].Value), nil
	}
	return nil, fmt.Errorf("invalid typed output json")
}

func phase4MoveRollbackEntriesFromValues(itemsValue any, stepResultsValue any) []phase4MoveRollbackEntry {
	items, _ := categoryRouterToItems(itemsValue)
	stepResults := processingStepResultsFromAny(stepResultsValue)
	entries := make([]phase4MoveRollbackEntry, 0, len(stepResults))
	for index, result := range stepResults {
		if strings.TrimSpace(result.NodeType) != phase4MoveNodeExecutorType {
			continue
		}
		if !strings.EqualFold(strings.TrimSpace(result.Status), "moved") {
			continue
		}
		entry := phase4MoveRollbackEntry{
			SourcePath: strings.TrimSpace(result.SourcePath),
			TargetPath: strings.TrimSpace(result.TargetPath),
		}
		entry.FolderID = phase4MoveResolveFolderIDForStep(items, result)
		if entry.FolderID == "" && index < len(items) {
			entry.FolderID = strings.TrimSpace(items[index].FolderID)
		}
		entries = append(entries, entry)
	}
	return entries
}

func phase4MoveResolveFolderIDForStep(items []ProcessingItem, step ProcessingStepResult) string {
	if folderID := strings.TrimSpace(step.FolderID); folderID != "" {
		return folderID
	}
	sourcePath := strings.TrimSpace(step.SourcePath)
	for _, item := range items {
		folderID := strings.TrimSpace(item.FolderID)
		if folderID == "" {
			continue
		}
		candidates := []string{
			normalizeWorkflowPath(item.SourcePath),
			normalizeWorkflowPath(item.CurrentPath),
			normalizeWorkflowPath(item.OriginalSourcePath),
		}
		for _, path := range candidates {
			if path == "" {
				continue
			}
			if sourcePath == path || strings.HasPrefix(sourcePath, path+string(filepath.Separator)) {
				return folderID
			}
		}
	}

	if len(items) == 1 {
		return strings.TrimSpace(items[0].FolderID)
	}
	return ""
}

func phase4MoveRemoveDirIfEmpty(ctx context.Context, adapter fs.FSAdapter, dir string) error {
	entries, err := adapter.ReadDir(ctx, dir)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("check target root %q for cleanup: %w", dir, err)
	}
	if len(entries) > 0 {
		return nil
	}
	if err := adapter.Remove(ctx, dir); err != nil {
		return fmt.Errorf("remove empty target root %q: %w", dir, err)
	}
	return nil
}

func phase4MoveSortedPathsDesc(values map[string]struct{}) []string {
	paths := make([]string, 0, len(values))
	for value := range values {
		paths = append(paths, value)
	}
	sort.Slice(paths, func(i, j int) bool {
		return len(paths[i]) > len(paths[j])
	})
	return paths
}

func phase4MoveFlattenPath(path string) string {
	normalized := normalizeWorkflowPath(path)
	if normalized == "" || normalized == "." {
		return ""
	}
	segments := strings.FieldsFunc(normalized, func(r rune) bool {
		return r == '/' || r == '\\'
	})
	parts := make([]string, 0, len(segments))
	for _, segment := range segments {
		trimmed := strings.TrimSpace(segment)
		if trimmed == "" || trimmed == "." {
			continue
		}
		parts = append(parts, trimmed)
	}
	return strings.Join(parts, "-")
}

func phase4MoveItemName(item ProcessingItem) string {
	if strings.TrimSpace(item.TargetName) != "" {
		return strings.TrimSpace(item.TargetName)
	}
	if strings.TrimSpace(item.FolderName) != "" {
		return strings.TrimSpace(item.FolderName)
	}

	currentPath := processingItemCurrentPath(item)
	if currentPath != "" {
		return strings.TrimSpace(filepath.Base(currentPath))
	}
	return strings.TrimSpace(filepath.Base(strings.TrimSpace(item.SourcePath)))
}
