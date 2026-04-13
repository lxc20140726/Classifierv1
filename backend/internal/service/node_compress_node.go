package service

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

const compressNodeExecutorType = "compress-node"

var defaultCompressNodeIncludePatterns = []string{"*.jpg", "*.jpeg", "*.png", "*.webp", "*.gif", "*.bmp"}

type compressNodeExecutor struct {
	fs fs.FSAdapter
}

func newCompressNodeExecutor(fsAdapter fs.FSAdapter) *compressNodeExecutor {
	return &compressNodeExecutor{fs: fsAdapter}
}

func (e *compressNodeExecutor) Type() string {
	return compressNodeExecutorType
}

func (e *compressNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "压缩节点",
		Description: "将处理项打包为 cbz/zip 压缩文件",
		Inputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, Description: "待压缩的处理项列表", Required: true, SkipOnEmpty: true, AcceptDefault: true},
		},
		Outputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, RequiredOutput: true, Description: "已处理的处理项列表"},
			{Name: "archive_items", Type: PortTypeProcessingItemList, RequiredOutput: true, Description: "压缩产物处理项列表"},
		},
	}
}

func (e *compressNodeExecutor) Execute(ctx context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	items, ok := categoryRouterExtractItems(input.Inputs)
	if !ok || len(items) == 0 {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: items input is required", e.Type())
	}

	scope := strings.ToLower(strings.TrimSpace(stringConfig(input.Node.Config, "scope")))
	if scope == "" {
		scope = "folder"
	}
	if scope != "folder" {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: unsupported scope %q, only folder is supported", e.Type(), scope)
	}

	if folderSplitterBoolConfig(input.Node.Config, "delete_source", false) {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: delete_source=true is not supported", e.Type())
	}

	format := strings.ToLower(strings.TrimSpace(stringConfig(input.Node.Config, "format")))
	if format == "" {
		format = "cbz"
	}
	if format != "cbz" && format != "zip" {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: unsupported format %q", e.Type(), format)
	}
	ext := "." + format

	archiveDir := resolveWorkflowNodePath(input.Node.Config, input.AppConfig, workflowNodePathOptions{
		DefaultType:      workflowPathRefTypeOutput,
		DefaultOutputKey: "mixed",
		LegacyKeys:       []string{"target_dir", "output_dir"},
	})
	if archiveDir == "" {
		archiveDir = ".archives"
	}

	createTarget := folderSplitterBoolConfig(input.Node.Config, "create_target_if_missing", true)
	exists, err := e.fs.Exists(ctx, archiveDir)
	if err != nil {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: check archive dir %q: %w", e.Type(), archiveDir, err)
	}
	if !exists {
		if !createTarget {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: archive dir %q does not exist and create_target_if_missing is false", e.Type(), archiveDir)
		}
		if err := e.fs.MkdirAll(ctx, archiveDir, 0o755); err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: create archive dir %q: %w", e.Type(), archiveDir, err)
		}
	}

	includePatterns := stringSliceConfig(input.Node.Config, "include_patterns", defaultCompressNodeIncludePatterns)
	excludePatterns := stringSliceConfig(input.Node.Config, "exclude_patterns", nil)

	stepResults := make([]ProcessingStepResult, 0, len(items))
	archiveItems := make([]ProcessingItem, 0, len(items))
	archives := make([]string, 0, len(items))
	for _, item := range items {
		item = processingItemNormalize(item)
		sourcePath := processingItemCurrentPath(item)
		if sourcePath == "" {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: item current_path is required", e.Type())
		}

		info, err := e.fs.Stat(ctx, sourcePath)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: stat source path %q: %w", e.Type(), sourcePath, err)
		}
		if !info.IsDir {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: source path %q is not a folder", e.Type(), sourcePath)
		}

		archiveName := processingItemArtifactName(item)
		if archiveName == "" {
			archiveName = info.Name
		}

		archivePath, err := compressNodeResolveArchivePath(ctx, e.fs, archiveDir, archiveName, ext)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: resolve archive name for %q: %w", e.Type(), sourcePath, err)
		}

		if err := e.createArchive(ctx, sourcePath, archivePath, includePatterns, excludePatterns); err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: create archive for %q: %w", e.Type(), sourcePath, err)
		}
		archives = append(archives, archivePath)
		archiveItems = append(archiveItems, compressNodeBuildArchiveItem(item, archivePath))
		stepResults = append(stepResults, ProcessingStepResult{
			FolderID:   strings.TrimSpace(item.FolderID),
			SourcePath: sourcePath,
			TargetPath: normalizeWorkflowPath(archivePath),
			NodeType:   input.Node.Type,
			NodeLabel:  strings.TrimSpace(input.Node.Label),
			Status:     "succeeded",
		})
		if input.ProgressFn != nil {
			percent := len(archives) * 100 / len(items)
			input.ProgressFn(percent, fmt.Sprintf("已完成 %d/%d 项压缩", len(archives), len(items)))
		}
	}

	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{
			"items":         {Type: PortTypeProcessingItemList, Value: items},
			"archive_items": {Type: PortTypeProcessingItemList, Value: archiveItems},
			"step_results":  {Type: PortTypeProcessingStepResultList, Value: stepResults},
		},
		Status: ExecutionSuccess,
	}, nil
}

func compressNodeBuildArchiveItem(item ProcessingItem, archivePath string) ProcessingItem {
	derived := processingItemNormalize(item)
	derived.CurrentPath = normalizeWorkflowPath(archivePath)
	derived.ParentPath = filepath.Dir(archivePath)
	derived.FolderName = filepath.Base(archivePath)
	derived.TargetName = filepath.Base(archivePath)
	derived.Files = nil
	derived.SourceKind = ProcessingItemSourceKindArchive
	if strings.TrimSpace(derived.OriginalSourcePath) == "" {
		derived.OriginalSourcePath = normalizeWorkflowPath(item.SourcePath)
	}
	return derived
}

func (e *compressNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *compressNodeExecutor) Rollback(ctx context.Context, input NodeRollbackInput) error {
	deleteSource, err := compressNodeDeleteSourceEnabled(input.NodeRun)
	if err != nil {
		return fmt.Errorf("%s.Rollback: %w", e.Type(), err)
	}
	if deleteSource {
		return fmt.Errorf("%s.Rollback: delete_source=true rollback is not supported", e.Type())
	}

	entries, err := compressNodeCollectRollbackEntries(input)
	if err != nil {
		return fmt.Errorf("%s.Rollback: %w", e.Type(), err)
	}
	entries = compressNodeFilterRollbackEntries(entries, input.Folder)

	for _, entry := range entries {
		archivePath := strings.TrimSpace(entry.ArchivePath)
		if archivePath == "" {
			continue
		}
		exists, err := e.fs.Exists(ctx, archivePath)
		if err != nil {
			return fmt.Errorf("%s.Rollback: check archive path %q: %w", e.Type(), archivePath, err)
		}
		if !exists {
			continue
		}
		if err := e.fs.Remove(ctx, archivePath); err != nil {
			return fmt.Errorf("%s.Rollback: remove archive path %q: %w", e.Type(), archivePath, err)
		}
	}

	return nil
}

func compressNodeDeleteSourceEnabled(nodeRun *repository.NodeRun) (bool, error) {
	if nodeRun == nil || strings.TrimSpace(nodeRun.InputJSON) == "" {
		return false, nil
	}

	var payload struct {
		Node struct {
			Config map[string]any `json:"config"`
		} `json:"node"`
	}
	if err := json.Unmarshal([]byte(nodeRun.InputJSON), &payload); err != nil {
		return false, fmt.Errorf("parse node input json for node run %q: %w", nodeRun.ID, err)
	}

	return folderSplitterBoolConfig(payload.Node.Config, "delete_source", false), nil
}

type compressRollbackEntry struct {
	FolderID    string
	SourcePath  string
	ArchivePath string
}

func compressNodeCollectRollbackEntries(input NodeRollbackInput) ([]compressRollbackEntry, error) {
	pathSet := map[string]compressRollbackEntry{}

	if input.NodeRun != nil && strings.TrimSpace(input.NodeRun.OutputJSON) != "" {
		typedOutputs, typed, err := parseTypedNodeOutputs(input.NodeRun.OutputJSON)
		if err != nil {
			return nil, fmt.Errorf("parse node output json for node run %q: %w", input.NodeRun.ID, err)
		}
		var entries []compressRollbackEntry
		if typed {
			entries = compressNodeRollbackEntriesFromValues(typedOutputs["items"].Value, typedOutputs["step_results"].Value)
		} else {
			return nil, fmt.Errorf("parse node output json for node run %q: typed outputs required", input.NodeRun.ID)
		}
		for _, entry := range entries {
			pathSet[strings.TrimSpace(entry.ArchivePath)] = entry
		}
	}

	for _, snapshot := range input.Snapshots {
		if snapshot == nil || snapshot.Kind != "post" || strings.TrimSpace(snapshot.OutputJSON) == "" {
			continue
		}
		typedOutputs, typed, err := parseTypedNodeOutputs(snapshot.OutputJSON)
		if err != nil {
			return nil, fmt.Errorf("parse node snapshot output json for snapshot %q: %w", snapshot.ID, err)
		}
		var entries []compressRollbackEntry
		if typed {
			entries = compressNodeRollbackEntriesFromValues(typedOutputs["items"].Value, typedOutputs["step_results"].Value)
		} else {
			return nil, fmt.Errorf("parse node snapshot output json for snapshot %q: typed outputs required", snapshot.ID)
		}
		for _, entry := range entries {
			pathSet[strings.TrimSpace(entry.ArchivePath)] = entry
		}
	}

	entries := make([]compressRollbackEntry, 0, len(pathSet))
	for _, entry := range pathSet {
		entries = append(entries, entry)
	}

	return entries, nil
}

func compressNodeRollbackEntriesFromValues(itemsValue any, stepResultsValue any) []compressRollbackEntry {
	items, _ := categoryRouterToItems(itemsValue)
	stepResults := processingStepResultsFromAny(stepResultsValue)
	entries := make([]compressRollbackEntry, 0, len(stepResults))
	for index, step := range stepResults {
		if strings.TrimSpace(step.NodeType) != compressNodeExecutorType {
			continue
		}
		target := strings.TrimSpace(step.TargetPath)
		if target == "" {
			continue
		}
		entry := compressRollbackEntry{
			FolderID:    strings.TrimSpace(step.FolderID),
			SourcePath:  strings.TrimSpace(step.SourcePath),
			ArchivePath: target,
		}
		if entry.FolderID == "" && index < len(items) {
			entry.FolderID = strings.TrimSpace(items[index].FolderID)
		}
		entries = append(entries, entry)
	}
	return entries
}

func compressNodeArchivePathsFromStepResults(raw any) []string {
	stepResults := processingStepResultsFromAny(raw)
	if len(stepResults) == 0 {
		return nil
	}
	paths := make([]string, 0, len(stepResults))
	for _, step := range stepResults {
		if strings.TrimSpace(step.NodeType) != compressNodeExecutorType {
			continue
		}
		if strings.TrimSpace(step.TargetPath) == "" {
			continue
		}
		paths = append(paths, strings.TrimSpace(step.TargetPath))
	}
	return uniqueCompactStringSlice(paths)
}

func compressNodeFilterRollbackEntries(entries []compressRollbackEntry, folder *repository.Folder) []compressRollbackEntry {
	if len(entries) == 0 || folder == nil {
		return entries
	}
	folderID := strings.TrimSpace(folder.ID)
	folderPath := strings.TrimSpace(folder.Path)
	if folderID == "" && folderPath == "" {
		return entries
	}

	filtered := make([]compressRollbackEntry, 0, len(entries))
	for _, entry := range entries {
		if folderID != "" && strings.TrimSpace(entry.FolderID) == folderID {
			filtered = append(filtered, entry)
			continue
		}
		sourcePath := strings.TrimSpace(entry.SourcePath)
		if folderPath != "" && (sourcePath == folderPath || strings.HasPrefix(sourcePath, folderPath+string(filepath.Separator))) {
			filtered = append(filtered, entry)
		}
	}
	return filtered
}

func anyToStringSlice(raw any) []string {
	switch typed := raw.(type) {
	case string:
		return []string{typed}
	case []string:
		return append([]string(nil), typed...)
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			value, ok := item.(string)
			if !ok {
				continue
			}
			out = append(out, value)
		}
		return out
	default:
		return nil
	}
}

func compactStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}

	return out
}

func uniqueCompactStringSlice(values []string) []string {
	if len(values) == 0 {
		return nil
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
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

func (e *compressNodeExecutor) createArchive(ctx context.Context, sourceDir, archivePath string, includePatterns, excludePatterns []string) error {
	fileWriter, err := e.fs.OpenFileWrite(ctx, archivePath, 0o644)
	if err != nil {
		return fmt.Errorf("open archive file %q: %w", archivePath, err)
	}
	defer fileWriter.Close()

	archiveWriter := zip.NewWriter(fileWriter)
	defer archiveWriter.Close()

	if err := e.walkAndArchive(ctx, sourceDir, sourceDir, archiveWriter, includePatterns, excludePatterns); err != nil {
		return err
	}

	return nil
}

func (e *compressNodeExecutor) walkAndArchive(
	ctx context.Context,
	rootDir string,
	currentDir string,
	archiveWriter *zip.Writer,
	includePatterns []string,
	excludePatterns []string,
) error {
	entries, err := e.fs.ReadDir(ctx, currentDir)
	if err != nil {
		return fmt.Errorf("read directory %q: %w", currentDir, err)
	}

	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return err
		}

		fullPath := filepath.Join(currentDir, entry.Name)
		relPath, err := filepath.Rel(rootDir, fullPath)
		if err != nil {
			return fmt.Errorf("build relative path for %q from %q: %w", fullPath, rootDir, err)
		}
		relPath = filepath.ToSlash(relPath)

		if entry.IsDir {
			if compressNodeShouldExclude(relPath, excludePatterns) {
				continue
			}
			if err := e.walkAndArchive(ctx, rootDir, fullPath, archiveWriter, includePatterns, excludePatterns); err != nil {
				return err
			}
			continue
		}

		if !compressNodeShouldInclude(relPath, includePatterns) || compressNodeShouldExclude(relPath, excludePatterns) {
			continue
		}

		reader, err := e.fs.OpenFileRead(ctx, fullPath)
		if err != nil {
			return fmt.Errorf("open source file %q: %w", fullPath, err)
		}

		writer, err := archiveWriter.Create(relPath)
		if err != nil {
			reader.Close()
			return fmt.Errorf("create archive entry %q: %w", relPath, err)
		}

		if _, err := io.Copy(writer, reader); err != nil {
			reader.Close()
			return fmt.Errorf("copy file %q to archive: %w", fullPath, err)
		}
		if err := reader.Close(); err != nil {
			return fmt.Errorf("close source file %q: %w", fullPath, err)
		}
	}

	return nil
}

func compressNodeResolveArchivePath(ctx context.Context, fsAdapter fs.FSAdapter, targetDir, baseName, extension string) (string, error) {
	trimmedBase := strings.TrimSpace(baseName)
	if trimmedBase == "" {
		return "", fmt.Errorf("archive base name is empty")
	}

	primary := filepath.Join(targetDir, trimmedBase+extension)
	exists, err := fsAdapter.Exists(ctx, primary)
	if err != nil {
		return "", err
	}
	if !exists {
		return primary, nil
	}

	for index := 1; index <= 9999; index++ {
		candidate := filepath.Join(targetDir, fmt.Sprintf("%s (%d)%s", trimmedBase, index, extension))
		taken, err := fsAdapter.Exists(ctx, candidate)
		if err != nil {
			return "", err
		}
		if !taken {
			return candidate, nil
		}
	}

	return "", fmt.Errorf("failed to allocate archive name for %q", primary)
}

func compressNodeShouldInclude(relPath string, includePatterns []string) bool {
	if len(includePatterns) == 0 {
		return true
	}

	name := filepath.Base(relPath)
	for _, pattern := range includePatterns {
		if strings.TrimSpace(pattern) == "" {
			continue
		}
		if matched, _ := filepath.Match(pattern, name); matched {
			return true
		}
		if matched, _ := filepath.Match(pattern, relPath); matched {
			return true
		}
	}

	return false
}

func compressNodeShouldExclude(relPath string, excludePatterns []string) bool {
	if len(excludePatterns) == 0 {
		return false
	}

	name := filepath.Base(relPath)
	for _, pattern := range excludePatterns {
		if strings.TrimSpace(pattern) == "" {
			continue
		}
		if matched, _ := filepath.Match(pattern, name); matched {
			return true
		}
		if matched, _ := filepath.Match(pattern, relPath); matched {
			return true
		}
	}

	return false
}
