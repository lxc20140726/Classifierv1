package service

import (
	"archive/zip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

const compressNodeExecutorType = "compress-node"

var defaultCompressNodeIncludePatterns = []string{"*.jpg", "*.jpeg", "*.png", "*.webp", "*.gif", "*.bmp"}
var compressNodeArchiveDirLocks sync.Map

type compressNodeExecutor struct {
	fs          fs.FSAdapter
	maxParallel int
}

func newCompressNodeExecutor(fsAdapter fs.FSAdapter) *compressNodeExecutor {
	return &compressNodeExecutor{
		fs:          fsAdapter,
		maxParallel: loadCompressMaxParallel(),
	}
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
	if scope == "" || scope == "all" {
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

	includePatterns := stringSliceConfig(input.Node.Config, "include_patterns", defaultCompressNodeIncludePatterns)
	excludePatterns := stringSliceConfig(input.Node.Config, "exclude_patterns", nil)
	createTarget := folderSplitterBoolConfig(input.Node.Config, "create_target_if_missing", true)

	uniqueItems := make([]ProcessingItem, 0, len(items))
	seenSourcePaths := map[string]struct{}{}
	for _, item := range items {
		item = processingItemNormalize(item)
		sourcePath := processingItemCurrentPath(item)
		if sourcePath == "" {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: item current_path is required", e.Type())
		}
		sourceKey := normalizeWorkflowPath(sourcePath)
		if _, exists := seenSourcePaths[sourceKey]; exists {
			continue
		}
		seenSourcePaths[sourceKey] = struct{}{}
		uniqueItems = append(uniqueItems, item)
	}

	type compressTask struct {
		item        ProcessingItem
		sourcePath  string
		archiveName string
		archiveDir  string
		files       []compressArchiveFile
	}

	tasks := make([]compressTask, 0, len(uniqueItems))
	ensuredDirs := map[string]struct{}{}
	for _, item := range uniqueItems {
		sourcePath := processingItemCurrentPath(item)

		info, err := e.fs.Stat(ctx, sourcePath)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: stat source path %q: %w", e.Type(), sourcePath, err)
		}
		if !info.IsDir {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: source path %q is not a folder", e.Type(), sourcePath)
		}

		archiveName := compressNodeArchiveBaseName(item)
		if archiveName == "" {
			archiveName = info.Name
		}

		archiveDir := compressNodeArchiveDirForItem(input.Node.Config, input.AppConfig, item)
		if archiveDir == "" {
			archiveDir = ".archives"
		}
		archiveDir = normalizeWorkflowPath(archiveDir)
		if _, ensured := ensuredDirs[archiveDir]; !ensured {
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
			ensuredDirs[archiveDir] = struct{}{}
		}

		files, err := e.collectArchiveFiles(ctx, sourcePath, sourcePath, includePatterns, excludePatterns)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: collect archive files for %q: %w", e.Type(), sourcePath, err)
		}
		if len(files) == 0 {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: no files matched include_patterns in %q", e.Type(), sourcePath)
		}
		tasks = append(tasks, compressTask{
			item:        item,
			sourcePath:  sourcePath,
			archiveName: archiveName,
			archiveDir:  archiveDir,
			files:       files,
		})
	}

	totalFiles := 0
	for _, task := range tasks {
		totalFiles += len(task.files)
	}
	if totalFiles <= 0 {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: no files matched include_patterns", e.Type())
	}

	if input.ProgressFn != nil {
		input.ProgressFn(NodeProgressUpdate{
			Percent: 0,
			Done:    0,
			Total:   totalFiles,
			Stage:   "discovering",
			Message: fmt.Sprintf("发现 %d 个待打包文件", totalFiles),
		})
	}

	stepResults := make([]ProcessingStepResult, 0, len(uniqueItems))
	archiveItems := make([]ProcessingItem, 0, len(uniqueItems))
	archives := make([]string, 0, len(uniqueItems))
	var mu sync.Mutex
	doneFiles := 0

	workerCount := e.maxParallel
	if workerCount > len(tasks) {
		workerCount = len(tasks)
	}
	if workerCount <= 0 {
		workerCount = 1
	}

	taskCh := make(chan compressTask)
	errCh := make(chan error, 1)
	ctxWithCancel, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	for index := 0; index < workerCount; index++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				if ctxWithCancel.Err() != nil {
					return
				}
				unlock := compressNodeLockArchiveDir(task.archiveDir)
				archivePath, err := compressNodeResolveArchivePath(ctxWithCancel, e.fs, task.archiveDir, task.archiveName, ext)
				if err != nil {
					unlock()
					select {
					case errCh <- fmt.Errorf("%s.Execute: resolve archive name for %q: %w", e.Type(), task.sourcePath, err):
					default:
					}
					cancel()
					return
				}
				err = e.createArchive(ctxWithCancel, task.sourcePath, archivePath, task.files, func(file compressArchiveFile) {
					mu.Lock()
					doneFiles++
					currentDone := doneFiles
					mu.Unlock()
					if input.ProgressFn != nil {
						percent := currentDone * 100 / totalFiles
						input.ProgressFn(NodeProgressUpdate{
							Percent:    percent,
							Done:       currentDone,
							Total:      totalFiles,
							Stage:      "writing",
							Message:    fmt.Sprintf("已打包 %d/%d 个文件", currentDone, totalFiles),
							SourcePath: normalizeWorkflowPath(filepath.Join(task.sourcePath, file.RelPath)),
							TargetPath: normalizeWorkflowPath(archivePath),
						})
					}
				})
				unlock()
				if err != nil {
					select {
					case errCh <- fmt.Errorf("%s.Execute: create archive for %q: %w", e.Type(), task.sourcePath, err):
					default:
					}
					cancel()
					return
				}

				mu.Lock()
				archives = append(archives, archivePath)
				archiveItems = append(archiveItems, compressNodeBuildArchiveItem(task.item, archivePath))
				stepResults = append(stepResults, ProcessingStepResult{
					FolderID:   strings.TrimSpace(task.item.FolderID),
					SourcePath: task.sourcePath,
					TargetPath: normalizeWorkflowPath(archivePath),
					NodeType:   input.Node.Type,
					NodeLabel:  strings.TrimSpace(input.Node.Label),
					Status:     "succeeded",
				})
				mu.Unlock()
			}
		}()
	}

	go func() {
		defer close(taskCh)
		for _, task := range tasks {
			if ctxWithCancel.Err() != nil {
				return
			}
			taskCh <- task
		}
	}()

	wg.Wait()
	select {
	case err := <-errCh:
		return NodeExecutionOutput{}, err
	default:
	}

	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{
			"items":         {Type: PortTypeProcessingItemList, Value: uniqueItems},
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

type compressArchiveFile struct {
	FullPath string
	RelPath  string
}

func (e *compressNodeExecutor) createArchive(
	ctx context.Context,
	sourceDir string,
	archivePath string,
	files []compressArchiveFile,
	onFileWritten func(file compressArchiveFile),
) error {
	_ = sourceDir
	fileWriter, err := e.fs.OpenFileWrite(ctx, archivePath, 0o644)
	if err != nil {
		return fmt.Errorf("open archive file %q: %w", archivePath, err)
	}

	archiveWriter := zip.NewWriter(fileWriter)
	success := false
	defer func() {
		if !success {
			_ = archiveWriter.Close()
			_ = fileWriter.Close()
			_ = e.fs.Remove(ctx, archivePath)
		}
	}()

	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		reader, err := e.fs.OpenFileRead(ctx, file.FullPath)
		if err != nil {
			return fmt.Errorf("open source file %q: %w", file.FullPath, err)
		}
		header := &zip.FileHeader{
			Name:   file.RelPath,
			Method: zip.Store,
		}
		writer, err := archiveWriter.CreateHeader(header)
		if err != nil {
			_ = reader.Close()
			return fmt.Errorf("create archive entry %q: %w", file.RelPath, err)
		}
		if _, err := io.Copy(writer, reader); err != nil {
			_ = reader.Close()
			return fmt.Errorf("copy file %q to archive: %w", file.FullPath, err)
		}
		if err := reader.Close(); err != nil {
			return fmt.Errorf("close source file %q: %w", file.FullPath, err)
		}
		if onFileWritten != nil {
			onFileWritten(file)
		}
	}
	if err := archiveWriter.Close(); err != nil {
		return fmt.Errorf("close archive writer %q: %w", archivePath, err)
	}
	if err := fileWriter.Close(); err != nil {
		return fmt.Errorf("close archive file %q: %w", archivePath, err)
	}

	success = true
	return nil
}

func (e *compressNodeExecutor) collectArchiveFiles(
	ctx context.Context,
	rootDir string,
	currentDir string,
	includePatterns []string,
	excludePatterns []string,
) ([]compressArchiveFile, error) {
	entries, err := e.fs.ReadDir(ctx, currentDir)
	if err != nil {
		return nil, fmt.Errorf("read directory %q: %w", currentDir, err)
	}

	files := make([]compressArchiveFile, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		fullPath := filepath.Join(currentDir, entry.Name)
		relPath, err := filepath.Rel(rootDir, fullPath)
		if err != nil {
			return nil, fmt.Errorf("build relative path for %q from %q: %w", fullPath, rootDir, err)
		}
		relPath = filepath.ToSlash(relPath)

		if entry.IsDir {
			if compressNodeShouldExclude(relPath, excludePatterns) {
				continue
			}
			childFiles, err := e.collectArchiveFiles(ctx, rootDir, fullPath, includePatterns, excludePatterns)
			if err != nil {
				return nil, err
			}
			files = append(files, childFiles...)
			continue
		}

		if !compressNodeShouldInclude(relPath, includePatterns) || compressNodeShouldExclude(relPath, excludePatterns) {
			continue
		}
		files = append(files, compressArchiveFile{
			FullPath: fullPath,
			RelPath:  relPath,
		})
	}

	return files, nil
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

func compressNodeArchiveBaseName(item ProcessingItem) string {
	normalized := processingItemNormalize(item)
	if relative := compressNodeRelativePathBase(normalized.RelativePath); relative != "" {
		return relative
	}

	if preferred := strings.TrimSpace(phase4MoveItemName(normalized)); preferred != "" && !compressNodeIsGenericArchiveBase(preferred) {
		return preferred
	}

	for _, candidate := range []string{
		normalizeWorkflowPath(normalized.CurrentPath),
		normalizeWorkflowPath(normalized.SourcePath),
	} {
		base := compressNodeFileBaseName(candidate)
		if base != "" && !compressNodeIsGenericArchiveBase(base) {
			return base
		}
	}

	return strings.TrimSpace(processingItemArtifactName(normalized))
}

func compressNodeRelativePathBase(relativePath string) string {
	trimmed := normalizeWorkflowPath(relativePath)
	if trimmed == "" || trimmed == "." || trimmed == "/" {
		return ""
	}
	base := compressNodeFileBaseName(trimmed)
	if compressNodeIsGenericArchiveBase(base) {
		return ""
	}
	return base
}

func compressNodeFileBaseName(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}
	base := strings.TrimSpace(filepath.Base(trimmed))
	if base == "" || base == "." || base == string(filepath.Separator) {
		return ""
	}
	return base
}

func compressNodeIsGenericArchiveBase(name string) bool {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "photo", "manga", "video", "other", "mixed", "mixed_leaf", "unsupported", "__photo", "__manga", "__video", "__unsupported":
		return true
	default:
		return false
	}
}

func compressNodeArchiveDirForItem(config map[string]any, appConfig *repository.AppConfig, item ProcessingItem) string {
	baseDir := resolveWorkflowNodePath(config, appConfig, workflowNodePathOptions{
		DefaultType:      workflowPathRefTypeOutput,
		DefaultOutputKey: "mixed",
		LegacyKeys:       []string{"target_dir", "output_dir"},
	})
	if baseDir == "" {
		baseDir = ".archives"
	}

	refType := strings.ToLower(strings.TrimSpace(stringConfig(config, "path_ref_type")))
	refKey := strings.TrimSpace(stringConfig(config, "path_ref_key"))
	legacyRefType, legacyRefKey := legacyPathRefFromConfig(config)
	if refType == "" {
		refType = legacyRefType
	}
	if refKey == "" {
		refKey = legacyRefKey
	}
	if refType != workflowPathRefTypeOutput {
		return normalizeWorkflowPath(baseDir)
	}

	category, index := parseOutputDirRef(refKey)
	if category != "mixed" || index < 0 {
		return normalizeWorkflowPath(baseDir)
	}

	itemCategory := strings.ToLower(strings.TrimSpace(item.Category))
	switch itemCategory {
	case "photo", "manga", "video", "other":
	default:
		return normalizeWorkflowPath(baseDir)
	}

	preferredRefKey := itemCategory + ":" + strconv.Itoa(index)
	preferredDir := resolveOutputDirByKey(appConfig, preferredRefKey)
	if preferredDir == "" {
		return normalizeWorkflowPath(baseDir)
	}
	return normalizeWorkflowPath(preferredDir)
}

func compressNodeLockArchiveDir(archiveDir string) func() {
	key := normalizeWorkflowPath(archiveDir)
	if key == "" {
		key = "."
	}
	lockEntry, _ := compressNodeArchiveDirLocks.LoadOrStore(key, &sync.Mutex{})
	lock := lockEntry.(*sync.Mutex)
	lock.Lock()
	return lock.Unlock
}

func compressNodeShouldInclude(relPath string, includePatterns []string) bool {
	if len(includePatterns) == 0 {
		return true
	}

	name := filepath.Base(relPath)
	for _, pattern := range includePatterns {
		if compressNodePatternMatches(pattern, name, relPath) {
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
		if compressNodePatternMatches(pattern, name, relPath) {
			return true
		}
	}

	return false
}

func compressNodePatternMatches(pattern, name, relPath string) bool {
	trimmedPattern := strings.TrimSpace(pattern)
	if trimmedPattern == "" {
		return false
	}

	for _, candidate := range []string{name, relPath} {
		if matched, _ := filepath.Match(trimmedPattern, candidate); matched {
			return true
		}
	}

	lowerPattern := strings.ToLower(trimmedPattern)
	if lowerPattern == trimmedPattern {
		for _, candidate := range []string{strings.ToLower(name), strings.ToLower(relPath)} {
			if matched, _ := filepath.Match(lowerPattern, candidate); matched {
				return true
			}
		}
		return false
	}

	for _, candidate := range []string{strings.ToLower(name), strings.ToLower(relPath)} {
		if matched, _ := filepath.Match(lowerPattern, candidate); matched {
			return true
		}
	}

	return false
}
