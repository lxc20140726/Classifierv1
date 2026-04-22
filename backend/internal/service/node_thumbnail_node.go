package service

import (
	"context"
	"fmt"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

const thumbnailNodeExecutorType = "thumbnail-node"

var thumbnailVideoExtensions = map[string]struct{}{
	".mp4":  {},
	".mkv":  {},
	".mov":  {},
	".avi":  {},
	".wmv":  {},
	".flv":  {},
	".webm": {},
	".m4v":  {},
	".ts":   {},
}

type thumbnailNodeExecutor struct {
	fs          fs.FSAdapter
	folders     repository.FolderRepository
	lookPath    func(string) (string, error)
	runFFmpeg   func(ctx context.Context, command string, args ...string) ([]byte, error)
	maxParallel int
	sem         chan struct{}
}

func newThumbnailNodeExecutor(fsAdapter fs.FSAdapter, folderRepo repository.FolderRepository) *thumbnailNodeExecutor {
	maxParallel := loadThumbnailMaxParallel()
	return &thumbnailNodeExecutor{
		fs:          fsAdapter,
		folders:     folderRepo,
		lookPath:    exec.LookPath,
		maxParallel: maxParallel,
		sem:         make(chan struct{}, maxParallel),
		runFFmpeg: func(ctx context.Context, command string, args ...string) ([]byte, error) {
			cmd := exec.CommandContext(ctx, command, args...)
			return cmd.CombinedOutput()
		},
	}
}

func (e *thumbnailNodeExecutor) Type() string {
	return thumbnailNodeExecutorType
}

func (e *thumbnailNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "缩略图节点",
		Description: "为视频文件夹提取代表帧生成缩略图（依赖运行环境中的 ffmpeg）",
		Inputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, Description: "待生成缩略图的处理项列表", Required: true, SkipOnEmpty: true, AcceptDefault: true},
		},
		Outputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, Description: "已处理的处理项列表"},
		},
	}
}

func (e *thumbnailNodeExecutor) Execute(ctx context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	items, ok := categoryRouterExtractItems(input.Inputs)
	if !ok || len(items) == 0 {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: items input is required", e.Type())
	}

	ffmpegPath, err := e.lookPath("ffmpeg")
	if err != nil {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: ffmpeg binary not found: %w", e.Type(), err)
	}

	configuredOutputDir := resolveWorkflowNodePath(input.Node.Config, input.AppConfig, workflowNodePathOptions{
		LegacyKeys: []string{"output_dir", "target_dir"},
	})
	createTarget := folderSplitterBoolConfig(input.Node.Config, "create_target_if_missing", true)

	offsetSeconds := intConfig(input.Node.Config, "offset_seconds", 8)
	if offsetSeconds < 0 {
		offsetSeconds = 0
	}
	width := intConfig(input.Node.Config, "width", 640)

	type thumbnailTask struct {
		item             ProcessingItem
		videoSource      thumbnailVideoSource
		thumbnailPath    string
		isRepresentative bool
	}

	tasks := make([]thumbnailTask, 0, len(items))
	ensuredDirs := map[string]struct{}{}
	seenVideoPaths := map[string]struct{}{}
	for _, item := range items {
		item = processingItemNormalize(item)
		currentPath := processingItemCurrentPath(item)
		if currentPath == "" {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: item current_path is required", e.Type())
		}

		videoSources, representativeVideoPath, err := e.videoSources(ctx, item)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: %w", e.Type(), err)
		}
		outputDir := configuredOutputDir
		if outputDir == "" {
			outputDir = normalizeWorkflowPath(thumbnailNodeDefaultOutputDir(item))
		}
		if outputDir == "" {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: cannot resolve output dir for %q", e.Type(), currentPath)
		}
		if _, ok := ensuredDirs[outputDir]; !ok {
			outExists, err := e.fs.Exists(ctx, outputDir)
			if err != nil {
				return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: check output dir %q: %w", e.Type(), outputDir, err)
			}
			if !outExists {
				if !createTarget {
					return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: output dir %q does not exist and create_target_if_missing is false", e.Type(), outputDir)
				}
				if err := e.fs.MkdirAll(ctx, outputDir, 0o755); err != nil {
					return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: create output dir %q: %w", e.Type(), outputDir, err)
				}
			}
			ensuredDirs[outputDir] = struct{}{}
		}
		outputNames := thumbnailNodeBuildOutputNames(videoSources)
		for _, videoSource := range videoSources {
			normalizedVideoPath := normalizeWorkflowPath(videoSource.Path)
			if _, exists := seenVideoPaths[normalizedVideoPath]; exists {
				continue
			}
			seenVideoPaths[normalizedVideoPath] = struct{}{}
			outputName := strings.TrimSpace(outputNames[videoSource.Path])
			if outputName == "" {
				outputName = strings.TrimSuffix(filepath.Base(videoSource.Path), filepath.Ext(videoSource.Path))
			}
			thumbnailPath := joinWorkflowPath(outputDir, outputName+".jpg")
			tasks = append(tasks, thumbnailTask{
				item:             item,
				videoSource:      videoSource,
				thumbnailPath:    thumbnailPath,
				isRepresentative: normalizeWorkflowPath(videoSource.Path) == normalizeWorkflowPath(representativeVideoPath),
			})
		}
	}
	if len(tasks) == 0 {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: no video files matched current items", e.Type())
	}

	stepResults := make([]ProcessingStepResult, 0, len(tasks))
	coverByFolderID := map[string]string{}
	var stepMu sync.Mutex
	done := 0
	failed := 0
	total := len(tasks)

	for _, task := range tasks {
		if err := ctx.Err(); err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: %w", e.Type(), err)
		}
		if err := e.acquireFFmpegSlot(ctx); err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: acquire ffmpeg slot: %w", e.Type(), err)
		}

		args := thumbnailNodeBuildArgs(task.videoSource.Path, task.thumbnailPath, offsetSeconds, width)
		combined, err := e.runFFmpeg(ctx, ffmpegPath, args...)
		e.releaseFFmpegSlot()
		done++
		if err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: %w", e.Type(), ctxErr)
			}
			failed++
			stepMu.Lock()
			stepResults = append(stepResults, ProcessingStepResult{
				FolderID:   strings.TrimSpace(task.item.FolderID),
				SourcePath: normalizeWorkflowPath(task.videoSource.Path),
				TargetPath: normalizeWorkflowPath(task.thumbnailPath),
				NodeType:   input.Node.Type,
				NodeLabel:  strings.TrimSpace(input.Node.Label),
				Status:     "failed",
				Error:      thumbnailNodeFormatFFmpegError(err, combined),
			})
			stepMu.Unlock()
			if input.ProgressFn != nil {
				input.ProgressFn(thumbnailNodeProgressUpdate(done, total, failed, task.videoSource.Path, task.thumbnailPath, false))
			}
			continue
		}

		stepMu.Lock()
		stepResults = append(stepResults, ProcessingStepResult{
			FolderID:   strings.TrimSpace(task.item.FolderID),
			SourcePath: normalizeWorkflowPath(task.videoSource.Path),
			TargetPath: normalizeWorkflowPath(task.thumbnailPath),
			NodeType:   input.Node.Type,
			NodeLabel:  strings.TrimSpace(input.Node.Label),
			Status:     "succeeded",
		})
		if task.isRepresentative && strings.TrimSpace(task.item.FolderID) != "" {
			coverByFolderID[strings.TrimSpace(task.item.FolderID)] = normalizeWorkflowPath(task.thumbnailPath)
		}
		stepMu.Unlock()

		if input.ProgressFn != nil {
			input.ProgressFn(thumbnailNodeProgressUpdate(done, total, failed, task.videoSource.Path, task.thumbnailPath, true))
		}
	}
	if e.folders != nil {
		for folderID, coverThumbnailPath := range coverByFolderID {
			if err := e.folders.UpdateCoverImagePath(ctx, folderID, coverThumbnailPath); err != nil {
				return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: update cover image path for %q: %w", e.Type(), folderID, err)
			}
		}
	}

	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{
			"items":        {Type: PortTypeProcessingItemList, Value: items},
			"step_results": {Type: PortTypeProcessingStepResultList, Value: stepResults},
		},
		Status: ExecutionSuccess,
	}, nil
}

func (e *thumbnailNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *thumbnailNodeExecutor) Rollback(ctx context.Context, input NodeRollbackInput) error {
	entries, err := thumbnailNodeCollectRollbackEntries(input)
	if err != nil {
		return fmt.Errorf("%s.Rollback: %w", e.Type(), err)
	}
	entries = thumbnailNodeFilterRollbackEntries(entries, input.Folder)
	thumbnailPaths := make([]string, 0, len(entries))
	folderIDSet := map[string]struct{}{}
	for _, entry := range entries {
		if strings.TrimSpace(entry.Path) == "" {
			continue
		}
		thumbnailPaths = append(thumbnailPaths, strings.TrimSpace(entry.Path))
		if strings.TrimSpace(entry.FolderID) != "" {
			folderIDSet[strings.TrimSpace(entry.FolderID)] = struct{}{}
		}
	}
	folderIDs := make([]string, 0, len(folderIDSet))
	for folderID := range folderIDSet {
		folderIDs = append(folderIDs, folderID)
	}

	for _, thumbnailPath := range thumbnailPaths {
		exists, err := e.fs.Exists(ctx, thumbnailPath)
		if err != nil {
			return fmt.Errorf("%s.Rollback: check thumbnail path %q: %w", e.Type(), thumbnailPath, err)
		}
		if !exists {
			continue
		}
		if err := e.fs.Remove(ctx, thumbnailPath); err != nil {
			return fmt.Errorf("%s.Rollback: remove thumbnail path %q: %w", e.Type(), thumbnailPath, err)
		}
	}

	if err := e.clearCoverImagePathIfNeeded(ctx, folderIDs, thumbnailPaths); err != nil {
		return fmt.Errorf("%s.Rollback: %w", e.Type(), err)
	}

	return nil
}

func thumbnailNodeFormatFFmpegError(err error, combined []byte) string {
	if err == nil {
		return ""
	}
	message := strings.TrimSpace(err.Error())
	output := strings.TrimSpace(string(combined))
	if output == "" {
		return message
	}
	if message == "" {
		return output
	}
	return message + ": " + output
}

func thumbnailNodeProgressUpdate(done, total, failed int, videoPath, thumbnailPath string, succeeded bool) NodeProgressUpdate {
	percent := done * 100 / total
	message := fmt.Sprintf("\u5df2\u751f\u6210 %d/%d \u5f20\u7f29\u7565\u56fe", done-failed, total)
	if failed > 0 {
		message = fmt.Sprintf("\u5df2\u5904\u7406 %d/%d \u4e2a\u7f29\u7565\u56fe\u4efb\u52a1\uff0c%d \u4e2a\u5931\u8d25", done, total, failed)
	}
	stage := "generating_thumbnails"
	if !succeeded {
		stage = "thumbnail_failed"
	}

	return NodeProgressUpdate{
		Percent:    percent,
		Done:       done,
		Total:      total,
		Stage:      stage,
		Message:    message,
		SourcePath: normalizeWorkflowPath(videoPath),
		TargetPath: normalizeWorkflowPath(thumbnailPath),
	}
}

type thumbnailRollbackEntry struct {
	FolderID string
	Path     string
}

func thumbnailNodeFilterRollbackEntries(entries []thumbnailRollbackEntry, folder *repository.Folder) []thumbnailRollbackEntry {
	if len(entries) == 0 || folder == nil {
		return entries
	}
	folderID := strings.TrimSpace(folder.ID)
	folderPath := strings.TrimSpace(folder.Path)
	if folderID == "" && folderPath == "" {
		return entries
	}

	filtered := make([]thumbnailRollbackEntry, 0, len(entries))
	for _, entry := range entries {
		if folderID != "" && strings.TrimSpace(entry.FolderID) == folderID {
			filtered = append(filtered, entry)
			continue
		}
		if folderPath != "" && strings.Contains(strings.TrimSpace(entry.Path), strings.TrimSpace(filepath.Base(folderPath))) {
			filtered = append(filtered, entry)
			continue
		}
	}
	return filtered
}

func thumbnailNodeCollectRollbackEntries(input NodeRollbackInput) ([]thumbnailRollbackEntry, error) {
	entrySet := map[string]thumbnailRollbackEntry{}
	collect := func(outputJSON string, source string) error {
		typedOutputs, typed, err := parseTypedNodeOutputs(outputJSON)
		if err != nil {
			return fmt.Errorf("parse %s output json: %w", source, err)
		}
		if !typed {
			return fmt.Errorf("parse %s output json: typed outputs required", source)
		}
		entries := thumbnailNodeRollbackEntriesFromValues(typedOutputs["items"].Value, typedOutputs["step_results"].Value)
		for _, entry := range entries {
			if strings.TrimSpace(entry.Path) == "" {
				continue
			}
			key := strings.TrimSpace(entry.Path) + "|" + strings.TrimSpace(entry.FolderID)
			entrySet[key] = entry
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

	entries := make([]thumbnailRollbackEntry, 0, len(entrySet))
	for _, entry := range entrySet {
		entries = append(entries, entry)
	}
	return entries, nil
}

func thumbnailNodeRollbackEntriesFromValues(itemsValue any, stepResultsValue any) []thumbnailRollbackEntry {
	items, _ := categoryRouterToItems(itemsValue)
	stepResults := processingStepResultsFromAny(stepResultsValue)
	entries := make([]thumbnailRollbackEntry, 0, len(stepResults))
	for _, step := range stepResults {
		if strings.TrimSpace(step.NodeType) != thumbnailNodeExecutorType {
			continue
		}
		if !isStepSucceeded(step.Status) {
			continue
		}
		path := strings.TrimSpace(step.TargetPath)
		if path == "" {
			continue
		}
		entry := thumbnailRollbackEntry{
			FolderID: thumbnailNodeResolveFolderIDForStep(items, step),
			Path:     path,
		}
		entries = append(entries, entry)
	}
	return entries
}

func thumbnailNodeCollectRollbackData(input NodeRollbackInput) ([]string, []string, error) {
	pathSet := map[string]struct{}{}
	folderIDSet := map[string]struct{}{}

	if input.NodeRun != nil && strings.TrimSpace(input.NodeRun.OutputJSON) != "" {
		typedOutputs, typed, err := parseTypedNodeOutputs(input.NodeRun.OutputJSON)
		if err != nil {
			return nil, nil, fmt.Errorf("parse node output json for node run %q: %w", input.NodeRun.ID, err)
		}
		var paths []string
		var folderIDs []string
		if typed {
			paths = thumbnailNodePathsFromStepResults(typedOutputs["step_results"].Value)
			folderIDs = thumbnailNodeExtractFolderIDs(typedOutputs["items"].Value)
		} else {
			return nil, nil, fmt.Errorf("parse node output json for node run %q: typed outputs required", input.NodeRun.ID)
		}
		for _, path := range paths {
			pathSet[path] = struct{}{}
		}
		for _, folderID := range folderIDs {
			folderIDSet[folderID] = struct{}{}
		}
	}

	for _, snapshot := range input.Snapshots {
		if snapshot == nil || snapshot.Kind != "post" || strings.TrimSpace(snapshot.OutputJSON) == "" {
			continue
		}
		typedOutputs, typed, err := parseTypedNodeOutputs(snapshot.OutputJSON)
		if err != nil {
			return nil, nil, fmt.Errorf("parse node snapshot output json for snapshot %q: %w", snapshot.ID, err)
		}
		var paths []string
		var folderIDs []string
		if typed {
			paths = thumbnailNodePathsFromStepResults(typedOutputs["step_results"].Value)
			folderIDs = thumbnailNodeExtractFolderIDs(typedOutputs["items"].Value)
		} else {
			return nil, nil, fmt.Errorf("parse node snapshot output json for snapshot %q: typed outputs required", snapshot.ID)
		}
		for _, path := range paths {
			pathSet[path] = struct{}{}
		}
		for _, folderID := range folderIDs {
			folderIDSet[folderID] = struct{}{}
		}
	}

	thumbnailPaths := make([]string, 0, len(pathSet))
	for path := range pathSet {
		thumbnailPaths = append(thumbnailPaths, path)
	}

	folderIDs := make([]string, 0, len(folderIDSet))
	for folderID := range folderIDSet {
		folderIDs = append(folderIDs, folderID)
	}

	return thumbnailPaths, folderIDs, nil
}

func thumbnailNodePathsFromStepResults(raw any) []string {
	stepResults := processingStepResultsFromAny(raw)
	if len(stepResults) == 0 {
		return nil
	}
	paths := make([]string, 0, len(stepResults))
	for _, step := range stepResults {
		if strings.TrimSpace(step.NodeType) != thumbnailNodeExecutorType {
			continue
		}
		if !isStepSucceeded(step.Status) {
			continue
		}
		path := strings.TrimSpace(step.TargetPath)
		if path == "" {
			continue
		}
		paths = append(paths, path)
	}
	return uniqueCompactStringSlice(paths)
}

func thumbnailNodeExtractFolderIDs(raw any) []string {
	switch typed := raw.(type) {
	case []ProcessingItem:
		out := make([]string, 0, len(typed))
		seen := map[string]struct{}{}
		for _, item := range typed {
			folderID := strings.TrimSpace(item.FolderID)
			if folderID == "" {
				continue
			}
			if _, ok := seen[folderID]; ok {
				continue
			}
			seen[folderID] = struct{}{}
			out = append(out, folderID)
		}
		return out
	case ProcessingItem:
		folderID := strings.TrimSpace(typed.FolderID)
		if folderID == "" {
			return nil
		}
		return []string{folderID}
	}

	items := thumbnailNodeAsMapSlice(raw)
	if len(items) == 0 {
		return nil
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(items))
	for _, item := range items {
		folderID := strings.TrimSpace(anyString(item["folder_id"]))
		if folderID == "" {
			continue
		}
		if _, ok := seen[folderID]; ok {
			continue
		}
		seen[folderID] = struct{}{}
		out = append(out, folderID)
	}

	return out
}

func thumbnailNodeAsMapSlice(raw any) []map[string]any {
	switch typed := raw.(type) {
	case map[string]any:
		return []map[string]any{typed}
	case []any:
		out := make([]map[string]any, 0, len(typed))
		for _, item := range typed {
			itemMap, ok := item.(map[string]any)
			if !ok {
				continue
			}
			out = append(out, itemMap)
		}
		return out
	default:
		return nil
	}
}

func (e *thumbnailNodeExecutor) clearCoverImagePathIfNeeded(ctx context.Context, folderIDs, thumbnailPaths []string) error {
	if e.folders == nil || len(thumbnailPaths) == 0 {
		return nil
	}

	uniqueFolderIDs := uniqueCompactStringSlice(folderIDs)
	if len(uniqueFolderIDs) != 1 {
		return nil
	}

	folder, err := e.folders.GetByID(ctx, uniqueFolderIDs[0])
	if err != nil {
		return nil
	}

	currentCover := strings.TrimSpace(folder.CoverImagePath)
	if currentCover == "" {
		return nil
	}

	for _, thumbnailPath := range thumbnailPaths {
		if currentCover != thumbnailPath {
			continue
		}
		if err := e.folders.UpdateCoverImagePath(ctx, uniqueFolderIDs[0], ""); err != nil {
			return nil
		}
		return nil
	}

	return nil
}

func (e *thumbnailNodeExecutor) representativeVideoPath(ctx context.Context, item ProcessingItem) (string, error) {
	mediaPath := processingItemMediaPath(item)
	if mediaPath == "" {
		return "", fmt.Errorf("item current_path is required")
	}

	info, err := e.fs.Stat(ctx, mediaPath)
	if err != nil {
		return "", fmt.Errorf("stat current path %q: %w", mediaPath, err)
	}
	if !info.IsDir {
		if thumbnailNodeIsVideoFile(filepath.Base(mediaPath)) {
			return mediaPath, nil
		}
		return "", fmt.Errorf("当前处理项不包含可生成缩略图的视频源: %q 不是视频目录或视频文件", mediaPath)
	}

	entries, err := e.fs.ReadDir(ctx, mediaPath)
	if err != nil {
		return "", fmt.Errorf("read source dir %q: %w", mediaPath, err)
	}

	bestName := ""
	bestSize := int64(-1)
	for _, entry := range entries {
		if entry.IsDir {
			continue
		}
		if !thumbnailNodeIsVideoFile(entry.Name) {
			continue
		}
		if entry.Size > bestSize {
			bestSize = entry.Size
			bestName = entry.Name
		}
	}

	if bestName == "" {
		return "", fmt.Errorf("当前处理项不包含可生成缩略图的视频源: %q 中没有可提取的视频文件", mediaPath)
	}

	return joinWorkflowPath(mediaPath, bestName), nil
}

func thumbnailNodeDefaultOutputDir(item ProcessingItem) string {
	currentPath := processingItemCurrentPath(item)
	if currentPath == "" {
		return ""
	}
	sourceKind := strings.ToLower(strings.TrimSpace(item.SourceKind))
	if sourceKind == ProcessingItemSourceKindDirectory {
		return currentPath
	}
	if ext := strings.ToLower(filepath.Ext(currentPath)); ext != "" {
		return normalizeWorkflowPath(filepath.Dir(currentPath))
	}
	return currentPath
}

func thumbnailNodeOutputBaseName(item ProcessingItem) string {
	currentPath := processingItemCurrentPath(item)
	if currentPath == "" {
		return ""
	}
	if item.TargetName != "" {
		return strings.TrimSpace(item.TargetName)
	}
	if item.FolderName != "" {
		return strings.TrimSpace(item.FolderName)
	}
	if strings.ToLower(strings.TrimSpace(item.SourceKind)) == ProcessingItemSourceKindDirectory {
		return filepath.Base(currentPath)
	}
	return strings.TrimSuffix(filepath.Base(currentPath), filepath.Ext(currentPath))
}

type thumbnailVideoSource struct {
	Path string
	Size int64
}

func (e *thumbnailNodeExecutor) videoSources(ctx context.Context, item ProcessingItem) ([]thumbnailVideoSource, string, error) {
	mediaPath := processingItemMediaPath(item)
	if mediaPath == "" {
		return nil, "", fmt.Errorf("item current_path is required")
	}

	info, err := e.fs.Stat(ctx, mediaPath)
	if err != nil {
		return nil, "", fmt.Errorf("stat current path %q: %w", mediaPath, err)
	}
	if !info.IsDir {
		if thumbnailNodeIsVideoFile(filepath.Base(mediaPath)) {
			normalized := normalizeWorkflowPath(mediaPath)
			return []thumbnailVideoSource{{Path: normalized, Size: info.Size}}, normalized, nil
		}
		return nil, "", fmt.Errorf("褰撳墠澶勭悊椤逛笉鍖呭惈鍙敓鎴愮缉鐣ゅ浘鐨勮棰戞簮: %q 涓嶆槸瑙嗛鐩綍鎴栬棰戞枃浠?", mediaPath)
	}

	entries, err := e.fs.ReadDir(ctx, mediaPath)
	if err != nil {
		return nil, "", fmt.Errorf("read source dir %q: %w", mediaPath, err)
	}

	videoSources := make([]thumbnailVideoSource, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir {
			continue
		}
		if !thumbnailNodeIsVideoFile(entry.Name) {
			continue
		}
		videoSources = append(videoSources, thumbnailVideoSource{
			Path: joinWorkflowPath(mediaPath, entry.Name),
			Size: entry.Size,
		})
	}

	if len(videoSources) == 0 {
		return nil, "", fmt.Errorf("褰撳墠澶勭悊椤逛笉鍖呭惈鍙敓鎴愮缉鐣ゅ浘鐨勮棰戞簮: %q 涓病鏈夊彲鎻愬彇鐨勮棰戞枃浠?", mediaPath)
	}

	sort.Slice(videoSources, func(i, j int) bool {
		return strings.ToLower(filepath.Base(videoSources[i].Path)) < strings.ToLower(filepath.Base(videoSources[j].Path))
	})

	representative := videoSources[0]
	for _, source := range videoSources[1:] {
		if source.Size > representative.Size {
			representative = source
		}
	}

	return videoSources, representative.Path, nil
}

func thumbnailNodeBuildOutputNames(videoSources []thumbnailVideoSource) map[string]string {
	names := make(map[string]string, len(videoSources))
	stemCounts := map[string]int{}
	for _, source := range videoSources {
		stem := strings.TrimSuffix(filepath.Base(source.Path), filepath.Ext(source.Path))
		stemCounts[strings.ToLower(stem)]++
	}

	for _, source := range videoSources {
		base := filepath.Base(source.Path)
		stem := strings.TrimSuffix(base, filepath.Ext(base))
		name := stem
		if stemCounts[strings.ToLower(stem)] > 1 {
			name = base
		}
		names[source.Path] = strings.TrimSpace(name)
	}

	return names
}

func thumbnailNodeResolveFolderIDForStep(items []ProcessingItem, step ProcessingStepResult) string {
	if folderID := strings.TrimSpace(step.FolderID); folderID != "" {
		return folderID
	}
	if len(items) == 0 {
		return ""
	}

	sourcePath := normalizeWorkflowPath(strings.TrimSpace(step.SourcePath))
	targetPath := normalizeWorkflowPath(strings.TrimSpace(step.TargetPath))
	for _, item := range items {
		folderID := strings.TrimSpace(item.FolderID)
		if folderID == "" {
			continue
		}
		currentPath := normalizeWorkflowPath(processingItemCurrentPath(item))
		if currentPath == "" {
			continue
		}
		if sourcePath == currentPath || strings.HasPrefix(sourcePath, currentPath+string(filepath.Separator)) {
			return folderID
		}
		if targetPath == currentPath || strings.HasPrefix(targetPath, currentPath+string(filepath.Separator)) {
			return folderID
		}
	}

	if len(items) == 1 {
		return strings.TrimSpace(items[0].FolderID)
	}

	return ""
}

func thumbnailNodeBuildArgs(videoPath, outputPath string, offsetSeconds, width int) []string {
	args := []string{
		"-nostdin",
		"-hide_banner",
		"-loglevel", "error",
		"-threads", "1",
		"-y",
		"-ss", strconv.Itoa(offsetSeconds),
		"-i", videoPath,
		"-frames:v", "1",
		"-q:v", "2",
	}
	if width > 0 {
		args = append(args, "-vf", fmt.Sprintf("scale=%d:-2", width))
	}
	args = append(args, outputPath)

	return args
}

func thumbnailNodeIsVideoFile(name string) bool {
	_, ok := thumbnailVideoExtensions[strings.ToLower(filepath.Ext(strings.TrimSpace(name)))]
	return ok
}

func (e *thumbnailNodeExecutor) acquireFFmpegSlot(ctx context.Context) error {
	select {
	case e.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *thumbnailNodeExecutor) releaseFFmpegSlot() {
	select {
	case <-e.sem:
	default:
	}
}
