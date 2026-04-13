package service

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

const folderTreeScannerExecutorType = "folder-tree-scanner"

var defaultFolderTreeScannerExcludePatterns = []string{".DS_Store", "Thumbs.db", "desktop.ini", "@eaDir"}

type folderTreeScannerExecutor struct {
	fs fs.FSAdapter
}

func newFolderTreeScannerExecutor(fsAdapter fs.FSAdapter) *folderTreeScannerExecutor {
	return &folderTreeScannerExecutor{fs: fsAdapter}
}

func NewFolderTreeScannerExecutor(fsAdapter fs.FSAdapter) WorkflowNodeExecutor {
	return newFolderTreeScannerExecutor(fsAdapter)
}

func (e *folderTreeScannerExecutor) Type() string {
	return folderTreeScannerExecutorType
}

func (e *folderTreeScannerExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        folderTreeScannerExecutorType,
		Label:       "目录树扫描器",
		Description: "递归扫描源目录，输出顶层子目录 FolderTree 列表",
		Inputs: []PortDef{{
			Name:        "source_dir",
			Type:        PortTypePath,
			Description: "扫描根目录，必须由上游节点连入",
			Required:    true,
		}},
		Outputs: []PortDef{{
			Name:           "tree",
			Type:           PortTypeFolderTreeList,
			RequiredOutput: true,
			Description:    "顶层子目录 FolderTree 列表",
		}},
	}
}

func (e *folderTreeScannerExecutor) Execute(ctx context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	rawInputs := typedInputsToAny(input.Inputs)
	sourceDir := normalizeWorkflowPath(anyString(rawInputs["source_dir"]))
	if sourceDir == "" {
		return NodeExecutionOutput{}, fmt.Errorf("folderTreeScanner.Execute: source_dir is required, connect an upstream node to the source_dir port")
	}

	maxDepth := intConfig(input.Node.Config, "max_depth", 5)
	if maxDepth < 0 {
		maxDepth = 0
	}

	excludePatterns := stringSliceConfig(input.Node.Config, "exclude_patterns", defaultFolderTreeScannerExcludePatterns)
	excludeSet := make(map[string]struct{}, len(excludePatterns))
	for _, pattern := range excludePatterns {
		excludeSet[strings.ToLower(pattern)] = struct{}{}
	}
	excludeDirs := normalizeScanPaths(workflowScanExcludeDirs(input.AppConfig))
	if isExcludedScanPath(sourceDir, excludeDirs) {
		return NodeExecutionOutput{
			Outputs: map[string]TypedValue{"tree": {Type: PortTypeFolderTreeList, Value: []FolderTree{}}},
			Status:  ExecutionSuccess,
		}, nil
	}

	minFileCount := intConfig(input.Node.Config, "min_file_count", 0)
	if minFileCount < 0 {
		minFileCount = 0
	}

	entries, err := e.fs.ReadDir(ctx, sourceDir)
	if err != nil {
		return NodeExecutionOutput{}, fmt.Errorf("folderTreeScanner.Execute read source dir %q: %w", sourceDir, err)
	}

	trees := make([]FolderTree, 0, len(entries))
	hasTopLevelDir := false
	for _, entry := range entries {
		if !entry.IsDir {
			continue
		}
		hasTopLevelDir = true
		if isExcluded(entry.Name, excludeSet) {
			continue
		}

		treePath := joinWorkflowPath(sourceDir, entry.Name)
		if isExcludedScanPath(treePath, excludeDirs) {
			continue
		}
		tree, fileCount, err := e.buildTree(ctx, treePath, 0, maxDepth, excludeSet, excludeDirs)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("folderTreeScanner.Execute build tree for %q: %w", treePath, err)
		}
		if fileCount < minFileCount {
			continue
		}

		trees = append(trees, tree)
	}

	// 当 source_dir 本身就是“待处理文件夹”而非“父目录”时（例如由 folder-picker 输出），
	// 顶层可能没有子目录，此时回退为扫描 source_dir 本身，避免整个链路空跑。
	if len(trees) == 0 && !hasTopLevelDir {
		rootTree, fileCount, err := e.buildTree(ctx, sourceDir, 0, maxDepth, excludeSet, excludeDirs)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("folderTreeScanner.Execute build root tree for %q: %w", sourceDir, err)
		}
		if fileCount >= minFileCount {
			trees = append(trees, rootTree)
		}
	}
	return NodeExecutionOutput{Outputs: map[string]TypedValue{"tree": {Type: PortTypeFolderTreeList, Value: trees}}, Status: ExecutionSuccess}, nil
}

func (e *folderTreeScannerExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *folderTreeScannerExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func (e *folderTreeScannerExecutor) buildTree(
	ctx context.Context,
	path string,
	depth,
	maxDepth int,
	excludePatterns map[string]struct{},
	excludeDirs []string,
) (FolderTree, int, error) {
	entries, err := e.fs.ReadDir(ctx, path)
	if err != nil {
		return FolderTree{}, 0, fmt.Errorf("folderTreeScanner.buildTree read dir %q: %w", path, err)
	}

	files := make([]FileEntry, 0)
	subdirs := make([]FolderTree, 0)
	totalFiles := 0

	for _, entry := range entries {
		if isExcluded(entry.Name, excludePatterns) {
			continue
		}

		if entry.IsDir {
			if depth >= maxDepth {
				continue
			}

			childPath := joinWorkflowPath(path, entry.Name)
			if isExcludedScanPath(childPath, excludeDirs) {
				continue
			}
			childTree, childFileCount, childErr := e.buildTree(ctx, childPath, depth+1, maxDepth, excludePatterns, excludeDirs)
			if childErr != nil {
				return FolderTree{}, 0, childErr
			}

			subdirs = append(subdirs, childTree)
			totalFiles += childFileCount
			continue
		}

		files = append(files, FileEntry{
			Name:      entry.Name,
			Ext:       strings.ToLower(filepath.Ext(entry.Name)),
			SizeBytes: entry.Size,
		})
		totalFiles++
	}

	sort.Slice(files, func(i, j int) bool {
		return files[i].Name < files[j].Name
	})
	sort.Slice(subdirs, func(i, j int) bool {
		return subdirs[i].Name < subdirs[j].Name
	})

	return FolderTree{
		Path:    normalizeWorkflowPath(path),
		Name:    filepath.Base(path),
		Files:   files,
		Subdirs: subdirs,
	}, totalFiles, nil
}

func anyString(value any) string {
	text, ok := value.(string)
	if !ok {
		return ""
	}

	return text
}

func intConfig(config map[string]any, key string, fallback int) int {
	if config == nil {
		return fallback
	}

	raw, ok := config[key]
	if !ok {
		return fallback
	}

	switch value := raw.(type) {
	case int:
		return value
	case int8:
		return int(value)
	case int16:
		return int(value)
	case int32:
		return int(value)
	case int64:
		return int(value)
	case float32:
		return int(value)
	case float64:
		return int(value)
	case string:
		parsed, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return fallback
		}
		return parsed
	default:
		return fallback
	}
}

func stringSliceConfig(config map[string]any, key string, fallback []string) []string {
	if config == nil {
		return append([]string(nil), fallback...)
	}

	raw, ok := config[key]
	if !ok {
		return append([]string(nil), fallback...)
	}

	asStrings, ok := raw.([]string)
	if ok {
		return normalizePatterns(asStrings)
	}

	asAny, ok := raw.([]any)
	if !ok {
		return append([]string(nil), fallback...)
	}

	result := make([]string, 0, len(asAny))
	for _, item := range asAny {
		text, ok := item.(string)
		if !ok {
			continue
		}
		result = append(result, text)
	}

	return normalizePatterns(result)
}

func normalizePatterns(patterns []string) []string {
	if len(patterns) == 0 {
		return []string{}
	}

	out := make([]string, 0, len(patterns))
	for _, pattern := range patterns {
		trimmed := strings.TrimSpace(pattern)
		if trimmed == "" {
			continue
		}
		out = append(out, trimmed)
	}

	return out
}

func isExcluded(name string, excludeSet map[string]struct{}) bool {
	_, ok := excludeSet[strings.ToLower(name)]
	return ok
}

func workflowScanExcludeDirs(appConfig *repository.AppConfig) []string {
	if appConfig == nil {
		return nil
	}

	return flattenOutputDirs(appConfig.OutputDirs)
}
