package service

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/liqiye/classifier/internal/fs"
)

const mixedLeafRouterExecutorType = "mixed-leaf-router"

const (
	mixedLeafRouterVideoPort       = "video"
	mixedLeafRouterPhotoPort       = "photo"
	mixedLeafRouterUnsupportedPort = "unsupported"
)

var mixedLeafRouterPortOrder = []string{
	mixedLeafRouterVideoPort,
	mixedLeafRouterPhotoPort,
	mixedLeafRouterUnsupportedPort,
}

var mixedLeafRouterStagingDirs = map[string]string{
	mixedLeafRouterVideoPort:       "__video",
	mixedLeafRouterPhotoPort:       "__photo",
	mixedLeafRouterUnsupportedPort: "__unsupported",
}

var mixedLeafRouterPromoKeywords = []string{
	"promo",
	"sample",
	"ad",
	"2048",
	"\u5ba3\u4f20",
}

type mixedLeafRouterNodeExecutor struct {
	fs fs.FSAdapter
}

func newMixedLeafRouterExecutor(fsAdapter fs.FSAdapter) *mixedLeafRouterNodeExecutor {
	return &mixedLeafRouterNodeExecutor{fs: fsAdapter}
}

func NewMixedLeafRouterExecutor(fsAdapter fs.FSAdapter) WorkflowNodeExecutor {
	return newMixedLeafRouterExecutor(fsAdapter)
}

func (e *mixedLeafRouterNodeExecutor) Type() string {
	return mixedLeafRouterExecutorType
}

func (e *mixedLeafRouterNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "\u6df7\u5408\u53f6\u5b50\u5206\u6d41\u5668",
		Description: "\u6d88\u8d39 mixed_leaf \u76ee\u5f55\u5e76\u539f\u5730\u62c6\u5206\u4e3a video/photo/unsupported \u4e09\u8def\u76ee\u5f55\u5904\u7406\u9879\u3002",
		Inputs: []PortDef{
			{
				Name:          "items",
				Type:          PortTypeProcessingItemList,
				Description:   "mixed \u53f6\u5b50\u76ee\u5f55\u5904\u7406\u9879\u5217\u8868",
				Required:      true,
				SkipOnEmpty:   true,
				AcceptDefault: true,
			},
		},
		Outputs: []PortDef{
			{Name: mixedLeafRouterVideoPort, Type: PortTypeProcessingItemList, AllowEmpty: true, Description: "\u89c6\u9891\u76ee\u5f55\u5904\u7406\u9879"},
			{Name: mixedLeafRouterPhotoPort, Type: PortTypeProcessingItemList, AllowEmpty: true, Description: "\u56fe\u7247\u76ee\u5f55\u5904\u7406\u9879"},
			{Name: mixedLeafRouterUnsupportedPort, Type: PortTypeProcessingItemList, AllowEmpty: true, Description: "\u4e0d\u652f\u6301\u6587\u4ef6\u76ee\u5f55\u5904\u7406\u9879"},
			{Name: "step_results", Type: PortTypeProcessingStepResultList, AllowEmpty: true, Description: "\u6df7\u5408\u53f6\u5b50\u62c6\u5206\u8fc1\u79fb\u8be6\u60c5"},
		},
	}
}

func (e *mixedLeafRouterNodeExecutor) Execute(ctx context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	items, ok := categoryRouterExtractItems(input.Inputs)
	if !ok {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: items input is required", e.Type())
	}

	outputs := mixedLeafRouterEmptyOutputs()
	stepResults := make([]ProcessingStepResult, 0)
	if len(items) == 0 {
		outputs["step_results"] = TypedValue{Type: PortTypeProcessingStepResultList, Value: stepResults}
		return NodeExecutionOutput{Outputs: outputs, Status: ExecutionSuccess}, nil
	}

	for _, rawItem := range items {
		item := processingItemNormalize(rawItem)
		if !strings.EqualFold(strings.TrimSpace(item.Category), "mixed") {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: item category must be mixed, got %q", e.Type(), item.Category)
		}

		mixedRoot := processingItemCurrentPath(item)
		if strings.TrimSpace(mixedRoot) == "" {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: current_path is required for mixed item", e.Type())
		}

		info, err := e.fs.Stat(ctx, mixedRoot)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: stat mixed root %q: %w", e.Type(), mixedRoot, err)
		}
		if !info.IsDir {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: mixed current_path %q must be a directory", e.Type(), mixedRoot)
		}

		entries, err := e.fs.ReadDir(ctx, mixedRoot)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: read mixed root %q: %w", e.Type(), mixedRoot, err)
		}

		for _, entry := range entries {
			if entry.IsDir || mixedLeafRouterIsInternalStagingDir(strings.TrimSpace(entry.Name)) {
				continue
			}

			portName := mixedLeafRouterClassifyPort(entry.Name)
			sourcePath := joinWorkflowPath(mixedRoot, entry.Name)
			if err := e.mixedLeafRouterMoveToStage(ctx, mixedRoot, sourcePath, portName, entry.Name, &stepResults); err != nil {
				return NodeExecutionOutput{}, err
			}
		}

		for _, entry := range entries {
			if !entry.IsDir {
				continue
			}
			if mixedLeafRouterIsInternalStagingDir(strings.TrimSpace(entry.Name)) {
				continue
			}
			shouldAbsorb, err := e.mixedLeafRouterShouldAbsorbSubdir(ctx, mixedRoot, entry)
			if err != nil {
				return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: inspect subdir %q: %w", e.Type(), joinWorkflowPath(mixedRoot, entry.Name), err)
			}
			if !shouldAbsorb {
				continue
			}
			if err := e.mixedLeafRouterAbsorbSubdir(ctx, mixedRoot, entry.Name, &stepResults); err != nil {
				return NodeExecutionOutput{}, err
			}
		}

		for _, portName := range mixedLeafRouterPortOrder {
			stagePath := mixedLeafRouterStagePath(mixedRoot, portName)
			exists, err := e.fs.Exists(ctx, stagePath)
			if err != nil {
				return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: check staging dir %q: %w", e.Type(), stagePath, err)
			}
			if !exists {
				continue
			}

			stageEntries, err := e.fs.ReadDir(ctx, stagePath)
			if err != nil {
				return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: read staging dir %q: %w", e.Type(), stagePath, err)
			}
			hasFiles := false
			for _, stageEntry := range stageEntries {
				if !stageEntry.IsDir {
					hasFiles = true
					break
				}
			}
			if !hasFiles {
				continue
			}

			outputs[portName] = TypedValue{
				Type:  PortTypeProcessingItemList,
				Value: append(outputs[portName].Value.([]ProcessingItem), mixedLeafRouterBuildOutputItem(item, mixedRoot, stagePath, portName)),
			}
		}
	}
	outputs["step_results"] = TypedValue{Type: PortTypeProcessingStepResultList, Value: stepResults}

	return NodeExecutionOutput{Outputs: outputs, Status: ExecutionSuccess}, nil
}

func (e *mixedLeafRouterNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *mixedLeafRouterNodeExecutor) Rollback(ctx context.Context, input NodeRollbackInput) error {
	stepResults, err := mixedLeafRouterCollectRollbackStepResults(input)
	if err != nil {
		return fmt.Errorf("%s.Rollback: %w", e.Type(), err)
	}
	for i := len(stepResults) - 1; i >= 0; i-- {
		step := stepResults[i]
		from := strings.TrimSpace(step.TargetPath)
		to := strings.TrimSpace(step.SourcePath)
		if from == "" || to == "" {
			continue
		}
		exists, err := e.fs.Exists(ctx, from)
		if err != nil {
			return fmt.Errorf("check rollback source %q: %w", from, err)
		}
		if !exists {
			continue
		}
		if err := e.fs.MkdirAll(ctx, filepath.Dir(to), 0o755); err != nil {
			return fmt.Errorf("mkdir rollback target parent %q: %w", filepath.Dir(to), err)
		}
		if err := e.fs.MoveFile(ctx, from, to); err != nil {
			return fmt.Errorf("move file back %q to %q: %w", from, to, err)
		}
	}

	roots, err := mixedLeafRouterCollectRollbackRoots(input)
	if err != nil {
		return fmt.Errorf("%s.Rollback: %w", e.Type(), err)
	}

	for _, root := range roots {
		for _, portName := range mixedLeafRouterPortOrder {
			stagePath := mixedLeafRouterStagePath(root, portName)
			exists, err := e.fs.Exists(ctx, stagePath)
			if err != nil {
				return fmt.Errorf("check staging dir %q: %w", stagePath, err)
			}
			if !exists {
				continue
			}

			entries, err := e.fs.ReadDir(ctx, stagePath)
			if err != nil {
				return fmt.Errorf("read staging dir %q: %w", stagePath, err)
			}
			for _, entry := range entries {
				if entry.IsDir {
					continue
				}
				src := joinWorkflowPath(stagePath, entry.Name)
				dst := joinWorkflowPath(root, entry.Name)
				if err := e.fs.MoveFile(ctx, src, dst); err != nil {
					return fmt.Errorf("move file back %q to %q: %w", src, dst, err)
				}
			}

			if err := phase4MoveRemoveDirIfEmpty(ctx, e.fs, stagePath); err != nil {
				return err
			}
		}
	}

	return nil
}

func mixedLeafRouterEmptyOutputs() map[string]TypedValue {
	return map[string]TypedValue{
		mixedLeafRouterVideoPort:       {Type: PortTypeProcessingItemList, Value: []ProcessingItem{}},
		mixedLeafRouterPhotoPort:       {Type: PortTypeProcessingItemList, Value: []ProcessingItem{}},
		mixedLeafRouterUnsupportedPort: {Type: PortTypeProcessingItemList, Value: []ProcessingItem{}},
		"step_results":                 {Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{}},
	}
}

func mixedLeafRouterIsInternalStagingDir(name string) bool {
	for _, dirName := range mixedLeafRouterStagingDirs {
		if name == dirName {
			return true
		}
	}
	return false
}

func mixedLeafRouterClassifyPort(fileName string) string {
	ext := strings.ToLower(strings.TrimSpace(filepath.Ext(fileName)))
	if videoExtsSet[ext] {
		return mixedLeafRouterVideoPort
	}
	if imageExtsSet[ext] {
		return mixedLeafRouterPhotoPort
	}
	return mixedLeafRouterUnsupportedPort
}

func mixedLeafRouterStagePath(mixedRoot, portName string) string {
	return joinWorkflowPath(mixedRoot, mixedLeafRouterStagingDirs[portName])
}

func mixedLeafRouterBuildOutputItem(source ProcessingItem, mixedRoot, stagePath, portName string) ProcessingItem {
	folderName := strings.TrimSpace(source.FolderName)
	if folderName == "" {
		folderName = strings.TrimSpace(source.TargetName)
	}
	if folderName == "" {
		folderName = strings.TrimSpace(filepath.Base(mixedRoot))
	}

	return ProcessingItem{
		SourcePath:         stagePath,
		CurrentPath:        stagePath,
		FolderID:           "",
		FolderName:         folderName,
		TargetName:         folderName,
		Category:           portName,
		ParentPath:         mixedRoot,
		RootPath:           mixedRoot,
		RelativePath:       portName,
		SourceKind:         ProcessingItemSourceKindDirectory,
		OriginalSourcePath: mixedRoot,
	}
}

func mixedLeafRouterCollectRollbackRoots(input NodeRollbackInput) ([]string, error) {
	roots := map[string]struct{}{}
	collect := func(raw string, source string) error {
		if strings.TrimSpace(raw) == "" {
			return nil
		}

		typedOutputs, typed, err := parseTypedNodeOutputs(raw)
		if err != nil {
			return fmt.Errorf("parse %s output json: %w", source, err)
		}
		if !typed {
			return nil
		}

		for _, portName := range mixedLeafRouterPortOrder {
			typedValue, ok := typedOutputs[portName]
			if !ok {
				continue
			}
			items, ok := categoryRouterToItems(typedValue.Value)
			if !ok {
				return fmt.Errorf("parse %s port %q items", source, portName)
			}
			for _, item := range items {
				normalized := processingItemNormalize(item)
				root := strings.TrimSpace(normalized.RootPath)
				if root == "" {
					root = normalizeWorkflowPath(filepath.Dir(normalized.CurrentPath))
				}
				if root == "" {
					continue
				}
				roots[root] = struct{}{}
			}
		}
		return nil
	}

	if input.NodeRun != nil {
		if err := collect(input.NodeRun.OutputJSON, "node run"); err != nil {
			return nil, err
		}
	}
	for _, snapshot := range input.Snapshots {
		if snapshot == nil || snapshot.Kind != "post" {
			continue
		}
		if err := collect(snapshot.OutputJSON, fmt.Sprintf("snapshot %q", snapshot.ID)); err != nil {
			return nil, err
		}
	}

	ordered := make([]string, 0, len(roots))
	for root := range roots {
		ordered = append(ordered, root)
	}
	return ordered, nil
}

func mixedLeafRouterCollectRollbackStepResults(input NodeRollbackInput) ([]ProcessingStepResult, error) {
	steps := make([]ProcessingStepResult, 0)
	collect := func(raw string, source string) error {
		if strings.TrimSpace(raw) == "" {
			return nil
		}
		typedOutputs, typed, err := parseTypedNodeOutputs(raw)
		if err != nil {
			return fmt.Errorf("parse %s output json: %w", source, err)
		}
		if !typed {
			return nil
		}
		stepValue, ok := typedOutputs["step_results"]
		if !ok {
			return nil
		}
		steps = append(steps, processingStepResultsFromAny(stepValue.Value)...)
		return nil
	}

	if input.NodeRun != nil {
		if err := collect(input.NodeRun.OutputJSON, "node run"); err != nil {
			return nil, err
		}
	}
	for _, snapshot := range input.Snapshots {
		if snapshot == nil || snapshot.Kind != "post" {
			continue
		}
		if err := collect(snapshot.OutputJSON, fmt.Sprintf("snapshot %q", snapshot.ID)); err != nil {
			return nil, err
		}
	}
	return steps, nil
}

func (e *mixedLeafRouterNodeExecutor) mixedLeafRouterMoveToStage(ctx context.Context, mixedRoot, sourcePath, portName, preferredName string, stepResults *[]ProcessingStepResult) error {
	stagePath := mixedLeafRouterStagePath(mixedRoot, portName)
	if err := e.fs.MkdirAll(ctx, stagePath, 0o755); err != nil {
		return fmt.Errorf("%s.Execute: create staging dir %q: %w", e.Type(), stagePath, err)
	}
	targetPath, err := e.mixedLeafRouterResolveUniqueTargetPath(ctx, stagePath, preferredName)
	if err != nil {
		return fmt.Errorf("%s.Execute: resolve stage target under %q: %w", e.Type(), stagePath, err)
	}
	if err := e.fs.MoveFile(ctx, sourcePath, targetPath); err != nil {
		return fmt.Errorf("%s.Execute: move file %q to %q: %w", e.Type(), sourcePath, targetPath, err)
	}
	*stepResults = append(*stepResults, ProcessingStepResult{
		SourcePath: sourcePath,
		TargetPath: targetPath,
		NodeType:   e.Type(),
		Status:     "moved",
	})
	return nil
}

func (e *mixedLeafRouterNodeExecutor) mixedLeafRouterResolveUniqueTargetPath(ctx context.Context, stagePath, preferredName string) (string, error) {
	baseName := strings.TrimSpace(preferredName)
	if baseName == "" {
		baseName = "unknown.bin"
	}
	ext := filepath.Ext(baseName)
	stem := strings.TrimSuffix(baseName, ext)
	candidate := joinWorkflowPath(stagePath, baseName)
	exists, err := e.fs.Exists(ctx, candidate)
	if err != nil {
		return "", err
	}
	if !exists {
		return candidate, nil
	}
	for index := 1; index < 10_000; index++ {
		nextName := fmt.Sprintf("%s-%d%s", stem, index, ext)
		candidate = joinWorkflowPath(stagePath, nextName)
		exists, err = e.fs.Exists(ctx, candidate)
		if err != nil {
			return "", err
		}
		if !exists {
			return candidate, nil
		}
	}
	return "", fmt.Errorf("cannot resolve unique target for %q", baseName)
}

func (e *mixedLeafRouterNodeExecutor) mixedLeafRouterShouldAbsorbSubdir(ctx context.Context, mixedRoot string, entry fs.DirEntry) (bool, error) {
	subdirName := strings.TrimSpace(entry.Name)
	subdirPath := joinWorkflowPath(mixedRoot, subdirName)
	flatFiles, hasBusinessSubdir, hasInternalStagingSubdir, err := e.mixedLeafRouterClassifySubdir(ctx, subdirPath)
	if err != nil {
		return false, err
	}
	if hasBusinessSubdir || len(flatFiles) == 0 {
		return false, nil
	}

	hasUnsupported := false
	for _, filePath := range flatFiles {
		portName := mixedLeafRouterClassifyPort(filepath.Base(filePath))
		if portName == mixedLeafRouterVideoPort {
			return false, nil
		}
		if portName == mixedLeafRouterUnsupportedPort {
			hasUnsupported = true
		}
	}

	lowerName := strings.ToLower(subdirName)
	for _, keyword := range mixedLeafRouterPromoKeywords {
		if strings.Contains(lowerName, keyword) {
			return true, nil
		}
	}

	return hasUnsupported || hasInternalStagingSubdir, nil
}

func (e *mixedLeafRouterNodeExecutor) mixedLeafRouterClassifySubdir(ctx context.Context, subdirPath string) ([]string, bool, bool, error) {
	entries, err := e.fs.ReadDir(ctx, subdirPath)
	if err != nil {
		return nil, false, false, err
	}

	files := make([]string, 0)
	hasBusinessSubdir := false
	hasInternalStagingSubdir := false
	for _, entry := range entries {
		fullPath := joinWorkflowPath(subdirPath, entry.Name)
		if !entry.IsDir {
			files = append(files, fullPath)
			continue
		}
		if !mixedLeafRouterIsInternalStagingDir(strings.TrimSpace(entry.Name)) {
			hasBusinessSubdir = true
			continue
		}
		hasInternalStagingSubdir = true
		nestedFiles, _, nestedInternal, err := e.mixedLeafRouterClassifySubdir(ctx, fullPath)
		if err != nil {
			return nil, false, false, err
		}
		files = append(files, nestedFiles...)
		if nestedInternal {
			hasInternalStagingSubdir = true
		}
	}

	sort.Strings(files)
	return files, hasBusinessSubdir, hasInternalStagingSubdir, nil
}

func (e *mixedLeafRouterNodeExecutor) mixedLeafRouterAbsorbSubdir(ctx context.Context, mixedRoot, subdirName string, stepResults *[]ProcessingStepResult) error {
	subdirPath := joinWorkflowPath(mixedRoot, subdirName)
	paths, hasBusinessSubdir, _, err := e.mixedLeafRouterClassifySubdir(ctx, subdirPath)
	if err != nil {
		return fmt.Errorf("%s.Execute: scan absorbable subdir %q: %w", e.Type(), subdirPath, err)
	}
	if hasBusinessSubdir {
		return nil
	}
	for _, sourcePath := range paths {
		rel, err := filepath.Rel(subdirPath, sourcePath)
		if err != nil {
			return fmt.Errorf("%s.Execute: build absorb relative path %q -> %q: %w", e.Type(), subdirPath, sourcePath, err)
		}
		rel = normalizeWorkflowPath(rel)
		if strings.HasPrefix(rel, "..") {
			continue
		}
		portName := mixedLeafRouterClassifyPort(filepath.Base(sourcePath))
		preferredName := fmt.Sprintf("%s__%s", subdirName, strings.ReplaceAll(rel, "/", "__"))
		if err := e.mixedLeafRouterMoveToStage(ctx, mixedRoot, sourcePath, portName, preferredName, stepResults); err != nil {
			return err
		}
	}
	return nil
}
