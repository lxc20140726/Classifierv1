package service

import (
	"context"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const folderSplitterExecutorType = "folder-splitter"

type folderSplitterNodeExecutor struct{}

func newFolderSplitterExecutor() *folderSplitterNodeExecutor {
	return &folderSplitterNodeExecutor{}
}

func NewFolderSplitterExecutor() WorkflowNodeExecutor {
	return newFolderSplitterExecutor()
}

func (e *folderSplitterNodeExecutor) Type() string {
	return folderSplitterExecutorType
}

func (e *folderSplitterNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "\u6587\u4ef6\u5939\u62c6\u5206\u5668",
		Description: "\u5c06\u5206\u7c7b\u6761\u76ee\u8f6c\u4e3a\u5904\u7406\u9879\u5217\u8868\u3002",
		Inputs: []PortDef{
			{Name: "entry", Type: PortTypeJSON, Description: "\u5df2\u5206\u7c7b\u6761\u76ee", Required: true},
		},
		Outputs: []PortDef{
			{Name: "items", Type: PortTypeProcessingItemList, RequiredOutput: true, Description: "\u62c6\u5206\u540e\u7684\u5904\u7406\u9879\u5217\u8868"},
		},
	}
}

func (e *folderSplitterNodeExecutor) Execute(_ context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	rawInputs := typedInputsToAny(input.Inputs)
	entry, ok := classificationReaderResolveInputEntry(rawInputs)
	if !ok {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: entry is required", e.Type())
	}

	splitMixed := folderSplitterBoolConfig(input.Node.Config, "split_mixed", true)
	splitWithSubdirs := folderSplitterBoolConfig(input.Node.Config, "split_with_subdirs", true)
	splitDepth := intConfig(input.Node.Config, "split_depth", 1)
	if splitWithSubdirs {
		splitDepth = math.MaxInt32
	}

	rootPath := normalizeWorkflowPath(entry.Path)
	items := []ProcessingItem{folderSplitterBuildSelfItem(entry, rootPath)}
	entryCategory := folderSplitterResolveEntryCategory(entry)
	shouldSplit := (splitWithSubdirs && len(entry.Subtree) > 0) ||
		(strings.EqualFold(entryCategory, "mixed") && splitMixed && splitDepth > 0)
	if shouldSplit {
		splitItems := folderSplitterBuildRecursiveItems(entry, rootPath, splitDepth, splitWithSubdirs)
		if len(splitItems) > 0 {
			items = splitItems
		}
	}

	return NodeExecutionOutput{Outputs: map[string]TypedValue{"items": {Type: PortTypeProcessingItemList, Value: items}}, Status: ExecutionSuccess}, nil
}

func (e *folderSplitterNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *folderSplitterNodeExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func folderSplitterBoolConfig(config map[string]any, key string, fallback bool) bool {
	if config == nil {
		return fallback
	}

	raw, ok := config[key]
	if !ok {
		return fallback
	}

	switch value := raw.(type) {
	case bool:
		return value
	case string:
		trimmed := strings.TrimSpace(strings.ToLower(value))
		if trimmed == "true" {
			return true
		}
		if trimmed == "false" {
			return false
		}
	}

	return fallback
}

func folderSplitterBuildSelfItem(entry ClassifiedEntry, rootPath string) ProcessingItem {
	normalizedPath := normalizeWorkflowPath(entry.Path)
	normalizedRoot := normalizeWorkflowPath(rootPath)
	relativePath := folderSplitterRelativePath(normalizedRoot, normalizedPath)
	return ProcessingItem{
		SourcePath:         normalizedPath,
		CurrentPath:        normalizedPath,
		FolderID:           entry.FolderID,
		FolderName:         entry.Name,
		TargetName:         entry.Name,
		Category:           folderSplitterResolveItemCategory(entry),
		Files:              append([]FileEntry(nil), entry.Files...),
		ParentPath:         normalizeWorkflowPath(filepath.Dir(normalizedPath)),
		RootPath:           normalizedRoot,
		RelativePath:       relativePath,
		SourceKind:         ProcessingItemSourceKindDirectory,
		OriginalSourcePath: normalizedPath,
	}
}

func folderSplitterBuildRecursiveItems(entry ClassifiedEntry, rootPath string, splitDepth int, splitWithSubdirs bool) []ProcessingItem {
	if len(entry.Subtree) == 0 || splitDepth <= 0 {
		return nil
	}

	collected := make([]ProcessingItem, 0)
	folderSplitterCollectItems(entry, rootPath, splitDepth, splitWithSubdirs, &collected)
	sort.Slice(collected, func(i, j int) bool {
		if collected[i].SourcePath == collected[j].SourcePath {
			return collected[i].FolderName < collected[j].FolderName
		}
		return collected[i].SourcePath < collected[j].SourcePath
	})

	return collected
}

func folderSplitterCollectItems(entry ClassifiedEntry, rootPath string, depth int, splitWithSubdirs bool, out *[]ProcessingItem) {
	if depth <= 0 || len(entry.Subtree) == 0 {
		*out = append(*out, folderSplitterBuildSelfItem(entry, rootPath))
		return
	}

	isMixedRoot := strings.EqualFold(folderSplitterResolveEntryCategory(entry), "mixed")
	keepMixedRoot := isMixedRoot && folderSplitterHasRecognizedMediaFiles(entry.Files)
	if keepMixedRoot {
		*out = append(*out, folderSplitterBuildSelfItem(entry, rootPath))
	}

	directChildren := folderSplitterDirectChildren(entry)
	inheritRootFolderID := ""
	if len(directChildren) == 1 {
		child := directChildren[0]
		if folderSplitterShouldInheritRootFolder(entry, child) {
			inheritRootFolderID = strings.TrimSpace(entry.FolderID)
		}
	}

	for _, child := range directChildren {
		if keepMixedRoot && folderSplitterShouldAbsorbPromoChild(entry, child) {
			continue
		}
		if splitWithSubdirs && len(child.Subtree) > 0 {
			folderSplitterCollectItems(child, rootPath, depth-1, splitWithSubdirs, out)
			continue
		}
		if !splitWithSubdirs && len(child.Subtree) > 0 && depth-1 > 0 {
			folderSplitterCollectItems(child, rootPath, depth-1, splitWithSubdirs, out)
			continue
		}
		item := folderSplitterBuildSelfItem(child, rootPath)
		if inheritRootFolderID != "" {
			item.FolderID = inheritRootFolderID
			item.FolderName = strings.TrimSpace(entry.Name)
			item.TargetName = strings.TrimSpace(entry.Name)
		}
		*out = append(*out, item)
	}
}

func folderSplitterDirectChildren(entry ClassifiedEntry) []ClassifiedEntry {
	if len(entry.Subtree) == 0 {
		return nil
	}

	direct := make([]ClassifiedEntry, 0, len(entry.Subtree))
	for _, child := range entry.Subtree {
		if !folderSplitterIsDirectChild(entry, child) {
			continue
		}
		direct = append(direct, child)
	}
	return direct
}

func folderSplitterShouldInheritRootFolder(parent, child ClassifiedEntry) bool {
	parentCategory := strings.ToLower(strings.TrimSpace(folderSplitterResolveEntryCategory(parent)))
	childCategory := strings.ToLower(strings.TrimSpace(folderSplitterResolveEntryCategory(child)))
	if parentCategory == "" || childCategory == "" || parentCategory != childCategory {
		return false
	}
	if len(child.Subtree) > 0 {
		return false
	}
	if folderSplitterHasRecognizedMediaFiles(parent.Files) {
		return false
	}

	name := strings.ToLower(strings.TrimSpace(child.Name))
	switch parentCategory {
	case "video":
		return name == "video" || name == "videos"
	case "photo":
		return name == "photo" || name == "photos" || name == "image" || name == "images" || name == "picture" || name == "pictures"
	case "manga":
		return name == "manga" || name == "mangas" || name == "comic" || name == "comics"
	default:
		return false
	}
}

func folderSplitterRelativePath(rootPath, sourcePath string) string {
	normalizedRoot := normalizeWorkflowPath(rootPath)
	normalizedSource := normalizeWorkflowPath(sourcePath)
	if normalizedRoot == "" || normalizedSource == "" {
		return ""
	}
	rel, err := filepath.Rel(normalizedRoot, normalizedSource)
	if err != nil || rel == "." || rel == "" || strings.HasPrefix(rel, "..") {
		return ""
	}
	return normalizeWorkflowPath(rel)
}

func folderSplitterIsDirectChild(entry ClassifiedEntry, child ClassifiedEntry) bool {
	if entry.Path != "" && child.Path != "" {
		rel, err := filepath.Rel(entry.Path, child.Path)
		if err == nil && rel != "." && !strings.HasPrefix(rel, "..") {
			return !strings.ContainsRune(rel, os.PathSeparator)
		}
	}

	return false
}

func folderSplitterHasRecognizedMediaFiles(files []FileEntry) bool {
	for _, file := range files {
		ext := strings.ToLower(strings.TrimSpace(file.Ext))
		if videoExtsSet[ext] || imageExtsSet[ext] {
			return true
		}
	}
	return false
}

var folderSplitterPromoKeywords = []string{
	"promo",
	"sample",
	"ad",
	"2048",
	"\u5ba3\u4f20",
}

func folderSplitterShouldAbsorbPromoChild(parent ClassifiedEntry, child ClassifiedEntry) bool {
	if !strings.EqualFold(folderSplitterResolveEntryCategory(parent), "mixed") {
		return false
	}
	if folderSplitterContainsBusinessMedia(child) {
		return false
	}

	name := strings.ToLower(strings.TrimSpace(child.Name))
	for _, keyword := range folderSplitterPromoKeywords {
		if strings.Contains(name, keyword) {
			return true
		}
	}

	containsRealBusinessSubdir := false
	for _, sub := range child.Subtree {
		if !folderSplitterIsDirectChild(child, sub) {
			continue
		}
		if mixedLeafRouterIsInternalStagingDir(strings.TrimSpace(sub.Name)) {
			continue
		}
		containsRealBusinessSubdir = true
		break
	}
	if containsRealBusinessSubdir {
		return false
	}

	if folderSplitterHasUnsupportedFiles(child.Files) && !folderSplitterHasVideoFiles(child.Files) {
		return true
	}
	if folderSplitterHasOnlyInternalStagingSubdirs(child) {
		return true
	}
	childCategory := strings.ToLower(strings.TrimSpace(child.Category))
	if (childCategory == "mixed" || childCategory == "other") && len(child.Files) > 0 && !folderSplitterHasVideoFiles(child.Files) && len(child.Subtree) == 0 {
		return true
	}

	return false
}

func folderSplitterContainsBusinessMedia(entry ClassifiedEntry) bool {
	category := strings.ToLower(strings.TrimSpace(entry.Category))
	if category == "video" || category == "manga" {
		return true
	}
	if folderSplitterHasVideoFiles(entry.Files) {
		return true
	}
	for _, sub := range entry.Subtree {
		if folderSplitterContainsBusinessMedia(sub) {
			return true
		}
	}
	return false
}

func folderSplitterHasVideoFiles(files []FileEntry) bool {
	for _, file := range files {
		ext := strings.ToLower(strings.TrimSpace(file.Ext))
		if videoExtsSet[ext] {
			return true
		}
	}
	return false
}

func folderSplitterHasUnsupportedFiles(files []FileEntry) bool {
	for _, file := range files {
		ext := strings.ToLower(strings.TrimSpace(file.Ext))
		if !videoExtsSet[ext] && !imageExtsSet[ext] {
			return true
		}
	}
	return false
}

func folderSplitterHasOnlyInternalStagingSubdirs(entry ClassifiedEntry) bool {
	if len(entry.Subtree) == 0 {
		return false
	}
	hasInternal := false
	for _, sub := range entry.Subtree {
		if !folderSplitterIsDirectChild(entry, sub) {
			continue
		}
		if !mixedLeafRouterIsInternalStagingDir(strings.TrimSpace(sub.Name)) {
			return false
		}
		hasInternal = true
	}
	return hasInternal
}

func folderSplitterResolveEntryCategory(entry ClassifiedEntry) string {
	category := strings.ToLower(strings.TrimSpace(entry.Category))
	if folderSplitterShouldTreatAsMixedDirectory(entry) {
		return "mixed"
	}
	if len(entry.Subtree) == 0 {
		switch category {
		case "", "other", "mixed":
			if inferred := folderSplitterInferLeafCategory(entry.Files); inferred != "" {
				return inferred
			}
		}
	}
	if category == "" {
		return "other"
	}
	return category
}

func folderSplitterResolveItemCategory(entry ClassifiedEntry) string {
	return folderSplitterResolveEntryCategory(entry)
}

func folderSplitterInferLeafCategory(files []FileEntry) string {
	hasVideo := false
	hasImage := false
	hasManga := false

	for _, file := range files {
		ext := strings.ToLower(strings.TrimSpace(file.Ext))
		if ext == "" {
			ext = strings.ToLower(strings.TrimSpace(filepath.Ext(file.Name)))
		}

		switch {
		case mangaExts[ext]:
			hasManga = true
		case videoExtsSet[ext]:
			hasVideo = true
		case imageExtsSet[ext]:
			hasImage = true
		}
	}

	switch {
	case hasManga:
		return "manga"
	case hasVideo && !hasImage:
		return "video"
	case hasImage && !hasVideo:
		return "photo"
	case hasVideo && hasImage:
		return "mixed"
	default:
		return ""
	}
}

func folderSplitterShouldTreatAsMixedDirectory(entry ClassifiedEntry) bool {
	if len(entry.Subtree) == 0 || !folderSplitterHasRecognizedMediaFiles(entry.Files) {
		return false
	}

	for _, child := range entry.Subtree {
		if !folderSplitterIsDirectChild(entry, child) {
			continue
		}
		if mixedLeafRouterIsInternalStagingDir(strings.TrimSpace(child.Name)) {
			continue
		}
		return true
	}

	return false
}
