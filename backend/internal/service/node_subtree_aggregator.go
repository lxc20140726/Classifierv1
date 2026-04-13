package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/liqiye/classifier/internal/repository"
)

const subtreeAggregatorExecutorType = "subtree-aggregator"

type subtreeAggregatorNodeExecutor struct {
	folders   repository.FolderRepository
	snapshots repository.SnapshotRepository
	audit     *AuditService
}

func newSubtreeAggregatorExecutor(
	folders repository.FolderRepository,
	snapshots repository.SnapshotRepository,
	audit *AuditService,
) *subtreeAggregatorNodeExecutor {
	return &subtreeAggregatorNodeExecutor{folders: folders, snapshots: snapshots, audit: audit}
}

func NewSubtreeAggregatorExecutor(
	folders repository.FolderRepository,
	snapshots repository.SnapshotRepository,
	audit *AuditService,
) WorkflowNodeExecutor {
	return newSubtreeAggregatorExecutor(folders, snapshots, audit)
}

func (e *subtreeAggregatorNodeExecutor) Type() string {
	return subtreeAggregatorExecutorType
}

func (e *subtreeAggregatorNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "子树聚合器",
		Description: "聚合各分类器的信号，取最高置信度结果并将最终分类持久化到数据库",
		Inputs: []PortDef{
			{Name: "trees", Type: PortTypeFolderTreeList, Description: "目录树列表", Required: false},
			{Name: "signal_kw", Type: PortTypeClassificationSignalList, Description: "关键词分类器信号", Required: false, Lazy: true},
			{Name: "signal_ft", Type: PortTypeClassificationSignalList, Description: "文件树分类器信号", Required: false, Lazy: true},
			{Name: "signal_ext", Type: PortTypeClassificationSignalList, Description: "扩展名分类器信号", Required: false, Lazy: true},
			{Name: "signal_high", Type: PortTypeClassificationSignalList, Description: "置信度检查高置信度信号", Required: false, Lazy: true},
		},
		Outputs: []PortDef{
			{Name: "entry", Type: PortTypeClassifiedEntryList, Description: "已分类条目列表"},
		},
	}
}

func (e *subtreeAggregatorNodeExecutor) Execute(ctx context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	if e.folders == nil {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute: folder repository is required", e.Type())
	}

	rawInputs := typedInputsToAny(input.Inputs)
	rawTrees, hasTrees := firstPresent(rawInputs, "trees")
	var (
		trees []FolderTree
		err   error
	)
	if hasTrees {
		trees, _, err = parseFolderTreesInput(rawTrees)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute parse trees: %w", e.Type(), err)
		}
	}
	if len(trees) == 0 {
		return NodeExecutionOutput{
			Outputs: map[string]TypedValue{
				"entry": {Type: PortTypeClassifiedEntryList, Value: []ClassifiedEntry{}},
			},
			Status: ExecutionSuccess,
		}, nil
	}

	signalsBySource, err := buildSignalIndex(rawInputs)
	if err != nil {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute build signal index: %w", e.Type(), err)
	}

	entries := make([]ClassifiedEntry, 0, len(trees))
	for _, tree := range trees {
		entry, err := e.aggregateTree(ctx, input, tree, signalsBySource)
		if err != nil {
			return NodeExecutionOutput{}, fmt.Errorf("%s.Execute aggregate tree %q: %w", e.Type(), tree.Path, err)
		}
		entries = append(entries, entry)
	}

	if err := e.syncSourceDirSummaryClassification(ctx, input, entries); err != nil {
		return NodeExecutionOutput{}, fmt.Errorf("%s.Execute sync source dir summary: %w", e.Type(), err)
	}

	return NodeExecutionOutput{Outputs: map[string]TypedValue{"entry": {Type: PortTypeClassifiedEntryList, Value: entries}}, Status: ExecutionSuccess}, nil
}

func (e *subtreeAggregatorNodeExecutor) syncSourceDirSummaryClassification(ctx context.Context, input NodeExecutionInput, entries []ClassifiedEntry) error {
	if e.folders == nil {
		return nil
	}

	candidates := sourceDirCandidates(input.SourceDir, entries)
	if len(candidates) == 0 {
		return nil
	}

	var rootFolder *repository.Folder
	sourceDir := ""
	for _, candidate := range candidates {
		folder, err := e.folders.GetCurrentByPath(ctx, candidate)
		if err != nil {
			if errors.Is(err, repository.ErrNotFound) {
				continue
			}
			return err
		}
		rootFolder = folder
		sourceDir = candidate
		break
	}
	if rootFolder == nil {
		return nil
	}

	for _, entry := range entries {
		if strings.EqualFold(strings.TrimSpace(entry.Path), sourceDir) || strings.TrimSpace(entry.FolderID) == strings.TrimSpace(rootFolder.ID) {
			return nil
		}
	}

	summaryCategory := summarizeEntriesCategory(entries)
	if summaryCategory == "" {
		return nil
	}
	summary := mediaSummary{}
	for _, entry := range entries {
		summary = mergeMediaSummary(summary, summarizeClassifiedEntryMedia(entry))
	}
	rootFolder.ImageCount = summary.imageCount
	rootFolder.VideoCount = summary.videoCount
	rootFolder.OtherFileCount = summary.otherFileCount
	rootFolder.HasOtherFiles = summary.hasOtherFiles
	rootFolder.TotalFiles = summary.totalFiles
	if err := e.folders.Upsert(ctx, rootFolder); err != nil {
		return fmt.Errorf("upsert source dir summary stats for folder %q: %w", rootFolder.ID, err)
	}

	snapshotID, err := e.createCategorySnapshot(ctx, input, rootFolder, sourceDir)
	if err != nil {
		return fmt.Errorf("create source dir category snapshot for folder %q: %w", rootFolder.ID, err)
	}
	if err := e.folders.UpdateCategory(ctx, rootFolder.ID, summaryCategory, "workflow"); err != nil {
		return fmt.Errorf("update source dir category for folder %q: %w", rootFolder.ID, err)
	}
	if err := e.commitCategorySnapshot(ctx, snapshotID, rootFolder, summaryCategory); err != nil {
		return fmt.Errorf("commit source dir category snapshot for folder %q: %w", rootFolder.ID, err)
	}

	return nil
}

func sourceDirCandidates(inputSourceDir string, entries []ClassifiedEntry) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, 2)

	if source := strings.TrimSpace(inputSourceDir); source != "" {
		seen[source] = struct{}{}
		out = append(out, source)
	}

	parent := commonParentPath(entries)
	if parent != "" {
		if _, ok := seen[parent]; !ok {
			out = append(out, parent)
		}
	}

	return out
}

func commonParentPath(entries []ClassifiedEntry) string {
	if len(entries) == 0 {
		return ""
	}

	parent := strings.TrimSpace(filepath.Dir(strings.TrimSpace(entries[0].Path)))
	if parent == "" || parent == "." {
		return ""
	}

	for _, entry := range entries[1:] {
		currentParent := strings.TrimSpace(filepath.Dir(strings.TrimSpace(entry.Path)))
		if !strings.EqualFold(parent, currentParent) {
			return ""
		}
	}

	return parent
}

func summarizeEntriesCategory(entries []ClassifiedEntry) string {
	if len(entries) == 0 {
		return ""
	}

	summary := mediaSummary{}
	for _, entry := range entries {
		summary = mergeMediaSummary(summary, summarizeClassifiedEntryMedia(entry))
	}

	category, _, _ := aggregateTreeCategory(ClassificationSignal{}, summary, mediaSummary{})
	return category
}

func (e *subtreeAggregatorNodeExecutor) aggregateTree(ctx context.Context, input NodeExecutionInput, tree FolderTree, signalsBySource map[string]map[string]ClassificationSignal) (ClassifiedEntry, error) {
	folder, err := e.ensureFolderForTree(ctx, input, tree)
	if err != nil {
		return ClassifiedEntry{}, err
	}

	subtreeEntries := make([]ClassifiedEntry, 0, len(tree.Subdirs))
	for _, subdir := range tree.Subdirs {
		childEntry, err := e.aggregateTree(ctx, input, subdir, signalsBySource)
		if err != nil {
			return ClassifiedEntry{}, err
		}
		subtreeEntries = append(subtreeEntries, childEntry)
	}

	bestSignal := pickBestSignalByPath(tree.Path, signalsBySource)
	summary := summarizeFolderTreeMedia(tree)
	directSummary := summarizeCurrentFolderMedia(tree)
	finalCategory, confidence, reason := aggregateTreeCategory(bestSignal, summary, directSummary)
	if finalCategory == "" {
		finalCategory = folder.Category
	}
	if finalCategory == "" {
		finalCategory = "other"
	}

	folder.ImageCount = summary.imageCount
	folder.VideoCount = summary.videoCount
	folder.OtherFileCount = summary.otherFileCount
	folder.HasOtherFiles = summary.hasOtherFiles
	folder.TotalFiles = summary.totalFiles
	if err := e.folders.Upsert(ctx, folder); err != nil {
		return ClassifiedEntry{}, fmt.Errorf("upsert folder stats for %q: %w", folder.ID, err)
	}

	snapshotID, err := e.createCategorySnapshot(ctx, input, folder, tree.Path)
	if err != nil {
		return ClassifiedEntry{}, fmt.Errorf("create category snapshot for folder %q: %w", folder.ID, err)
	}

	if err := e.folders.UpdateCategory(ctx, folder.ID, finalCategory, "workflow"); err != nil {
		return ClassifiedEntry{}, fmt.Errorf("update category for folder %q: %w", folder.ID, err)
	}
	if err := e.commitCategorySnapshot(ctx, snapshotID, folder, finalCategory); err != nil {
		return ClassifiedEntry{}, fmt.Errorf("commit category snapshot for folder %q: %w", folder.ID, err)
	}

	entry := ClassifiedEntry{
		FolderID:      folder.ID,
		Path:          folder.Path,
		Name:          folder.Name,
		Category:      finalCategory,
		Confidence:    confidence,
		Reason:        reason,
		Classifier:    e.Type(),
		HasOtherFiles: summary.hasOtherFiles,
		Files:         append([]FileEntry(nil), tree.Files...),
		Subtree:       subtreeEntries,
	}
	if strings.TrimSpace(tree.Path) != "" {
		entry.Path = tree.Path
	}
	if strings.TrimSpace(tree.Name) != "" {
		entry.Name = tree.Name
	}

	if e.audit != nil {
		log := &repository.AuditLog{
			JobID:      workflowJobIDForSnapshot(input.WorkflowRun),
			FolderID:   folder.ID,
			FolderPath: entry.Path,
			Action:     e.Type(),
			Result:     "success",
		}
		if err := e.audit.Write(ctx, log); err != nil {
			return ClassifiedEntry{}, fmt.Errorf("write audit for folder %q: %w", folder.ID, err)
		}
	}

	return entry, nil
}

func aggregateTreeCategory(bestSignal ClassificationSignal, summary mediaSummary, directSummary mediaSummary) (string, float64, string) {
	if !bestSignal.IsEmpty && strings.TrimSpace(bestSignal.Category) == "manga" {
		return "manga", bestSignal.Confidence, bestSignal.Reason
	}
	if directSummary.hasManga {
		return "manga", 1, "direct:manga-only"
	}
	if directSummary.hasVideo && !directSummary.hasImage && !directSummary.hasManga {
		return "video", 1, "direct:video-only"
	}
	if summary.hasManga {
		return "manga", 1, "subtree:manga"
	}
	if summary.hasImage && summary.hasVideo {
		return "mixed", 1, "subtree:media-mixed"
	}
	if summary.hasImage {
		return "photo", 1, "subtree:image-only"
	}
	if summary.hasVideo {
		return "video", 1, "subtree:video-only"
	}

	if !bestSignal.IsEmpty && strings.TrimSpace(bestSignal.Category) != "" {
		return bestSignal.Category, bestSignal.Confidence, bestSignal.Reason
	}

	return "other", 0, "subtree:no-media"
}

func (e *subtreeAggregatorNodeExecutor) ensureFolderForTree(ctx context.Context, input NodeExecutionInput, tree FolderTree) (*repository.Folder, error) {
	if strings.TrimSpace(tree.Path) == "" {
		return nil, fmt.Errorf("folder path is required")
	}

	existing, err := e.folders.GetCurrentByPath(ctx, tree.Path)
	if err == nil {
		return existing, nil
	}
	if !errors.Is(err, repository.ErrNotFound) {
		return nil, err
	}

	folder := &repository.Folder{
		ID:             uuid.NewString(),
		Path:           tree.Path,
		SourceDir:      strings.TrimSpace(input.SourceDir),
		RelativePath:   relativePathFromSourceDir(input.SourceDir, tree.Path),
		Name:           strings.TrimSpace(tree.Name),
		Category:       "other",
		CategorySource: "auto",
		Status:         "pending",
		TotalFiles:     countTreeFiles(tree),
	}
	if folder.Name == "" {
		folder.Name = filepath.Base(tree.Path)
	}

	if err := e.folders.Upsert(ctx, folder); err != nil {
		return nil, err
	}

	return folder, nil
}

func relativePathFromSourceDir(sourceDir, path string) string {
	trimmedSource := strings.TrimSpace(sourceDir)
	if trimmedSource == "" {
		return ""
	}

	rel, err := filepath.Rel(trimmedSource, path)
	if err != nil {
		return ""
	}
	if rel == "." {
		return ""
	}

	return rel
}

func countTreeFiles(tree FolderTree) int {
	total := len(tree.Files)
	for _, sub := range tree.Subdirs {
		total += countTreeFiles(sub)
	}

	return total
}

func (e *subtreeAggregatorNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *subtreeAggregatorNodeExecutor) Rollback(ctx context.Context, input NodeRollbackInput) error {
	if e.snapshots == nil || input.WorkflowRun == nil || input.NodeRun == nil {
		return nil
	}

	allSnapshots, err := e.snapshots.ListByJobID(ctx, input.WorkflowRun.JobID)
	if err != nil {
		return fmt.Errorf("%s.Rollback list snapshots: %w", e.Type(), err)
	}

	filtered := make([]*repository.Snapshot, 0, len(allSnapshots))
	for _, snapshot := range allSnapshots {
		if snapshot == nil || snapshot.OperationType != "classify" {
			continue
		}
		if !snapshotMatchesNodeRun(snapshot.Detail, input.WorkflowRun.ID, input.NodeRun.ID) {
			continue
		}
		filtered = append(filtered, snapshot)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return filtered[i].CreatedAt.After(filtered[j].CreatedAt)
	})

	for _, snapshot := range filtered {
		before := snapshotBeforeFolder{}
		if err := json.Unmarshal(snapshot.Before, &before); err != nil {
			return fmt.Errorf("%s.Rollback parse snapshot before %q: %w", e.Type(), snapshot.ID, err)
		}
		category := strings.TrimSpace(before.Category)
		if category == "" {
			category = "other"
		}
		source := strings.TrimSpace(before.CategorySource)
		if source == "" {
			source = "workflow"
		}
		if err := e.folders.UpdateCategory(ctx, snapshot.FolderID, category, source); err != nil {
			return fmt.Errorf("%s.Rollback restore category for folder %q: %w", e.Type(), snapshot.FolderID, err)
		}
		_ = e.snapshots.UpdateStatus(ctx, snapshot.ID, "reverted")
	}

	return nil
}

type snapshotBeforeFolder struct {
	Category       string `json:"category"`
	CategorySource string `json:"category_source"`
}

type snapshotAfterFolder struct {
	Category       string `json:"category"`
	CategorySource string `json:"category_source"`
}

func (e *subtreeAggregatorNodeExecutor) createCategorySnapshot(
	ctx context.Context,
	input NodeExecutionInput,
	folder *repository.Folder,
	folderPath string,
) (string, error) {
	if e.snapshots == nil || folder == nil {
		return "", nil
	}

	before, err := json.Marshal(snapshotBeforeFolder{
		Category:       folder.Category,
		CategorySource: folder.CategorySource,
	})
	if err != nil {
		return "", err
	}
	detail, err := json.Marshal(map[string]any{
		"workflow_run_id": workflowRunID(input.WorkflowRun),
		"node_run_id":     nodeRunIDForSnapshot(input.NodeRun),
		"folder_path":     folderPath,
		"node_type":       e.Type(),
	})
	if err != nil {
		return "", err
	}

	snapshotID := uuid.NewString()
	if err := e.snapshots.Create(ctx, &repository.Snapshot{
		ID:            snapshotID,
		JobID:         workflowJobIDForSnapshot(input.WorkflowRun),
		FolderID:      folder.ID,
		OperationType: "classify",
		Before:        before,
		After:         nil,
		Detail:        detail,
		Status:        "pending",
	}); err != nil {
		return "", err
	}

	return snapshotID, nil
}

func (e *subtreeAggregatorNodeExecutor) commitCategorySnapshot(
	ctx context.Context,
	snapshotID string,
	folder *repository.Folder,
	finalCategory string,
) error {
	if e.snapshots == nil || strings.TrimSpace(snapshotID) == "" || folder == nil {
		return nil
	}

	after, err := json.Marshal(snapshotAfterFolder{
		Category:       finalCategory,
		CategorySource: "workflow",
	})
	if err != nil {
		return err
	}
	if err := e.snapshots.CommitAfter(ctx, snapshotID, after); err != nil {
		return err
	}
	if err := e.snapshots.UpdateStatus(ctx, snapshotID, "committed"); err != nil {
		return err
	}

	return nil
}

func workflowJobIDForSnapshot(run *repository.WorkflowRun) string {
	if run == nil {
		return ""
	}

	return run.JobID
}

func nodeRunIDForSnapshot(run *repository.NodeRun) string {
	if run == nil {
		return ""
	}

	return run.ID
}

func snapshotMatchesNodeRun(detail json.RawMessage, workflowRunID, nodeRunID string) bool {
	if len(detail) == 0 {
		return false
	}
	var parsed struct {
		WorkflowRunID string `json:"workflow_run_id"`
		NodeRunID     string `json:"node_run_id"`
	}
	if err := json.Unmarshal(detail, &parsed); err != nil {
		return false
	}

	return strings.TrimSpace(parsed.WorkflowRunID) == strings.TrimSpace(workflowRunID) &&
		strings.TrimSpace(parsed.NodeRunID) == strings.TrimSpace(nodeRunID)
}

func buildSignalIndex(inputs map[string]any) (map[string]map[string]ClassificationSignal, error) {
	portKeys := []string{"signal_kw", "signal_ft", "signal_high", "signal_ext"}
	index := make(map[string]map[string]ClassificationSignal)

	for _, key := range portKeys {
		raw, exists := inputs[key]
		if !exists || raw == nil {
			continue
		}
		signals, _, err := parseSignalListInput(raw)
		if err != nil {
			return nil, fmt.Errorf("parse %s: %w", key, err)
		}
		for _, signal := range signals {
			if signal.IsEmpty || strings.TrimSpace(signal.Category) == "" {
				continue
			}
			path := strings.TrimSpace(signal.SourcePath)
			if path == "" {
				continue
			}
			if _, ok := index[path]; !ok {
				index[path] = make(map[string]ClassificationSignal)
			}
			index[path][key] = signal
		}
	}

	return index, nil
}

func pickBestSignalByPath(path string, indexed map[string]map[string]ClassificationSignal) ClassificationSignal {
	byPath, ok := indexed[strings.TrimSpace(path)]
	if !ok {
		return ClassificationSignal{}
	}

	for _, key := range []string{"signal_kw", "signal_ft", "signal_high", "signal_ext"} {
		signal, ok := byPath[key]
		if !ok {
			continue
		}
		if signal.IsEmpty || strings.TrimSpace(signal.Category) == "" {
			continue
		}
		return signal
	}

	return ClassificationSignal{}
}

func workflowRunID(run *repository.WorkflowRun) string {
	if run == nil {
		return ""
	}

	return run.ID
}
