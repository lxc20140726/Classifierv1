package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/liqiye/classifier/internal/repository"
)

type subtreeAggregatorFakeFolderRepo struct {
	updateCategoryCalls []subtreeAggregatorUpdateCategoryCall
	updateCategoryErr   error
	foldersByPath       map[string]*repository.Folder
}

type subtreeAggregatorUpdateCategoryCall struct {
	id       string
	category string
	source   string
}

func (r *subtreeAggregatorFakeFolderRepo) Upsert(_ context.Context, folder *repository.Folder) error {
	if r.foldersByPath == nil {
		r.foldersByPath = make(map[string]*repository.Folder)
	}
	r.foldersByPath[folder.Path] = folder
	return nil
}

func (r *subtreeAggregatorFakeFolderRepo) GetByID(_ context.Context, _ string) (*repository.Folder, error) {
	return nil, repository.ErrNotFound
}

func (r *subtreeAggregatorFakeFolderRepo) GetByPath(_ context.Context, path string) (*repository.Folder, error) {
	if r.foldersByPath == nil {
		return nil, repository.ErrNotFound
	}
	folder, ok := r.foldersByPath[path]
	if !ok {
		return nil, repository.ErrNotFound
	}
	return folder, nil
}

func (r *subtreeAggregatorFakeFolderRepo) GetCurrentByPath(ctx context.Context, path string) (*repository.Folder, error) {
	return r.GetByPath(ctx, path)
}

func (r *subtreeAggregatorFakeFolderRepo) GetByHistoricalPath(_ context.Context, _ string) (*repository.Folder, error) {
	return nil, repository.ErrNotFound
}

func (r *subtreeAggregatorFakeFolderRepo) GetCurrentBySourceAndRelativePath(_ context.Context, _ string, _ string) (*repository.Folder, error) {
	return nil, repository.ErrNotFound
}

func (r *subtreeAggregatorFakeFolderRepo) ResolveScanTarget(_ context.Context, path, _ string, _ string) (*repository.Folder, repository.FolderScanMatchType, error) {
	folder, err := r.GetByPath(context.Background(), path)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			return nil, repository.FolderScanMatchTypeNewDiscovery, nil
		}
		return nil, "", err
	}
	return folder, repository.FolderScanMatchTypeCurrentPathMatch, nil
}

func (r *subtreeAggregatorFakeFolderRepo) RecordObservation(_ context.Context, _ string, _ string, _ string, _ string, _ time.Time) error {
	return nil
}

func (r *subtreeAggregatorFakeFolderRepo) List(_ context.Context, _ repository.FolderListFilter) ([]*repository.Folder, int, error) {
	return nil, 0, nil
}

func (r *subtreeAggregatorFakeFolderRepo) ListWorkflowSummariesByFolderIDs(_ context.Context, _ []string) (map[string]repository.FolderWorkflowSummary, error) {
	return map[string]repository.FolderWorkflowSummary{}, nil
}

func (r *subtreeAggregatorFakeFolderRepo) ListByPathPrefix(_ context.Context, _ string) ([]*repository.Folder, error) {
	return nil, nil
}

func (r *subtreeAggregatorFakeFolderRepo) UpdateCategory(_ context.Context, id, category, source string) error {
	r.updateCategoryCalls = append(r.updateCategoryCalls, subtreeAggregatorUpdateCategoryCall{id: id, category: category, source: source})
	if r.updateCategoryErr != nil {
		return r.updateCategoryErr
	}
	return nil
}

func (r *subtreeAggregatorFakeFolderRepo) UpdateStatus(_ context.Context, _ string, _ string) error {
	return nil
}
func (r *subtreeAggregatorFakeFolderRepo) UpdatePath(_ context.Context, _ string, _ string, _ string, _ string) error {
	return nil
}
func (r *subtreeAggregatorFakeFolderRepo) UpdateCoverImagePath(_ context.Context, _ string, _ string) error {
	return nil
}
func (r *subtreeAggregatorFakeFolderRepo) IsSuppressedPath(_ context.Context, _ string) (bool, error) {
	return false, nil
}
func (r *subtreeAggregatorFakeFolderRepo) Suppress(_ context.Context, _ string, _ string, _ string) error {
	return nil
}
func (r *subtreeAggregatorFakeFolderRepo) Unsuppress(_ context.Context, _ string) error { return nil }
func (r *subtreeAggregatorFakeFolderRepo) Delete(_ context.Context, _ string) error     { return nil }

type subtreeAggregatorFakeAuditRepo struct {
	writeCalls []*repository.AuditLog
	writeErr   error
}

func (r *subtreeAggregatorFakeAuditRepo) Write(_ context.Context, log *repository.AuditLog) error {
	r.writeCalls = append(r.writeCalls, log)
	if r.writeErr != nil {
		return r.writeErr
	}
	return nil
}

func (r *subtreeAggregatorFakeAuditRepo) List(_ context.Context, _ repository.AuditListFilter) ([]*repository.AuditLog, int, error) {
	return nil, 0, nil
}
func (r *subtreeAggregatorFakeAuditRepo) GetByID(_ context.Context, _ string) (*repository.AuditLog, error) {
	return nil, repository.ErrNotFound
}

func TestSubtreeAggregatorExecutorBatchMergeBySourcePath(t *testing.T) {
	t.Parallel()

	folderRepo := &subtreeAggregatorFakeFolderRepo{}
	auditRepo := &subtreeAggregatorFakeAuditRepo{}
	executor := newSubtreeAggregatorExecutor(folderRepo, nil, NewAuditService(auditRepo))

	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		SourceDir: "/src",
		WorkflowRun: &repository.WorkflowRun{
			ID: "run-1",
		},
		Inputs: testInputs(map[string]any{
			"trees": []FolderTree{
				{Path: "/src/f1", Name: "f1", Files: []FileEntry{{Name: "a.jpg", Ext: ".jpg"}}},
				{Path: "/src/f2", Name: "f2", Files: []FileEntry{{Name: "b.mp4", Ext: ".mp4"}}},
			},
			"signal_kw": []ClassificationSignal{
				{SourcePath: "/src/f1", Category: "manga", Confidence: 0.95, Reason: "keyword:漫画"},
			},
			"signal_ext": []ClassificationSignal{
				{SourcePath: "/src/f1", Category: "video", Confidence: 0.85, Reason: "ext-ratio:video"},
				{SourcePath: "/src/f2", Category: "video", Confidence: 0.85, Reason: "ext-ratio:video"},
			},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Status != ExecutionSuccess {
		t.Fatalf("status = %q, want success", out.Status)
	}

	entries, ok := out.Outputs["entry"].Value.([]ClassifiedEntry)
	if !ok {
		t.Fatalf("entry output type = %T, want []ClassifiedEntry", out.Outputs["entry"].Value)
	}
	if len(entries) != 2 {
		t.Fatalf("len(entries) = %d, want 2", len(entries))
	}
	if entries[0].Path != "/src/f1" || entries[0].Category != "manga" {
		t.Fatalf("entry[0] = %+v, want kw-priority manga for /src/f1", entries[0])
	}
	if entries[1].Path != "/src/f2" || entries[1].Category != "video" {
		t.Fatalf("entry[1] = %+v, want ext video for /src/f2", entries[1])
	}

	if len(folderRepo.updateCategoryCalls) != 2 {
		t.Fatalf("UpdateCategory calls = %d, want 2", len(folderRepo.updateCategoryCalls))
	}
	if len(auditRepo.writeCalls) != 2 {
		t.Fatalf("audit write calls = %d, want 2", len(auditRepo.writeCalls))
	}
}

func TestSubtreeAggregatorExecutorRecursiveMixedAggregation(t *testing.T) {
	t.Parallel()

	folderRepo := &subtreeAggregatorFakeFolderRepo{}
	executor := newSubtreeAggregatorExecutor(folderRepo, nil, nil)

	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		SourceDir: "/src",
		Inputs: testInputs(map[string]any{
			"trees": []FolderTree{{
				Path: "/src/MyCollection",
				Name: "MyCollection",
				Subdirs: []FolderTree{
					{
						Path:  "/src/MyCollection/photo-a",
						Name:  "photo-a",
						Files: []FileEntry{{Name: "a.jpg", Ext: ".jpg"}},
					},
					{
						Path:  "/src/MyCollection/video-b",
						Name:  "video-b",
						Files: []FileEntry{{Name: "b.mp4", Ext: ".mp4"}},
					},
				},
			}},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	entries := out.Outputs["entry"].Value.([]ClassifiedEntry)
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	if entries[0].Category != "mixed" {
		t.Fatalf("root category = %q, want mixed", entries[0].Category)
	}
	if len(entries[0].Subtree) != 2 {
		t.Fatalf("subtree count = %d, want 2", len(entries[0].Subtree))
	}
	if entries[0].Subtree[0].Category != "photo" {
		t.Fatalf("child[0] category = %q, want photo", entries[0].Subtree[0].Category)
	}
	if entries[0].Subtree[1].Category != "video" {
		t.Fatalf("child[1] category = %q, want video", entries[0].Subtree[1].Category)
	}
	if len(folderRepo.updateCategoryCalls) != 3 {
		t.Fatalf("UpdateCategory calls = %d, want 3", len(folderRepo.updateCategoryCalls))
	}
}

func TestSubtreeAggregatorExecutorUniformChildrenAndStrongDirectSignalOverride(t *testing.T) {
	t.Parallel()

	t.Run("uniform children inherit category", func(t *testing.T) {
		folderRepo := &subtreeAggregatorFakeFolderRepo{}
		executor := newSubtreeAggregatorExecutor(folderRepo, nil, nil)

		out, err := executor.Execute(context.Background(), NodeExecutionInput{
			SourceDir: "/src",
			Inputs: testInputs(map[string]any{
				"trees": []FolderTree{{
					Path: "/src/Series",
					Name: "Series",
					Subdirs: []FolderTree{
						{Path: "/src/Series/a", Name: "a", Files: []FileEntry{{Name: "a.jpg", Ext: ".jpg"}}},
						{Path: "/src/Series/b", Name: "b", Files: []FileEntry{{Name: "b.jpg", Ext: ".jpg"}}},
					},
				}},
			}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		entries := out.Outputs["entry"].Value.([]ClassifiedEntry)
		if entries[0].Category != "photo" {
			t.Fatalf("root category = %q, want photo", entries[0].Category)
		}
	})

	t.Run("strong direct signal overrides inferred category", func(t *testing.T) {
		folderRepo := &subtreeAggregatorFakeFolderRepo{}
		executor := newSubtreeAggregatorExecutor(folderRepo, nil, nil)

		out, err := executor.Execute(context.Background(), NodeExecutionInput{
			SourceDir: "/src",
			Inputs: testInputs(map[string]any{
				"trees": []FolderTree{{
					Path: "/src/Series",
					Name: "Series",
					Subdirs: []FolderTree{
						{Path: "/src/Series/a", Name: "a"},
						{Path: "/src/Series/b", Name: "b"},
					},
				}},
				"signal_kw": []ClassificationSignal{{SourcePath: "/src/Series", Category: "video", Confidence: 0.9, Reason: "keyword:root"}},
			}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		entries := out.Outputs["entry"].Value.([]ClassifiedEntry)
		if entries[0].Category != "video" {
			t.Fatalf("root category = %q, want video", entries[0].Category)
		}
	})
}

func TestSubtreeAggregatorExecutorUpdateError(t *testing.T) {
	t.Parallel()

	folderRepo := &subtreeAggregatorFakeFolderRepo{updateCategoryErr: errors.New("update-failed")}
	executor := newSubtreeAggregatorExecutor(folderRepo, nil, nil)
	_, err := executor.Execute(context.Background(), NodeExecutionInput{
		SourceDir: "/src",
		Inputs: testInputs(map[string]any{
			"trees": []FolderTree{{Path: "/src/f1", Name: "f1"}},
		}),
	})
	if err == nil {
		t.Fatalf("Execute() error = nil, want update error")
	}
}

func TestSubtreeAggregatorExecutorEmptyTreesReturnsSuccess(t *testing.T) {
	t.Parallel()

	folderRepo := &subtreeAggregatorFakeFolderRepo{}
	executor := newSubtreeAggregatorExecutor(folderRepo, nil, nil)

	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		SourceDir: "/src",
		Inputs: testInputs(map[string]any{
			"trees": []FolderTree{},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v, want nil", err)
	}
	if out.Status != ExecutionSuccess {
		t.Fatalf("status = %q, want success", out.Status)
	}

	entries, ok := out.Outputs["entry"].Value.([]ClassifiedEntry)
	if !ok {
		t.Fatalf("entry output type = %T, want []ClassifiedEntry", out.Outputs["entry"].Value)
	}
	if len(entries) != 0 {
		t.Fatalf("len(entries) = %d, want 0", len(entries))
	}
	if len(folderRepo.updateCategoryCalls) != 0 {
		t.Fatalf("UpdateCategory calls = %d, want 0", len(folderRepo.updateCategoryCalls))
	}
}

func TestSubtreeAggregatorExecutorHasOtherFilesPropagation(t *testing.T) {
	t.Parallel()

	folderRepo := &subtreeAggregatorFakeFolderRepo{}
	executor := newSubtreeAggregatorExecutor(folderRepo, nil, nil)

	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		SourceDir: "/src",
		Inputs: testInputs(map[string]any{
			"trees": []FolderTree{{
				Path: "/src/Album",
				Name: "Album",
				Files: []FileEntry{
					{Name: "cover.jpg", Ext: ".jpg"},
					{Name: "note.txt", Ext: ".txt"},
				},
				Subdirs: []FolderTree{
					{
						Path:  "/src/Album/clip",
						Name:  "clip",
						Files: []FileEntry{{Name: "a.mp4", Ext: ".mp4"}},
					},
				},
			}},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	entries := out.Outputs["entry"].Value.([]ClassifiedEntry)
	if len(entries) != 1 {
		t.Fatalf("len(entries) = %d, want 1", len(entries))
	}
	if entries[0].Category != "mixed" {
		t.Fatalf("root category = %q, want mixed", entries[0].Category)
	}
	if !entries[0].HasOtherFiles {
		t.Fatalf("root HasOtherFiles = false, want true")
	}
}
