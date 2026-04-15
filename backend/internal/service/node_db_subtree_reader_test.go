package service

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

func TestDBSubtreeReaderPopulateFilesPreservesMixedRootMediaForSplitter(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)

	sourceRoot := t.TempDir()
	mixedRoot := filepath.Join(sourceRoot, "album")
	promoDir := filepath.Join(mixedRoot, "promo")

	if err := os.MkdirAll(promoDir, 0o755); err != nil {
		t.Fatalf("os.MkdirAll(%q) error = %v", promoDir, err)
	}
	if err := os.WriteFile(filepath.Join(mixedRoot, "movie.mp4"), []byte("video"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(movie.mp4) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(promoDir, "poster.jpg"), []byte("img"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(poster.jpg) error = %v", err)
	}

	now := time.Now().UTC()
	if err := folderRepo.Upsert(ctx, &repository.Folder{
		ID:             "folder-root",
		Path:           normalizeWorkflowPath(mixedRoot),
		SourceDir:      normalizeWorkflowPath(sourceRoot),
		RelativePath:   "album",
		Name:           "album",
		Category:       "mixed",
		CategorySource: "auto",
		Status:         "pending",
		ScannedAt:      now,
	}); err != nil {
		t.Fatalf("folderRepo.Upsert(root) error = %v", err)
	}
	if err := folderRepo.Upsert(ctx, &repository.Folder{
		ID:             "folder-promo",
		Path:           normalizeWorkflowPath(promoDir),
		SourceDir:      normalizeWorkflowPath(sourceRoot),
		RelativePath:   "album/promo",
		Name:           "promo",
		Category:       "photo",
		CategorySource: "auto",
		Status:         "pending",
		ScannedAt:      now,
	}); err != nil {
		t.Fatalf("folderRepo.Upsert(promo) error = %v", err)
	}

	executor := newDBSubtreeReaderExecutor(folderRepo, fs.NewOSAdapter())
	out, err := executor.Execute(ctx, NodeExecutionInput{
		Inputs: testInputs(map[string]any{
			"path": normalizeWorkflowPath(mixedRoot),
		}),
	})
	if err != nil {
		t.Fatalf("dbSubtreeReader Execute() error = %v", err)
	}

	entry, ok := classificationReaderToEntry(out.Outputs["entry"].Value)
	if !ok {
		t.Fatalf("entry output type = %T, want ClassifiedEntry", out.Outputs["entry"].Value)
	}
	if len(entry.Files) != 1 {
		t.Fatalf("len(entry.Files) = %d, want 1", len(entry.Files))
	}
	if entry.Files[0].Name != "movie.mp4" || entry.Files[0].Ext != ".mp4" {
		t.Fatalf("entry.Files[0] = %#v, want movie.mp4/.mp4", entry.Files[0])
	}

	splitter := newFolderSplitterExecutor()
	splitOut, err := splitter.Execute(ctx, NodeExecutionInput{
		Node: repository.WorkflowGraphNode{
			Config: map[string]any{"split_mixed": true, "split_depth": 1},
		},
		Inputs: testInputs(map[string]any{
			"entry": entry,
		}),
	})
	if err != nil {
		t.Fatalf("folderSplitter Execute() error = %v", err)
	}
	items, ok := splitOut.Outputs["items"].Value.([]ProcessingItem)
	if !ok {
		t.Fatalf("items output type = %T, want []ProcessingItem", splitOut.Outputs["items"].Value)
	}

	rootPath := normalizeWorkflowPath(mixedRoot)
	hasRoot := false
	for _, item := range items {
		if normalizeWorkflowPath(item.SourcePath) != rootPath {
			continue
		}
		hasRoot = true
		if !strings.EqualFold(strings.TrimSpace(item.Category), "mixed") {
			t.Fatalf("root item category = %q, want mixed", item.Category)
		}
	}
	if !hasRoot {
		t.Fatalf("split items missing mixed root path %q", rootPath)
	}
}

func TestDBSubtreeBuildEntryCreatesSyntheticRootWhenMissingInDB(t *testing.T) {
	t.Parallel()

	root := "/scan-root"
	items := []*repository.Folder{
		{ID: "f-video", Path: "/scan-root/video-leaf", Name: "video-leaf", Category: "video"},
		{ID: "f-photo", Path: "/scan-root/photo-leaf", Name: "photo-leaf", Category: "photo"},
	}

	entry, ok := dbSubtreeBuildEntry(items, root)
	if !ok {
		t.Fatalf("dbSubtreeBuildEntry() ok = false, want true")
	}
	if entry.Path != root {
		t.Fatalf("entry.Path = %q, want %q", entry.Path, root)
	}
	if entry.Classifier != dbSubtreeReaderExecutorType {
		t.Fatalf("entry.Classifier = %q, want %q", entry.Classifier, dbSubtreeReaderExecutorType)
	}
	if len(entry.Subtree) != 2 {
		t.Fatalf("len(entry.Subtree) = %d, want 2", len(entry.Subtree))
	}
}
