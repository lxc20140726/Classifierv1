package service

import (
	"archive/zip"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

func TestCompressNodeUsesStoreAndReportsFileProgress(t *testing.T) {
	t.Setenv("COMPRESS_MAX_PARALLEL", "2")
	sourceRoot := t.TempDir()
	sourceDir := filepath.Join(sourceRoot, "gallery")
	targetDir := filepath.Join(sourceRoot, "out")
	if err := os.MkdirAll(sourceDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(sourceDir) error = %v", err)
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(targetDir) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "a.jpg"), []byte("a"), 0o644); err != nil {
		t.Fatalf("WriteFile(a.jpg) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "b.png"), []byte("b"), 0o644); err != nil {
		t.Fatalf("WriteFile(b.png) error = %v", err)
	}

	executor := newCompressNodeExecutor(fs.NewOSAdapter())
	var updates []NodeProgressUpdate
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{
			ID:     "compress-1",
			Type:   compressNodeExecutorType,
			Label:  "压缩节点",
			Config: map[string]any{"format": "cbz", "scope": "all", "target_dir": targetDir},
		},
		Inputs: map[string]*TypedValue{
			"items": {
				Type: PortTypeProcessingItemList,
				Value: []ProcessingItem{
					{
						CurrentPath: sourceDir,
						SourcePath:  sourceDir,
						FolderID:    "folder-1",
						SourceKind:  ProcessingItemSourceKindDirectory,
					},
				},
			},
		},
		ProgressFn: func(update NodeProgressUpdate) {
			updates = append(updates, update)
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	archiveItems, ok := out.Outputs["archive_items"].Value.([]ProcessingItem)
	if !ok || len(archiveItems) != 1 {
		t.Fatalf("archive_items type/len = %T/%d, want []ProcessingItem/1", out.Outputs["archive_items"].Value, len(archiveItems))
	}
	archivePath := archiveItems[0].CurrentPath
	if archivePath == "" {
		t.Fatalf("archive path is empty")
	}

	reader, err := zip.OpenReader(archivePath)
	if err != nil {
		t.Fatalf("zip.OpenReader(%q) error = %v", archivePath, err)
	}
	defer reader.Close()

	if len(reader.File) != 2 {
		t.Fatalf("archive entries = %d, want 2", len(reader.File))
	}
	for _, entry := range reader.File {
		if entry.Method != zip.Store {
			t.Fatalf("entry %q method = %d, want %d", entry.Name, entry.Method, zip.Store)
		}
	}

	if len(updates) == 0 {
		t.Fatalf("progress updates is empty")
	}
	last := updates[len(updates)-1]
	if last.Percent != 100 || last.Done != 2 || last.Total != 2 {
		t.Fatalf("last progress = %+v, want percent=100 done=2 total=2", last)
	}
}

func TestCompressNodeReportsMonotonicDoneUnderParallelWorkers(t *testing.T) {
	t.Setenv("COMPRESS_MAX_PARALLEL", "4")
	sourceRoot := t.TempDir()
	firstSourceDir := filepath.Join(sourceRoot, "gallery-a")
	secondSourceDir := filepath.Join(sourceRoot, "gallery-b")
	targetDir := filepath.Join(sourceRoot, "out")
	if err := os.MkdirAll(firstSourceDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(firstSourceDir) error = %v", err)
	}
	if err := os.MkdirAll(secondSourceDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(secondSourceDir) error = %v", err)
	}
	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(targetDir) error = %v", err)
	}

	for _, name := range []string{"a1.jpg", "a2.jpg", "a3.png"} {
		if err := os.WriteFile(filepath.Join(firstSourceDir, name), []byte(name), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
	}
	for _, name := range []string{"b1.jpg", "b2.png", "b3.webp"} {
		if err := os.WriteFile(filepath.Join(secondSourceDir, name), []byte(name), 0o644); err != nil {
			t.Fatalf("WriteFile(%s) error = %v", name, err)
		}
	}

	executor := newCompressNodeExecutor(fs.NewOSAdapter())
	var updates []NodeProgressUpdate
	_, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{
			ID:     "compress-1",
			Type:   compressNodeExecutorType,
			Label:  "压缩节点",
			Config: map[string]any{"format": "cbz", "scope": "all", "target_dir": targetDir},
		},
		Inputs: map[string]*TypedValue{
			"items": {
				Type: PortTypeProcessingItemList,
				Value: []ProcessingItem{
					{
						CurrentPath: firstSourceDir,
						SourcePath:  firstSourceDir,
						FolderID:    "folder-a",
						SourceKind:  ProcessingItemSourceKindDirectory,
					},
					{
						CurrentPath: secondSourceDir,
						SourcePath:  secondSourceDir,
						FolderID:    "folder-b",
						SourceKind:  ProcessingItemSourceKindDirectory,
					},
				},
			},
		},
		ProgressFn: func(update NodeProgressUpdate) {
			updates = append(updates, update)
		},
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if len(updates) == 0 {
		t.Fatalf("progress updates is empty")
	}
	lastDone := -1
	final := updates[len(updates)-1]
	for _, update := range updates {
		if update.Stage != "writing" {
			continue
		}
		if update.Done < lastDone {
			t.Fatalf("progress done is not monotonic: prev=%d current=%d", lastDone, update.Done)
		}
		lastDone = update.Done
	}
	if final.Done != 6 || final.Total != 6 || final.Percent != 100 {
		t.Fatalf("final progress = %+v, want done=6 total=6 percent=100", final)
	}
}
