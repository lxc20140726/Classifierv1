package service

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

func TestThumbnailNodeUsesTopLevelVideosAndFFmpegFlags(t *testing.T) {
	t.Setenv("THUMBNAIL_MAX_PARALLEL", "1")
	root := t.TempDir()
	sourceDir := filepath.Join(root, "video")
	nestedDir := filepath.Join(sourceDir, "nested")
	if err := os.MkdirAll(nestedDir, 0o755); err != nil {
		t.Fatalf("MkdirAll(nestedDir) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "a.mp4"), []byte("a"), 0o644); err != nil {
		t.Fatalf("WriteFile(a.mp4) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(sourceDir, "b.mkv"), []byte("b"), 0o644); err != nil {
		t.Fatalf("WriteFile(b.mkv) error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(nestedDir, "c.mp4"), []byte("c"), 0o644); err != nil {
		t.Fatalf("WriteFile(c.mp4) error = %v", err)
	}

	executor := newThumbnailNodeExecutor(fs.NewOSAdapter(), nil)
	executor.lookPath = func(_ string) (string, error) { return "ffmpeg", nil }
	var ffmpegCalls int32
	var concurrent int32
	var maxConcurrent int32
	var updates []NodeProgressUpdate
	executor.runFFmpeg = func(_ context.Context, _ string, args ...string) ([]byte, error) {
		atomic.AddInt32(&ffmpegCalls, 1)
		current := atomic.AddInt32(&concurrent, 1)
		if current > maxConcurrent {
			atomic.StoreInt32(&maxConcurrent, current)
		}
		defer atomic.AddInt32(&concurrent, -1)
		joined := strings.Join(args, " ")
		for _, required := range []string{"-threads 1", "-nostdin", "-hide_banner", "-loglevel error"} {
			if !strings.Contains(joined, required) {
				t.Fatalf("ffmpeg args missing %q: %s", required, joined)
			}
		}
		return []byte("ok"), nil
	}

	_, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{
			ID:     "thumb-1",
			Type:   thumbnailNodeExecutorType,
			Label:  "缩略图节点",
			Config: map[string]any{},
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

	if ffmpegCalls != 2 {
		t.Fatalf("ffmpegCalls = %d, want 2 (only top-level videos)", ffmpegCalls)
	}
	if maxConcurrent > 1 {
		t.Fatalf("max concurrent ffmpeg = %d, want <=1", maxConcurrent)
	}
	if len(updates) == 0 {
		t.Fatalf("progress updates is empty")
	}
	last := updates[len(updates)-1]
	if last.Percent != 100 || last.Total != 2 || last.Done != 2 {
		t.Fatalf("last progress = %+v, want percent=100 total=2 done=2", last)
	}
}
