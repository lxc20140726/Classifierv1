package service

import (
	"testing"

	"github.com/liqiye/classifier/internal/repository"
)

func TestResolveOutputDirByKey(t *testing.T) {
	t.Parallel()

	cfg := &repository.AppConfig{
		OutputDirs: repository.AppConfigOutputDirs{
			Video: []string{"/out/video-a", "/out/video-b"},
			Manga: []string{"/out/manga"},
			Photo: []string{},
			Other: []string{},
			Mixed: []string{"/out/mixed"},
		},
	}

	t.Run("category default first item", func(t *testing.T) {
		got := resolveOutputDirByKey(cfg, "video")
		if got != "/out/video-a" {
			t.Fatalf("resolveOutputDirByKey(video) = %q, want /out/video-a", got)
		}
	})

	t.Run("category with index", func(t *testing.T) {
		got := resolveOutputDirByKey(cfg, "video:1")
		if got != "/out/video-b" {
			t.Fatalf("resolveOutputDirByKey(video:1) = %q, want /out/video-b", got)
		}
	})

	t.Run("out of range index returns empty", func(t *testing.T) {
		got := resolveOutputDirByKey(cfg, "video:2")
		if got != "" {
			t.Fatalf("resolveOutputDirByKey(video:2) = %q, want empty", got)
		}
	})
}

func TestResolveWorkflowNodePath_UsesLegacyOptionReference(t *testing.T) {
	t.Parallel()

	cfg := &repository.AppConfig{
		OutputDirs: repository.AppConfigOutputDirs{
			Video: []string{"/out/video-a"},
			Photo: []string{"/out/photo-a", "/out/photo-b"},
		},
	}

	got := resolveWorkflowNodePath(map[string]any{
		"target_dir_source":    "output",
		"target_dir_option_id": "photo:1",
	}, cfg, workflowNodePathOptions{
		DefaultType:      workflowPathRefTypeOutput,
		DefaultOutputKey: "mixed",
		LegacyKeys:       []string{"target_dir", "targetDir", "output_dir"},
	})
	if got != "/out/photo-b" {
		t.Fatalf("resolveWorkflowNodePath(legacy output option) = %q, want /out/photo-b", got)
	}
}
