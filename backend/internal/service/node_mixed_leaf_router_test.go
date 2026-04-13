package service

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

func TestMixedLeafRouterExecutorSchema(t *testing.T) {
	t.Parallel()

	executor := newMixedLeafRouterExecutor(fs.NewMockAdapter())
	schema := executor.Schema()

	if schema.Type != mixedLeafRouterExecutorType {
		t.Fatalf("schema.Type = %q, want %q", schema.Type, mixedLeafRouterExecutorType)
	}
	if len(schema.Inputs) != 1 || schema.Inputs[0].Name != "items" || !schema.Inputs[0].Required {
		t.Fatalf("schema.Inputs = %#v, want required items input", schema.Inputs)
	}
	if !schema.Inputs[0].SkipOnEmpty {
		t.Fatalf("schema.Inputs[0].SkipOnEmpty = false, want true")
	}
	if !schema.Inputs[0].AcceptDefault {
		t.Fatalf("schema.Inputs[0].AcceptDefault = false, want true")
	}
	if len(schema.Outputs) != 4 {
		t.Fatalf("len(schema.Outputs) = %d, want 4", len(schema.Outputs))
	}
	gotOutputs := []string{schema.Outputs[0].Name, schema.Outputs[1].Name, schema.Outputs[2].Name, schema.Outputs[3].Name}
	wantOutputs := []string{mixedLeafRouterVideoPort, mixedLeafRouterPhotoPort, mixedLeafRouterUnsupportedPort, "step_results"}
	for i, want := range wantOutputs {
		if gotOutputs[i] != want {
			t.Fatalf("schema.Outputs[%d].Name = %q, want %q", i, gotOutputs[i], want)
		}
	}
}

func TestMixedLeafRouterExecutorRoutesByFileType(t *testing.T) {
	t.Parallel()

	t.Run("video_only", func(t *testing.T) {
		t.Parallel()

		mixedRoot := filepath.Join(t.TempDir(), "mixed")
		mustMkdirAll(t, mixedRoot)
		writeTestFile(t, filepath.Join(mixedRoot, "a.mp4"))
		writeTestFile(t, filepath.Join(mixedRoot, "b.mkv"))

		out := executeMixedLeafRouter(t, mixedRoot)
		assertPortCount(t, out.Outputs, mixedLeafRouterVideoPort, 1)
		assertPortCount(t, out.Outputs, mixedLeafRouterPhotoPort, 0)
		assertPortCount(t, out.Outputs, mixedLeafRouterUnsupportedPort, 0)

		assertDirFilesEqual(t, filepath.Join(mixedRoot, "__video"), []string{"a.mp4", "b.mkv"})
		assertDirFilesEqual(t, mixedRoot, []string{"__video"})
	})

	t.Run("photo_only", func(t *testing.T) {
		t.Parallel()

		mixedRoot := filepath.Join(t.TempDir(), "mixed")
		mustMkdirAll(t, mixedRoot)
		writeTestFile(t, filepath.Join(mixedRoot, "a.jpg"))
		writeTestFile(t, filepath.Join(mixedRoot, "b.png"))

		out := executeMixedLeafRouter(t, mixedRoot)
		assertPortCount(t, out.Outputs, mixedLeafRouterVideoPort, 0)
		assertPortCount(t, out.Outputs, mixedLeafRouterPhotoPort, 1)
		assertPortCount(t, out.Outputs, mixedLeafRouterUnsupportedPort, 0)

		assertDirFilesEqual(t, filepath.Join(mixedRoot, "__photo"), []string{"a.jpg", "b.png"})
		assertDirFilesEqual(t, mixedRoot, []string{"__photo"})
	})

	t.Run("video_photo_unsupported", func(t *testing.T) {
		t.Parallel()

		mixedRoot := filepath.Join(t.TempDir(), "mixed")
		mustMkdirAll(t, mixedRoot)
		writeTestFile(t, filepath.Join(mixedRoot, "clip.mp4"))
		writeTestFile(t, filepath.Join(mixedRoot, "cover.jpg"))
		writeTestFile(t, filepath.Join(mixedRoot, "note.txt"))

		out := executeMixedLeafRouter(t, mixedRoot)
		assertPortCount(t, out.Outputs, mixedLeafRouterVideoPort, 1)
		assertPortCount(t, out.Outputs, mixedLeafRouterPhotoPort, 1)
		assertPortCount(t, out.Outputs, mixedLeafRouterUnsupportedPort, 1)

		videoItem := out.Outputs[mixedLeafRouterVideoPort].Value.([]ProcessingItem)[0]
		if videoItem.SourcePath != normalizeWorkflowPath(filepath.Join(mixedRoot, "__video")) {
			t.Fatalf("video SourcePath = %q, want %q", videoItem.SourcePath, normalizeWorkflowPath(filepath.Join(mixedRoot, "__video")))
		}
		if videoItem.OriginalSourcePath != normalizeWorkflowPath(mixedRoot) {
			t.Fatalf("video OriginalSourcePath = %q, want %q", videoItem.OriginalSourcePath, normalizeWorkflowPath(mixedRoot))
		}
		if videoItem.RootPath != normalizeWorkflowPath(mixedRoot) {
			t.Fatalf("video RootPath = %q, want %q", videoItem.RootPath, normalizeWorkflowPath(mixedRoot))
		}
		if videoItem.RelativePath != mixedLeafRouterVideoPort {
			t.Fatalf("video RelativePath = %q, want %q", videoItem.RelativePath, mixedLeafRouterVideoPort)
		}
		if videoItem.FolderName != "mixed" || videoItem.TargetName != "mixed" {
			t.Fatalf("video FolderName/TargetName = %q/%q, want mixed/mixed", videoItem.FolderName, videoItem.TargetName)
		}
		if videoItem.FolderID != "" {
			t.Fatalf("video FolderID = %q, want empty", videoItem.FolderID)
		}

		assertDirFilesEqual(t, filepath.Join(mixedRoot, "__video"), []string{"clip.mp4"})
		assertDirFilesEqual(t, filepath.Join(mixedRoot, "__photo"), []string{"cover.jpg"})
		assertDirFilesEqual(t, filepath.Join(mixedRoot, "__unsupported"), []string{"note.txt"})
	})
}

func TestMixedLeafRouterExecutorValidationAndRerun(t *testing.T) {
	t.Parallel()

	t.Run("reject_non_mixed_category", func(t *testing.T) {
		t.Parallel()

		mixedRoot := filepath.Join(t.TempDir(), "mixed")
		mustMkdirAll(t, mixedRoot)
		writeTestFile(t, filepath.Join(mixedRoot, "a.mp4"))

		executor := newMixedLeafRouterExecutor(fs.NewOSAdapter())
		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Inputs: testInputs(map[string]any{
				"items": []ProcessingItem{{
					SourcePath:  mixedRoot,
					CurrentPath: mixedRoot,
					Category:    "video",
				}},
			}),
		})
		if err == nil {
			t.Fatalf("Execute() error = nil, want non-nil")
		}
	})

	t.Run("reject_non_directory_path", func(t *testing.T) {
		t.Parallel()

		filePath := filepath.Join(t.TempDir(), "not-dir.txt")
		writeTestFile(t, filePath)

		executor := newMixedLeafRouterExecutor(fs.NewOSAdapter())
		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Inputs: testInputs(map[string]any{
				"items": []ProcessingItem{{
					SourcePath:  filePath,
					CurrentPath: filePath,
					Category:    "mixed",
				}},
			}),
		})
		if err == nil {
			t.Fatalf("Execute() error = nil, want non-nil")
		}
	})

	t.Run("ignore_business_subdirectory_and_route_root_files", func(t *testing.T) {
		t.Parallel()

		mixedRoot := filepath.Join(t.TempDir(), "mixed")
		mustMkdirAll(t, filepath.Join(mixedRoot, "child"))
		writeTestFile(t, filepath.Join(mixedRoot, "a.mp4"))

		executor := newMixedLeafRouterExecutor(fs.NewOSAdapter())
		out, err := executor.Execute(context.Background(), NodeExecutionInput{
			Inputs: testInputs(map[string]any{
				"items": []ProcessingItem{{
					SourcePath:  mixedRoot,
					CurrentPath: mixedRoot,
					Category:    "mixed",
				}},
			}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		assertPortCount(t, out.Outputs, mixedLeafRouterVideoPort, 1)
		assertPortCount(t, out.Outputs, mixedLeafRouterPhotoPort, 0)
		assertPortCount(t, out.Outputs, mixedLeafRouterUnsupportedPort, 0)

		assertDirFilesEqual(t, filepath.Join(mixedRoot, "__video"), []string{"a.mp4"})
		assertDirFilesEqual(t, mixedRoot, []string{"__video", "child"})
		if !pathExists(t, filepath.Join(mixedRoot, "child")) {
			t.Fatalf("child directory should remain in place")
		}
	})

	t.Run("rerun_reuses_staging_dirs", func(t *testing.T) {
		t.Parallel()

		mixedRoot := filepath.Join(t.TempDir(), "mixed")
		mustMkdirAll(t, mixedRoot)
		writeTestFile(t, filepath.Join(mixedRoot, "a.mp4"))

		first := executeMixedLeafRouter(t, mixedRoot)
		assertPortCount(t, first.Outputs, mixedLeafRouterVideoPort, 1)
		assertDirFilesEqual(t, filepath.Join(mixedRoot, "__video"), []string{"a.mp4"})

		writeTestFile(t, filepath.Join(mixedRoot, "b.mp4"))
		second := executeMixedLeafRouter(t, mixedRoot)
		assertPortCount(t, second.Outputs, mixedLeafRouterVideoPort, 1)
		assertDirFilesEqual(t, filepath.Join(mixedRoot, "__video"), []string{"a.mp4", "b.mp4"})
	})

	t.Run("absorb_promo_subdir_but_keep_business_subdir", func(t *testing.T) {
		t.Parallel()

		mixedRoot := filepath.Join(t.TempDir(), "mixed")
		promoDir := filepath.Join(mixedRoot, "2048")
		businessDir := filepath.Join(mixedRoot, "series")
		nestedPromoDir := filepath.Join(promoDir, "__photo")
		mustMkdirAll(t, nestedPromoDir)
		mustMkdirAll(t, businessDir)
		writeTestFile(t, filepath.Join(mixedRoot, "main.mp4"))
		writeTestFile(t, filepath.Join(promoDir, "poster.jpg"))
		writeTestFile(t, filepath.Join(promoDir, "note.txt"))
		writeTestFile(t, filepath.Join(nestedPromoDir, "nested.png"))
		writeTestFile(t, filepath.Join(businessDir, "ep01.mp4"))

		out := executeMixedLeafRouter(t, mixedRoot)
		assertPortCount(t, out.Outputs, mixedLeafRouterVideoPort, 1)
		assertPortCount(t, out.Outputs, mixedLeafRouterPhotoPort, 1)
		assertPortCount(t, out.Outputs, mixedLeafRouterUnsupportedPort, 1)
		stepResults := out.Outputs["step_results"].Value.([]ProcessingStepResult)
		if len(stepResults) != 4 {
			t.Fatalf("len(step_results) = %d, want 4", len(stepResults))
		}

		assertDirFilesEqual(t, filepath.Join(mixedRoot, "__video"), []string{"main.mp4"})
		assertDirFilesEqual(t, filepath.Join(mixedRoot, "__photo"), []string{"2048__poster.jpg", "2048____photo__nested.png"})
		assertDirFilesEqual(t, filepath.Join(mixedRoot, "__unsupported"), []string{"2048__note.txt"})
		assertDirFilesEqual(t, businessDir, []string{"ep01.mp4"})
	})
}

func TestMixedLeafRouterExecutorRollback(t *testing.T) {
	t.Parallel()

	mixedRoot := filepath.Join(t.TempDir(), "mixed")
	mustMkdirAll(t, mixedRoot)
	writeTestFile(t, filepath.Join(mixedRoot, "clip.mp4"))
	writeTestFile(t, filepath.Join(mixedRoot, "cover.jpg"))
	writeTestFile(t, filepath.Join(mixedRoot, "note.txt"))

	executor := newMixedLeafRouterExecutor(fs.NewOSAdapter())
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{
			"items": []ProcessingItem{{
				SourcePath:  mixedRoot,
				CurrentPath: mixedRoot,
				Category:    "mixed",
				FolderID:    "folder-mixed",
			}},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if err := executor.Rollback(context.Background(), NodeRollbackInput{
		Snapshots: []*repository.NodeSnapshot{
			{
				ID:         "mixed-router-snapshot",
				Kind:       "post",
				OutputJSON: mustJSONMarshal(t, mustTypedOutputsMap(t, out.Outputs)),
			},
		},
	}); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	assertDirFilesEqual(t, mixedRoot, []string{"clip.mp4", "cover.jpg", "note.txt"})
	if pathExists(t, filepath.Join(mixedRoot, "__video")) {
		t.Fatalf("__video should be removed after rollback")
	}
	if pathExists(t, filepath.Join(mixedRoot, "__photo")) {
		t.Fatalf("__photo should be removed after rollback")
	}
	if pathExists(t, filepath.Join(mixedRoot, "__unsupported")) {
		t.Fatalf("__unsupported should be removed after rollback")
	}
}

func TestMixedLeafRouterExecutorRollbackRestoresAbsorbedPromoSubdir(t *testing.T) {
	t.Parallel()

	mixedRoot := filepath.Join(t.TempDir(), "mixed")
	promoDir := filepath.Join(mixedRoot, "2048")
	internalPromoDir := filepath.Join(promoDir, "__photo")
	mustMkdirAll(t, internalPromoDir)
	writeTestFile(t, filepath.Join(mixedRoot, "main.mp4"))
	writeTestFile(t, filepath.Join(promoDir, "poster.jpg"))
	writeTestFile(t, filepath.Join(promoDir, "note.txt"))
	writeTestFile(t, filepath.Join(internalPromoDir, "nested.png"))

	executor := newMixedLeafRouterExecutor(fs.NewOSAdapter())
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{
			"items": []ProcessingItem{{
				SourcePath:  mixedRoot,
				CurrentPath: mixedRoot,
				Category:    "mixed",
			}},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if err := executor.Rollback(context.Background(), NodeRollbackInput{
		Snapshots: []*repository.NodeSnapshot{
			{
				ID:         "mixed-router-rollback-promo",
				Kind:       "post",
				OutputJSON: mustJSONMarshal(t, mustTypedOutputsMap(t, out.Outputs)),
			},
		},
	}); err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	assertDirFilesEqual(t, mixedRoot, []string{"2048", "main.mp4"})
	assertDirFilesEqual(t, promoDir, []string{"__photo", "note.txt", "poster.jpg"})
	assertDirFilesEqual(t, internalPromoDir, []string{"nested.png"})
}

func executeMixedLeafRouter(t *testing.T, mixedRoot string) NodeExecutionOutput {
	t.Helper()

	executor := newMixedLeafRouterExecutor(fs.NewOSAdapter())
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{
			"items": []ProcessingItem{{
				SourcePath:  mixedRoot,
				CurrentPath: mixedRoot,
				Category:    "mixed",
				FolderName:  "mixed",
				TargetName:  "mixed",
				SourceKind:  ProcessingItemSourceKindDirectory,
			}},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Status != ExecutionSuccess {
		t.Fatalf("status = %q, want %q", out.Status, ExecutionSuccess)
	}
	return out
}

func assertPortCount(t *testing.T, outputs map[string]TypedValue, port string, want int) {
	t.Helper()

	items, ok := outputs[port].Value.([]ProcessingItem)
	if !ok {
		t.Fatalf("port %q type = %T, want []ProcessingItem", port, outputs[port].Value)
	}
	if len(items) != want {
		t.Fatalf("port %q len = %d, want %d", port, len(items), want)
	}
}

func assertDirFilesEqual(t *testing.T, dir string, want []string) {
	t.Helper()

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("os.ReadDir(%q) error = %v", dir, err)
	}

	got := make([]string, 0, len(entries))
	for _, entry := range entries {
		got = append(got, entry.Name())
	}

	sort.Strings(got)
	sort.Strings(want)
	if len(got) != len(want) {
		t.Fatalf("dir %q entries len = %d, want %d; got=%v want=%v", dir, len(got), len(want), got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("dir %q entries = %v, want %v", dir, got, want)
		}
	}
}

func writeTestFile(t *testing.T, path string) {
	t.Helper()

	mustMkdirAll(t, filepath.Dir(path))
	if err := os.WriteFile(path, []byte("fixture"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", path, err)
	}
}
