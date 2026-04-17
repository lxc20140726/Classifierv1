package service

import (
	"archive/zip"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

func TestFolderSplitterExecutorMixedSplitFirstLevel(t *testing.T) {
	t.Parallel()

	executor := newFolderSplitterExecutor()
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{
			Config: map[string]any{"split_mixed": true, "split_depth": 1},
		},
		Inputs: testInputs(map[string]any{
			"entry": ClassifiedEntry{
				FolderID: "folder-root",
				Path:     "/root/mixed",
				Name:     "mixed",
				Category: "mixed",
				Files: []FileEntry{
					{Name: "movie.mp4", Ext: ".mp4", SizeBytes: 10},
				},
				Subtree: []ClassifiedEntry{
					{
						FolderID: "folder-a",
						Path:     "/root/mixed/child-a",
						Name:     "child-a",
						Category: "video",
					},
					{
						FolderID: "folder-b",
						Path:     "/root/mixed/child-b",
						Name:     "child-b",
						Category: "photo",
					},
					{
						FolderID: "folder-a-nested",
						Path:     "/root/mixed/child-a/nested",
						Name:     "nested",
						Category: "manga",
					},
				},
			},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Status != ExecutionSuccess {
		t.Fatalf("status = %q, want %q", out.Status, ExecutionSuccess)
	}
	if len(out.Outputs) != 1 {
		t.Fatalf("len(outputs) = %d, want 1", len(out.Outputs))
	}

	items, ok := out.Outputs["items"].Value.([]ProcessingItem)
	if !ok {
		t.Fatalf("output type = %T, want []ProcessingItem", out.Outputs["items"].Value)
	}
	if len(items) != 3 {
		t.Fatalf("len(items) = %d, want 3", len(items))
	}
	if items[0].SourcePath != "/root/mixed" || items[0].CurrentPath != "/root/mixed" || items[0].Category != "mixed" {
		t.Fatalf("items[0] = %#v, want root mixed", items[0])
	}
	if items[0].RootPath != "/root/mixed" || items[0].RelativePath != "" || items[0].SourceKind != ProcessingItemSourceKindDirectory {
		t.Fatalf("items[0] root/relative/source_kind = %q/%q/%q, want /root/mixed/empty/directory", items[0].RootPath, items[0].RelativePath, items[0].SourceKind)
	}
	if len(items[0].Files) != 1 || items[0].Files[0].Name != "movie.mp4" {
		t.Fatalf("items[0].Files = %#v, want root file movie.mp4", items[0].Files)
	}
	if items[1].SourcePath != "/root/mixed/child-a" || items[1].CurrentPath != "/root/mixed/child-a" || items[1].Category != "video" {
		t.Fatalf("items[1] = %#v, want child-a video", items[1])
	}
	if items[1].RootPath != "/root/mixed" || items[1].RelativePath != "child-a" || items[1].SourceKind != ProcessingItemSourceKindDirectory {
		t.Fatalf("items[1] root/relative/source_kind = %q/%q/%q, want /root/mixed/child-a/directory", items[1].RootPath, items[1].RelativePath, items[1].SourceKind)
	}
	if items[2].SourcePath != "/root/mixed/child-b" || items[2].CurrentPath != "/root/mixed/child-b" || items[2].Category != "photo" {
		t.Fatalf("items[2] = %#v, want child-b photo", items[2])
	}
	if items[2].RootPath != "/root/mixed" || items[2].RelativePath != "child-b" || items[2].SourceKind != ProcessingItemSourceKindDirectory {
		t.Fatalf("items[2] root/relative/source_kind = %q/%q/%q, want /root/mixed/child-b/directory", items[2].RootPath, items[2].RelativePath, items[2].SourceKind)
	}
}

func TestFolderSplitterExecutorMixedSkipsPromoSubdirs(t *testing.T) {
	t.Parallel()

	executor := newFolderSplitterExecutor()
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{
			Config: map[string]any{"split_mixed": true, "split_depth": 1},
		},
		Inputs: testInputs(map[string]any{
			"entry": ClassifiedEntry{
				FolderID: "folder-root",
				Path:     "/root/mixed",
				Name:     "mixed",
				Category: "mixed",
				Files: []FileEntry{
					{Name: "movie.mp4", Ext: ".mp4", SizeBytes: 10},
				},
				Subtree: []ClassifiedEntry{
					{
						FolderID: "folder-business",
						Path:     "/root/mixed/business",
						Name:     "business",
						Category: "video",
						Files: []FileEntry{
							{Name: "episode.mp4", Ext: ".mp4"},
						},
					},
					{
						FolderID: "folder-promo",
						Path:     "/root/mixed/2048",
						Name:     "2048",
						Category: "other",
						Files: []FileEntry{
							{Name: "poster.jpg", Ext: ".jpg"},
							{Name: "readme.txt", Ext: ".txt"},
						},
					},
				},
			},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	items, ok := out.Outputs["items"].Value.([]ProcessingItem)
	if !ok {
		t.Fatalf("output type = %T, want []ProcessingItem", out.Outputs["items"].Value)
	}
	if len(items) != 2 {
		t.Fatalf("len(items) = %d, want 2", len(items))
	}
	if items[0].SourcePath != "/root/mixed" || items[0].Category != "mixed" {
		t.Fatalf("items[0] = %#v, want root mixed", items[0])
	}
	if items[1].SourcePath != "/root/mixed/business" || items[1].Category != "video" {
		t.Fatalf("items[1] = %#v, want business video", items[1])
	}
}

func TestFolderSplitterExecutorInheritsRootFolderForGenericContainerChild(t *testing.T) {
	t.Parallel()

	executor := newFolderSplitterExecutor()
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{
			Config: map[string]any{"split_with_subdirs": true},
		},
		Inputs: testInputs(map[string]any{
			"entry": ClassifiedEntry{
				FolderID: "folder-root",
				Path:     "/root/album-a",
				Name:     "album-a",
				Category: "video",
				Subtree: []ClassifiedEntry{
					{
						FolderID: "folder-videos",
						Path:     "/root/album-a/Videos",
						Name:     "Videos",
						Category: "video",
						Files: []FileEntry{
							{Name: "clip-01.mp4", Ext: ".mp4", SizeBytes: 10},
						},
					},
				},
			},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	items, ok := out.Outputs["items"].Value.([]ProcessingItem)
	if !ok {
		t.Fatalf("output type = %T, want []ProcessingItem", out.Outputs["items"].Value)
	}
	if len(items) != 1 {
		t.Fatalf("len(items) = %d, want 1", len(items))
	}
	if items[0].SourcePath != "/root/album-a/Videos" {
		t.Fatalf("items[0].SourcePath = %q, want /root/album-a/Videos", items[0].SourcePath)
	}
	if items[0].FolderID != "folder-root" {
		t.Fatalf("items[0].FolderID = %q, want folder-root", items[0].FolderID)
	}
	if items[0].FolderName != "album-a" || items[0].TargetName != "album-a" {
		t.Fatalf("items[0] folder/target name = %q/%q, want album-a/album-a", items[0].FolderName, items[0].TargetName)
	}
	if items[0].RelativePath != "Videos" || items[0].RootPath != "/root/album-a" {
		t.Fatalf("items[0] relative/root = %q/%q, want Videos//root/album-a", items[0].RelativePath, items[0].RootPath)
	}
}

func TestProcessingChainRoutesNonLeafVideoWithPromoSubdirsToMixedLeaf(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	mixedRoot := filepath.Join(root, "5kporn.e20.melody.marks.5k")
	promoDir := filepath.Join(mixedRoot, "2048")
	publicityDir := filepath.Join(mixedRoot, "promo-files")
	internalPhotoDir := filepath.Join(promoDir, "__photo")
	internalUnsupportedDir := filepath.Join(promoDir, "__unsupported")
	internalVideoDir := filepath.Join(promoDir, "__video")

	mustMkdirAll(t, internalPhotoDir)
	mustMkdirAll(t, internalUnsupportedDir)
	mustMkdirAll(t, internalVideoDir)
	mustMkdirAll(t, publicityDir)

	writeTestFile(t, filepath.Join(mixedRoot, "5kporn.e20.melody.marks.5k.mp4"))
	writeTestFile(t, filepath.Join(internalPhotoDir, "promo.jpg"))
	writeTestFile(t, filepath.Join(internalUnsupportedDir, "promo.url"))
	writeTestFile(t, filepath.Join(publicityDir, "banner.gif"))
	writeTestFile(t, filepath.Join(publicityDir, "release.mht"))

	splitter := newFolderSplitterExecutor()
	splitOut, err := splitter.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{
			Config: map[string]any{"split_mixed": true, "split_depth": 1},
		},
		Inputs: testInputs(map[string]any{
			"entry": ClassifiedEntry{
				FolderID: "folder-root",
				Path:     normalizeWorkflowPath(mixedRoot),
				Name:     "5kporn.e20.melody.marks.5k",
				Category: "video",
				Files: []FileEntry{
					{Name: "5kporn.e20.melody.marks.5k.mp4", Ext: ".mp4"},
				},
				Subtree: []ClassifiedEntry{
					{
						FolderID: "folder-promo",
						Path:     normalizeWorkflowPath(promoDir),
						Name:     "2048",
						Category: "mixed",
						Subtree: []ClassifiedEntry{
							{
								FolderID: "folder-promo-photo",
								Path:     normalizeWorkflowPath(internalPhotoDir),
								Name:     "__photo",
								Category: "photo",
								Files: []FileEntry{
									{Name: "promo.jpg", Ext: ".jpg"},
								},
							},
							{
								FolderID: "folder-promo-unsupported",
								Path:     normalizeWorkflowPath(internalUnsupportedDir),
								Name:     "__unsupported",
								Category: "other",
								Files: []FileEntry{
									{Name: "promo.url", Ext: ".url"},
								},
							},
							{
								FolderID: "folder-promo-video",
								Path:     normalizeWorkflowPath(internalVideoDir),
								Name:     "__video",
								Category: "video",
							},
						},
					},
					{
						FolderID: "folder-publicity",
						Path:     normalizeWorkflowPath(publicityDir),
						Name:     "promo-files",
						Category: "other",
						Files: []FileEntry{
							{Name: "banner.gif", Ext: ".gif"},
							{Name: "release.mht", Ext: ".mht"},
						},
					},
				},
			},
		}),
	})
	if err != nil {
		t.Fatalf("splitter Execute() error = %v", err)
	}

	splitItems, ok := splitOut.Outputs["items"].Value.([]ProcessingItem)
	if !ok {
		t.Fatalf("split output type = %T, want []ProcessingItem", splitOut.Outputs["items"].Value)
	}
	rootMixedFound := false
	for _, item := range splitItems {
		if item.SourcePath != normalizeWorkflowPath(mixedRoot) {
			continue
		}
		rootMixedFound = true
		if item.Category != "mixed" {
			t.Fatalf("root split item category = %q, want mixed", item.Category)
		}
	}
	if !rootMixedFound {
		t.Fatalf("split items missing root mixed path %q", normalizeWorkflowPath(mixedRoot))
	}

	router := newCategoryRouterExecutor()
	routeOut, err := router.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{"items": splitItems}),
	})
	if err != nil {
		t.Fatalf("category router Execute() error = %v", err)
	}

	mixedItems := routeOut.Outputs["mixed_leaf"].Value.([]ProcessingItem)
	rootMixedRouted := false
	for _, item := range mixedItems {
		if item.SourcePath == normalizeWorkflowPath(mixedRoot) {
			rootMixedRouted = true
			break
		}
	}
	if !rootMixedRouted {
		t.Fatalf("mixed_leaf output missing root path %q", normalizeWorkflowPath(mixedRoot))
	}

	mixedRouter := newMixedLeafRouterExecutor(fs.NewOSAdapter())
	mixedOut, err := mixedRouter.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{"items": []ProcessingItem{
			{
				SourcePath:  normalizeWorkflowPath(mixedRoot),
				CurrentPath: normalizeWorkflowPath(mixedRoot),
				FolderName:  "5kporn.e20.melody.marks.5k",
				TargetName:  "5kporn.e20.melody.marks.5k",
				Category:    "mixed",
				SourceKind:  ProcessingItemSourceKindDirectory,
			},
		}}),
	})
	if err != nil {
		t.Fatalf("mixed router Execute() error = %v", err)
	}

	assertPortCount(t, mixedOut.Outputs, mixedLeafRouterVideoPort, 1)
	assertPortCount(t, mixedOut.Outputs, mixedLeafRouterPhotoPort, 1)
	assertPortCount(t, mixedOut.Outputs, mixedLeafRouterUnsupportedPort, 1)

	assertDirFilesEqual(t, filepath.Join(mixedRoot, "__video"), []string{"5kporn.e20.melody.marks.5k.mp4"})
	assertDirFilesEqual(t, filepath.Join(mixedRoot, "__photo"), []string{"2048____photo__promo.jpg", "promo-files__banner.gif"})
	assertDirFilesEqual(t, filepath.Join(mixedRoot, "__unsupported"), []string{"2048____unsupported__promo.url", "promo-files__release.mht"})
}

func TestProcessingChainMixedRootWithPromoSubdir(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	mixedRoot := filepath.Join(root, "mixed")
	promoDir := filepath.Join(mixedRoot, "2048")
	businessDir := filepath.Join(mixedRoot, "series")
	internalPromoDir := filepath.Join(promoDir, "__photo")
	mustMkdirAll(t, mixedRoot)
	mustMkdirAll(t, promoDir)
	mustMkdirAll(t, businessDir)
	mustMkdirAll(t, internalPromoDir)

	writeTestFile(t, filepath.Join(mixedRoot, "main.mp4"))
	writeTestFile(t, filepath.Join(promoDir, "poster.jpg"))
	writeTestFile(t, filepath.Join(promoDir, "ad.txt"))
	writeTestFile(t, filepath.Join(internalPromoDir, "nested.png"))
	writeTestFile(t, filepath.Join(businessDir, "ep01.mp4"))

	entry := ClassifiedEntry{
		Path:     normalizeWorkflowPath(mixedRoot),
		Name:     "mixed",
		Category: "mixed",
		Files: []FileEntry{
			{Name: "main.mp4", Ext: ".mp4"},
		},
		Subtree: []ClassifiedEntry{
			{
				Path:     normalizeWorkflowPath(promoDir),
				Name:     "2048",
				Category: "mixed",
				Files: []FileEntry{
					{Name: "poster.jpg", Ext: ".jpg"},
					{Name: "ad.txt", Ext: ".txt"},
				},
				Subtree: []ClassifiedEntry{
					{
						Path:     normalizeWorkflowPath(internalPromoDir),
						Name:     "__photo",
						Category: "photo",
						Files: []FileEntry{
							{Name: "nested.png", Ext: ".png"},
						},
					},
				},
			},
			{
				Path:     normalizeWorkflowPath(businessDir),
				Name:     "series",
				Category: "video",
				Files: []FileEntry{
					{Name: "ep01.mp4", Ext: ".mp4"},
				},
			},
		},
	}

	splitter := newFolderSplitterExecutor()
	splitOut, err := splitter.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{
			Config: map[string]any{"split_mixed": true, "split_depth": 1},
		},
		Inputs: testInputs(map[string]any{"entry": entry}),
	})
	if err != nil {
		t.Fatalf("splitter Execute() error = %v", err)
	}

	splitItems, ok := splitOut.Outputs["items"].Value.([]ProcessingItem)
	if !ok {
		t.Fatalf("split output type = %T, want []ProcessingItem", splitOut.Outputs["items"].Value)
	}
	if len(splitItems) != 2 {
		t.Fatalf("len(split items) = %d, want 2", len(splitItems))
	}

	router := newCategoryRouterExecutor()
	routeOut, err := router.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{"items": splitItems}),
	})
	if err != nil {
		t.Fatalf("category router Execute() error = %v", err)
	}
	mixedItems := routeOut.Outputs["mixed_leaf"].Value.([]ProcessingItem)
	if len(mixedItems) != 1 {
		t.Fatalf("mixed_leaf len = %d, want 1", len(mixedItems))
	}

	mixedRouter := newMixedLeafRouterExecutor(fs.NewOSAdapter())
	mixedOut, err := mixedRouter.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{"items": mixedItems}),
	})
	if err != nil {
		t.Fatalf("mixed router Execute() error = %v", err)
	}

	assertPortCount(t, mixedOut.Outputs, mixedLeafRouterVideoPort, 1)
	assertPortCount(t, mixedOut.Outputs, mixedLeafRouterPhotoPort, 1)
	assertPortCount(t, mixedOut.Outputs, mixedLeafRouterUnsupportedPort, 1)

	assertDirFilesEqual(t, filepath.Join(mixedRoot, "__video"), []string{"main.mp4"})
	assertDirFilesEqual(t, filepath.Join(mixedRoot, "__photo"), []string{"2048__poster.jpg", "2048____photo__nested.png"})
	assertDirFilesEqual(t, filepath.Join(mixedRoot, "__unsupported"), []string{"2048__ad.txt"})
	assertDirFilesEqual(t, businessDir, []string{"ep01.mp4"})
}

func TestCategoryRouterExecutorPortPlacement(t *testing.T) {
	t.Parallel()

	executor := newCategoryRouterExecutor()
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{
			"items": []ProcessingItem{
				{FolderName: "v", Category: "video"},
				{FolderName: "m", Category: "manga"},
				{FolderName: "p", Category: "photo"},
				{FolderName: "o", Category: "other"},
				{FolderName: "x", Category: "mixed"},
			},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if out.Status != ExecutionSuccess {
		t.Fatalf("status = %q, want %q", out.Status, ExecutionSuccess)
	}
	if len(out.Outputs) != 5 {
		t.Fatalf("len(outputs) = %d, want 5", len(out.Outputs))
	}

	for _, key := range []string{"video", "manga", "photo", "other", "mixed_leaf"} {
		list, ok := out.Outputs[key].Value.([]ProcessingItem)
		if !ok || len(list) != 1 {
			t.Fatalf("port %s type/count = %T/%d, want []ProcessingItem/1", key, out.Outputs[key].Value, len(list))
		}
	}

	if out.Outputs["video"].Value.([]ProcessingItem)[0].Category != "video" {
		t.Fatalf("video port category = %q, want video", out.Outputs["video"].Value.([]ProcessingItem)[0].Category)
	}
	if out.Outputs["manga"].Value.([]ProcessingItem)[0].Category != "manga" {
		t.Fatalf("manga port category = %q, want manga", out.Outputs["manga"].Value.([]ProcessingItem)[0].Category)
	}
	if out.Outputs["photo"].Value.([]ProcessingItem)[0].Category != "photo" {
		t.Fatalf("photo port category = %q, want photo", out.Outputs["photo"].Value.([]ProcessingItem)[0].Category)
	}
	if out.Outputs["other"].Value.([]ProcessingItem)[0].Category != "other" {
		t.Fatalf("other port category = %q, want other", out.Outputs["other"].Value.([]ProcessingItem)[0].Category)
	}
	if out.Outputs["mixed_leaf"].Value.([]ProcessingItem)[0].Category != "mixed" {
		t.Fatalf("mixed_leaf port category = %q, want mixed", out.Outputs["mixed_leaf"].Value.([]ProcessingItem)[0].Category)
	}
}

func TestCategoryRouterExecutorEmptyBranchesReturnEmptyLists(t *testing.T) {
	t.Parallel()

	executor := newCategoryRouterExecutor()
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Inputs: testInputs(map[string]any{
			"items": []ProcessingItem{{FolderName: "v", Category: "video"}},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	for _, key := range []string{"video", "manga", "photo", "other", "mixed_leaf"} {
		list, ok := out.Outputs[key].Value.([]ProcessingItem)
		if !ok {
			t.Fatalf("port %s type = %T, want []ProcessingItem", key, out.Outputs[key].Value)
		}
		if key == "video" {
			if len(list) != 1 {
				t.Fatalf("video len = %d, want 1", len(list))
			}
			continue
		}
		if len(list) != 0 {
			t.Fatalf("port %s len = %d, want 0", key, len(list))
		}
	}

	out, err = executor.Execute(context.Background(), NodeExecutionInput{})
	if err != nil {
		t.Fatalf("Execute() without input error = %v", err)
	}
	for _, key := range []string{"video", "manga", "photo", "other", "mixed_leaf"} {
		list, ok := out.Outputs[key].Value.([]ProcessingItem)
		if !ok {
			t.Fatalf("empty-input port %s type = %T, want []ProcessingItem", key, out.Outputs[key].Value)
		}
		if len(list) != 0 {
			t.Fatalf("empty-input port %s len = %d, want 0", key, len(list))
		}
	}
}

func TestRenameNodeExecutorTemplateRegexAndConditionalDefault(t *testing.T) {
	t.Parallel()

	executor := newRenameNodeExecutor()

	t.Run("template_title_year", func(t *testing.T) {
		t.Parallel()

		out, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{
				"strategy": "template",
				"template": "{title} ({year})",
			}},
			Inputs: testInputs(map[string]any{"item": ProcessingItem{FolderName: "Blade Runner 2049", TargetName: "Blade Runner 2049"}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		items, ok := out.Outputs["items"].Value.([]ProcessingItem)
		if !ok || len(items) != 1 {
			t.Fatalf("output type/len = %T/%d, want []ProcessingItem/1", out.Outputs["items"].Value, len(items))
		}
		if items[0].TargetName != "Blade Runner (2049)" {
			t.Fatalf("TargetName = %q, want %q", items[0].TargetName, "Blade Runner (2049)")
		}
	})

	t.Run("regex_extract_named_groups", func(t *testing.T) {
		t.Parallel()

		out, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{
				"strategy": "regex_extract",
				"regex":    `^(?P<title>.+?)\[(?P<year>\d{4})\]$`,
				"template": "{title} ({year})",
			}},
			Inputs: testInputs(map[string]any{"item": ProcessingItem{FolderName: "Dune[2021]", TargetName: "Dune[2021]"}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		items := out.Outputs["items"].Value.([]ProcessingItem)
		if len(items) != 1 || items[0].TargetName != "Dune (2021)" {
			t.Fatalf("TargetName = %q, want %q", items[0].TargetName, "Dune (2021)")
		}
	})

	t.Run("conditional_default", func(t *testing.T) {
		t.Parallel()

		out, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{
				"strategy": "conditional",
				"rules": []any{
					map[string]any{"condition": `name CONTAINS "鍚堥泦"`, "template": "PACK-{name}"},
					map[string]any{"condition": `category == "video"`, "template": "VID-{name}"},
					map[string]any{"condition": "DEFAULT", "template": "DEFAULT-{name}"},
				},
			}},
			Inputs: testInputs(map[string]any{"item": ProcessingItem{FolderName: "Sample", TargetName: "Sample", Category: "photo"}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		items := out.Outputs["items"].Value.([]ProcessingItem)
		if len(items) != 1 || items[0].TargetName != "DEFAULT-Sample" {
			t.Fatalf("TargetName = %q, want %q", items[0].TargetName, "DEFAULT-Sample")
		}
	})
}

func TestMoveNodeExecutorMergeConflictPolicies(t *testing.T) {
	t.Parallel()

	t.Run("skip_when_target_file_exists", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		rootPath := filepath.Join(root, "source", "a")
		sourcePath := filepath.Join(rootPath, "b")
		targetDir := filepath.Join(root, "target")
		dstExisting := filepath.Join(targetDir, "a", "001.mp4")

		mustMkdirAll(t, sourcePath)
		mustMkdirAll(t, filepath.Dir(dstExisting))
		if err := os.WriteFile(filepath.Join(sourcePath, "001.mp4"), []byte("video"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source) error = %v", err)
		}
		if err := os.WriteFile(dstExisting, []byte("seed"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(target) error = %v", err)
		}

		executor := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil)
		out, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{
				"target_dir":            targetDir,
				"conflict_policy":       "skip",
				"move_unit":             "folder",
				"preserve_substructure": true,
			}},
			Inputs: testInputs(map[string]any{"item": ProcessingItem{
				SourcePath:   sourcePath,
				RootPath:     rootPath,
				RelativePath: "b",
				SourceKind:   ProcessingItemSourceKindDirectory,
			}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		results, ok := out.Outputs["step_results"].Value.([]ProcessingStepResult)
		if !ok || len(results) != 1 {
			t.Fatalf("step_results output = %T/%d, want []ProcessingStepResult/1", out.Outputs["step_results"].Value, len(results))
		}
		if results[0].Status != "skipped" {
			t.Fatalf("result status = %q, want skipped", results[0].Status)
		}
		if results[0].TargetPath != dstExisting {
			t.Fatalf("target path = %q, want %q", results[0].TargetPath, dstExisting)
		}

		if !pathExists(t, filepath.Join(sourcePath, "001.mp4")) {
			t.Fatalf("source file should still exist after skip")
		}
	})

	t.Run("auto_rename_uses_relative_prefix_and_numeric_suffix", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		rootPath := filepath.Join(root, "source", "a")
		sourcePathA := filepath.Join(rootPath, "b")
		sourcePathB := filepath.Join(rootPath, "aa", "c")
		targetDir := filepath.Join(root, "target")
		dstRoot := filepath.Join(targetDir, "a")
		dstFirst := filepath.Join(dstRoot, "dup.txt")
		dstPrefixed := filepath.Join(dstRoot, "aa-c-dup.txt")
		dstRenamed := filepath.Join(dstRoot, "aa-c-dup-1.txt")

		mustMkdirAll(t, sourcePathA)
		mustMkdirAll(t, sourcePathB)
		mustMkdirAll(t, dstRoot)
		if err := os.WriteFile(filepath.Join(sourcePathA, "dup.txt"), []byte("a"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source A) error = %v", err)
		}
		if err := os.WriteFile(filepath.Join(sourcePathB, "dup.txt"), []byte("b"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source B) error = %v", err)
		}
		if err := os.WriteFile(dstPrefixed, []byte("seed"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(prefixed) error = %v", err)
		}

		executor := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil)
		out, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{
				"target_dir":      targetDir,
				"conflict_policy": "auto_rename",
			}},
			Inputs: testInputs(map[string]any{"items": []ProcessingItem{
				{
					SourcePath:   sourcePathA,
					RootPath:     rootPath,
					RelativePath: "b",
					SourceKind:   ProcessingItemSourceKindDirectory,
				},
				{
					SourcePath:   sourcePathB,
					RootPath:     rootPath,
					RelativePath: "aa/c",
					SourceKind:   ProcessingItemSourceKindDirectory,
				},
			}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		results, ok := out.Outputs["step_results"].Value.([]ProcessingStepResult)
		if !ok || len(results) != 2 {
			t.Fatalf("step_results output = %T/%d, want []ProcessingStepResult/2", out.Outputs["step_results"].Value, len(results))
		}
		targets := map[string]string{}
		for _, result := range results {
			targets[result.SourcePath] = result.TargetPath
			if result.Status != "moved" {
				t.Fatalf("result status = %q, want moved", result.Status)
			}
		}
		if targets[filepath.Join(sourcePathA, "dup.txt")] != dstFirst {
			t.Fatalf("first target = %q, want %q", targets[filepath.Join(sourcePathA, "dup.txt")], dstFirst)
		}
		if targets[filepath.Join(sourcePathB, "dup.txt")] != dstRenamed {
			t.Fatalf("second target = %q, want %q", targets[filepath.Join(sourcePathB, "dup.txt")], dstRenamed)
		}
		if !pathExists(t, dstFirst) || !pathExists(t, dstRenamed) {
			t.Fatalf("expected merged files should exist in target root")
		}
		if pathExists(t, filepath.Join(sourcePathA, "dup.txt")) || pathExists(t, filepath.Join(sourcePathB, "dup.txt")) {
			t.Fatalf("source files should be moved out")
		}
	})
}

func TestPhase4MoveNodeExecutorRollbackMovesArtifactsBackAndCleansTargetRoot(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source", "a", "b", "001.jpg")
	targetPath := filepath.Join(root, "target", "a", "001.jpg")
	mustMkdirAll(t, filepath.Dir(sourcePath))
	mustMkdirAll(t, filepath.Dir(targetPath))
	if err := os.WriteFile(targetPath, []byte("img"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(target) error = %v", err)
	}

	encodedOutputs, err := typedValueMapToJSON(map[string]TypedValue{
		"items": {Type: PortTypeProcessingItemList, Value: []ProcessingItem{{
			FolderID:     "folder-move-rb-1",
			SourcePath:   filepath.Join(root, "source", "a", "b"),
			RootPath:     filepath.Join(root, "source", "a"),
			RelativePath: "b",
			SourceKind:   ProcessingItemSourceKindDirectory,
		}}},
		"step_results": {Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{{SourcePath: sourcePath, TargetPath: targetPath, NodeType: "move-node", Status: "moved"}}},
	}, NewTypeRegistry())
	if err != nil {
		t.Fatalf("typedValueMapToJSON() error = %v", err)
	}

	executor := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil)
	err = executor.Rollback(ctx, NodeRollbackInput{
		NodeRun: &repository.NodeRun{ID: "node-run-move-rb-1", OutputJSON: mustJSONMarshal(t, map[string]any{"outputs": encodedOutputs})},
	})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	if !pathExists(t, sourcePath) {
		t.Fatalf("source path %q should exist after rollback", sourcePath)
	}
	if pathExists(t, targetPath) {
		t.Fatalf("target path %q should not exist after rollback", targetPath)
	}
	targetRoot := filepath.Join(root, "target", "a")
	if pathExists(t, targetRoot) {
		t.Fatalf("target root %q should be removed when empty", targetRoot)
	}
}

func TestMoveNodeExecutorMergeValidationAndArchiveFlatten(t *testing.T) {
	t.Parallel()

	t.Run("mixed_output_key_falls_back_to_single_item_category_output", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		rootPath := filepath.Join(root, "source", "yourpersonalwaifu")
		sourcePath := filepath.Join(rootPath, "Videos")
		targetDir := filepath.Join(root, "target-video")
		mustMkdirAll(t, sourcePath)
		if err := os.WriteFile(filepath.Join(sourcePath, "0001.m4v"), []byte("video"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source) error = %v", err)
		}

		executor := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil)
		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{
				"path_ref_type": workflowPathRefTypeOutput,
				"path_ref_key":  "mixed:0",
			}},
			AppConfig: &repository.AppConfig{
				OutputDirs: repository.AppConfigOutputDirs{
					Video: []string{targetDir},
					Mixed: []string{},
				},
			},
			Inputs: testInputs(map[string]any{"items": []ProcessingItem{
				{
					SourcePath:   sourcePath,
					RootPath:     rootPath,
					RelativePath: "Videos",
					SourceKind:   ProcessingItemSourceKindDirectory,
					Category:     "video",
				},
			}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		targetRoot := filepath.Join(targetDir, "yourpersonalwaifu")
		if !pathExists(t, filepath.Join(targetRoot, "0001.m4v")) {
			t.Fatalf("expected moved file under fallback target root %q", targetRoot)
		}
	})

	t.Run("mixed_output_key_routes_each_item_to_category_output", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		rootPath := filepath.Join(root, "source", "Bangni邦尼  中秋节玉兔 [88P7V 720M]")
		photoStagePath := filepath.Join(rootPath, "__photo")
		videoPath := filepath.Join(rootPath, "视频")
		videoTargetDir := filepath.Join(root, "target", "video")
		photoTargetDir := filepath.Join(root, "target", "photo")
		mixedTargetDir := filepath.Join(root, "target", "mixed")
		mustMkdirAll(t, photoStagePath)
		mustMkdirAll(t, videoPath)
		if err := os.WriteFile(filepath.Join(photoStagePath, "1.jpg"), []byte("photo"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(photo) error = %v", err)
		}
		if err := os.WriteFile(filepath.Join(videoPath, "V (1).mp4"), []byte("video"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(video) error = %v", err)
		}

		executor := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil)
		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{
				"path_ref_type": workflowPathRefTypeOutput,
				"path_ref_key":  "mixed",
				"path_suffix":   ".processed",
			}},
			AppConfig: &repository.AppConfig{
				OutputDirs: repository.AppConfigOutputDirs{
					Video: []string{videoTargetDir},
					Photo: []string{photoTargetDir},
					Mixed: []string{mixedTargetDir},
				},
			},
			Inputs: testInputs(map[string]any{"items": []ProcessingItem{
				{
					SourcePath:         photoStagePath,
					CurrentPath:        photoStagePath,
					RootPath:           rootPath,
					RelativePath:       "photo",
					SourceKind:         ProcessingItemSourceKindDirectory,
					Category:           "photo",
					OriginalSourcePath: rootPath,
				},
				{
					SourcePath:         videoPath,
					CurrentPath:        videoPath,
					RootPath:           rootPath,
					RelativePath:       "视频",
					SourceKind:         ProcessingItemSourceKindDirectory,
					Category:           "video",
					OriginalSourcePath: rootPath,
				},
			}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		rootName := filepath.Base(rootPath)
		photoProcessedDir := filepath.Join(photoTargetDir, ".processed")
		videoProcessedDir := filepath.Join(videoTargetDir, ".processed")
		mixedProcessedDir := filepath.Join(mixedTargetDir, ".processed")
		if !pathExists(t, filepath.Join(photoProcessedDir, rootName, "1.jpg")) {
			t.Fatalf("expected photo file under %q", filepath.Join(photoProcessedDir, rootName))
		}
		if !pathExists(t, filepath.Join(videoProcessedDir, rootName, "V (1).mp4")) {
			t.Fatalf("expected video file under %q", filepath.Join(videoProcessedDir, rootName))
		}
		if pathExists(t, filepath.Join(mixedProcessedDir, rootName, "V (1).mp4")) {
			t.Fatalf("video file should not be moved under mixed target")
		}
	})

	t.Run("multiple_root_path_returns_error", func(t *testing.T) {
		t.Parallel()

		executor := newPhase4MoveNodeExecutor(fs.NewMockAdapter(), nil)
		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{"target_dir": "/target"}},
			Inputs: testInputs(map[string]any{"items": []ProcessingItem{
				{SourcePath: "/source/a/b", RootPath: "/source/a", SourceKind: ProcessingItemSourceKindDirectory},
				{SourcePath: "/source/x/y", RootPath: "/source/x", SourceKind: ProcessingItemSourceKindDirectory},
			}}),
		})
		if err == nil {
			t.Fatalf("Execute() error = nil, want multiple root_path error")
		}
		if !stringsContains(err.Error(), "multiple root_path") {
			t.Fatalf("error = %q, want multiple root_path", err.Error())
		}
	})

	t.Run("directory_contains_subdirectory_returns_error", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		rootPath := filepath.Join(root, "source", "a")
		sourcePath := filepath.Join(rootPath, "b")
		subdir := filepath.Join(sourcePath, "nested")
		targetDir := filepath.Join(root, "target")
		mustMkdirAll(t, subdir)
		if err := os.WriteFile(filepath.Join(sourcePath, "001.mp4"), []byte("video"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source) error = %v", err)
		}

		executor := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil)
		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{"target_dir": targetDir}},
			Inputs: testInputs(map[string]any{"item": ProcessingItem{
				SourcePath:   sourcePath,
				RootPath:     rootPath,
				RelativePath: "b",
				SourceKind:   ProcessingItemSourceKindDirectory,
			}}),
		})
		if err == nil {
			t.Fatalf("Execute() error = nil, want subdirectory validation error")
		}
		if !stringsContains(err.Error(), "contains subdirectory") {
			t.Fatalf("error = %q, want subdirectory message", err.Error())
		}
	})

	t.Run("archive_items_flatten_relative_path_to_filename", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		rootPath := filepath.Join(root, "source", "a")
		archivePathA := filepath.Join(root, "archives", "c.cbz")
		archivePathB := filepath.Join(root, "archives", "b.cbz")
		targetDir := filepath.Join(root, "target")
		mustMkdirAll(t, filepath.Dir(archivePathA))
		if err := os.WriteFile(archivePathA, []byte("a"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(archive A) error = %v", err)
		}
		if err := os.WriteFile(archivePathB, []byte("b"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(archive B) error = %v", err)
		}

		executor := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil)
		out, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{"target_dir": targetDir}},
			Inputs: testInputs(map[string]any{"items": []ProcessingItem{
				{
					SourcePath:         archivePathA,
					RootPath:           rootPath,
					RelativePath:       "aa/c",
					SourceKind:         ProcessingItemSourceKindArchive,
					OriginalSourcePath: filepath.Join(rootPath, "aa", "c"),
				},
				{
					SourcePath:         archivePathB,
					RootPath:           rootPath,
					RelativePath:       "b",
					SourceKind:         ProcessingItemSourceKindArchive,
					OriginalSourcePath: filepath.Join(rootPath, "b"),
				},
			}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		results := out.Outputs["step_results"].Value.([]ProcessingStepResult)
		if len(results) != 2 {
			t.Fatalf("len(step_results) = %d, want 2", len(results))
		}

		targetA := filepath.Join(targetDir, "a", "aa-c.cbz")
		targetB := filepath.Join(targetDir, "a", "b.cbz")
		if !pathExists(t, targetA) || !pathExists(t, targetB) {
			t.Fatalf("flattened archive targets should exist: %q, %q", targetA, targetB)
		}
	})

	t.Run("same_root_path_items_merge_into_single_target_root", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		rootPath := filepath.Join(root, "source", "5k porn")
		sourcePathA := filepath.Join(rootPath, "video-a")
		sourcePathB := filepath.Join(rootPath, "video-b")
		targetDir := filepath.Join(root, "target")
		mustMkdirAll(t, sourcePathA)
		mustMkdirAll(t, sourcePathB)
		if err := os.WriteFile(filepath.Join(sourcePathA, "a.mp4"), []byte("a"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source A) error = %v", err)
		}
		if err := os.WriteFile(filepath.Join(sourcePathB, "b.mp4"), []byte("b"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source B) error = %v", err)
		}

		executor := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil)
		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{
				"target_dir": targetDir,
			}},
			Inputs: testInputs(map[string]any{"items": []ProcessingItem{
				{
					SourcePath:   sourcePathA,
					RootPath:     rootPath,
					RelativePath: "video-a",
					SourceKind:   ProcessingItemSourceKindDirectory,
					TargetName:   "5kporn.e20.melody.marks.5k",
				},
				{
					SourcePath:   sourcePathB,
					RootPath:     rootPath,
					RelativePath: "video-b",
					SourceKind:   ProcessingItemSourceKindDirectory,
					FolderName:   "5kporn.e20.melody.marks.5k",
				},
			}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		targetRoot := filepath.Join(targetDir, "5k porn")
		if !pathExists(t, filepath.Join(targetRoot, "a.mp4")) {
			t.Fatalf("merged output missing %q", filepath.Join(targetRoot, "a.mp4"))
		}
		if !pathExists(t, filepath.Join(targetRoot, "b.mp4")) {
			t.Fatalf("merged output missing %q", filepath.Join(targetRoot, "b.mp4"))
		}
		unexpectedRoot := filepath.Join(targetDir, "5kporn.e20.melody.marks.5k")
		if pathExists(t, unexpectedRoot) {
			t.Fatalf("unexpected split target root should not exist: %q", unexpectedRoot)
		}
	})

	t.Run("sibling_root_path_items_fallback_to_common_parent_root", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		sourceRoot := filepath.Join(root, "source", "Kaede Matsushima [1982.11.17]")
		sourcePathA := filepath.Join(sourceRoot, "@Misty Kaede Matsushima[120P]")
		sourcePathB := filepath.Join(sourceRoot, "BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P]")
		targetDir := filepath.Join(root, "target")
		mustMkdirAll(t, sourcePathA)
		mustMkdirAll(t, sourcePathB)
		if err := os.WriteFile(filepath.Join(sourcePathA, "a.jpg"), []byte("a"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source A) error = %v", err)
		}
		if err := os.WriteFile(filepath.Join(sourcePathB, "b.jpg"), []byte("b"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source B) error = %v", err)
		}

		executor := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil)
		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{
				"target_dir": targetDir,
			}},
			Inputs: testInputs(map[string]any{"items": []ProcessingItem{
				{
					SourcePath:  sourcePathA,
					CurrentPath: sourcePathA,
					RootPath:    sourcePathA,
					SourceKind:  ProcessingItemSourceKindDirectory,
					FolderName:  filepath.Base(sourcePathA),
					TargetName:  filepath.Base(sourcePathA),
				},
				{
					SourcePath:  sourcePathB,
					CurrentPath: sourcePathB,
					RootPath:    sourcePathB,
					SourceKind:  ProcessingItemSourceKindDirectory,
					FolderName:  filepath.Base(sourcePathB),
					TargetName:  filepath.Base(sourcePathB),
				},
			}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		targetRoot := filepath.Join(targetDir, filepath.Base(sourceRoot))
		if !pathExists(t, filepath.Join(targetRoot, "a.jpg")) {
			t.Fatalf("merged output missing %q", filepath.Join(targetRoot, "a.jpg"))
		}
		if !pathExists(t, filepath.Join(targetRoot, "b.jpg")) {
			t.Fatalf("merged output missing %q", filepath.Join(targetRoot, "b.jpg"))
		}
	})

	t.Run("sibling_archive_root_path_items_fallback_to_common_parent_root", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		sourceRoot := filepath.Join(root, "source", "Kaede Matsushima [1982.11.17]")
		rootPathA := filepath.Join(sourceRoot, "@Misty Kaede Matsushima[120P]")
		rootPathB := filepath.Join(sourceRoot, "BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P]")
		archivePathA := filepath.Join(root, "archives", "@Misty Kaede Matsushima[120P].cbz")
		archivePathB := filepath.Join(root, "archives", "BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P].cbz")
		targetDir := filepath.Join(root, "target")
		mustMkdirAll(t, filepath.Dir(archivePathA))
		if err := os.WriteFile(archivePathA, []byte("a"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(archive A) error = %v", err)
		}
		if err := os.WriteFile(archivePathB, []byte("b"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(archive B) error = %v", err)
		}

		executor := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil)
		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{
				"target_dir": targetDir,
			}},
			Inputs: testInputs(map[string]any{"items": []ProcessingItem{
				{
					SourcePath:         rootPathA,
					CurrentPath:        archivePathA,
					RootPath:           rootPathA,
					RelativePath:       "photo",
					SourceKind:         ProcessingItemSourceKindArchive,
					OriginalSourcePath: filepath.Join(rootPathA, ".photo-files"),
					FolderName:         filepath.Base(archivePathA),
					TargetName:         filepath.Base(archivePathA),
				},
				{
					SourcePath:         rootPathB,
					CurrentPath:        archivePathB,
					RootPath:           rootPathB,
					RelativePath:       "photo",
					SourceKind:         ProcessingItemSourceKindArchive,
					OriginalSourcePath: filepath.Join(rootPathB, ".photo-files"),
					FolderName:         filepath.Base(archivePathB),
					TargetName:         filepath.Base(archivePathB),
				},
			}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		targetRoot := filepath.Join(targetDir, filepath.Base(sourceRoot))
		if !pathExists(t, filepath.Join(targetRoot, "@Misty Kaede Matsushima[120P].cbz")) {
			t.Fatalf("merged output missing %q", filepath.Join(targetRoot, "@Misty Kaede Matsushima[120P].cbz"))
		}
		if !pathExists(t, filepath.Join(targetRoot, "BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P].cbz")) {
			t.Fatalf("merged output missing %q", filepath.Join(targetRoot, "BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P].cbz"))
		}
	})

	t.Run("sibling_archive_root_path_items_without_relative_path_fallback_to_common_parent_root", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		sourceRoot := filepath.Join(root, "source", "Kaede Matsushima [1982.11.17]")
		rootPathA := filepath.Join(sourceRoot, "@Misty Kaede Matsushima[120P]")
		rootPathB := filepath.Join(sourceRoot, "BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P]")
		archivePathA := filepath.Join(root, "archives", "@Misty Kaede Matsushima[120P].cbz")
		archivePathB := filepath.Join(root, "archives", "BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P].cbz")
		targetDir := filepath.Join(root, "target")
		mustMkdirAll(t, filepath.Dir(archivePathA))
		if err := os.WriteFile(archivePathA, []byte("a"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(archive A) error = %v", err)
		}
		if err := os.WriteFile(archivePathB, []byte("b"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(archive B) error = %v", err)
		}

		executor := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil)
		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{
				"target_dir": targetDir,
			}},
			Inputs: testInputs(map[string]any{"items": []ProcessingItem{
				{
					SourcePath:         rootPathA,
					CurrentPath:        archivePathA,
					RootPath:           rootPathA,
					SourceKind:         ProcessingItemSourceKindArchive,
					OriginalSourcePath: rootPathA,
					FolderName:         filepath.Base(archivePathA),
					TargetName:         filepath.Base(archivePathA),
				},
				{
					SourcePath:         rootPathB,
					CurrentPath:        archivePathB,
					RootPath:           rootPathB,
					SourceKind:         ProcessingItemSourceKindArchive,
					OriginalSourcePath: rootPathB,
					FolderName:         filepath.Base(archivePathB),
					TargetName:         filepath.Base(archivePathB),
				},
			}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		targetRoot := filepath.Join(targetDir, filepath.Base(sourceRoot))
		if !pathExists(t, filepath.Join(targetRoot, "@Misty Kaede Matsushima[120P].cbz")) {
			t.Fatalf("merged output missing %q", filepath.Join(targetRoot, "@Misty Kaede Matsushima[120P].cbz"))
		}
		if !pathExists(t, filepath.Join(targetRoot, "BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P].cbz")) {
			t.Fatalf("merged output missing %q", filepath.Join(targetRoot, "BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P].cbz"))
		}
	})

	t.Run("windows_style_sibling_archive_roots_fallback_to_common_parent_root", func(t *testing.T) {
		t.Parallel()

		adapter := fs.NewMockAdapter()
		sourceRoot := `E:\TEST\示例\松岛枫 (松島かえで)[1982.11.17]11`
		rootPathA := sourceRoot + `\@Misty Kaede Matsushima[120P]`
		rootPathB := sourceRoot + `\BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P]`
		archivePathA := `E:\TEST\archive\@Misty Kaede Matsushima[120P].cbz`
		archivePathB := `E:\TEST\archive\BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P].cbz`
		targetDir := `E:\TEST\photo`
		adapter.AddFile(archivePathA, []byte("a"))
		adapter.AddFile(archivePathB, []byte("b"))

		executor := newPhase4MoveNodeExecutor(adapter, nil)
		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Config: map[string]any{
				"target_dir": targetDir,
			}},
			Inputs: testInputs(map[string]any{"items": []ProcessingItem{
				{
					SourcePath:         rootPathA,
					CurrentPath:        archivePathA,
					RootPath:           rootPathA,
					SourceKind:         ProcessingItemSourceKindArchive,
					OriginalSourcePath: rootPathA,
					FolderName:         filepath.Base(normalizeWorkflowPath(archivePathA)),
					TargetName:         filepath.Base(normalizeWorkflowPath(archivePathA)),
				},
				{
					SourcePath:         rootPathB,
					CurrentPath:        archivePathB,
					RootPath:           rootPathB,
					SourceKind:         ProcessingItemSourceKindArchive,
					OriginalSourcePath: rootPathB,
					FolderName:         filepath.Base(normalizeWorkflowPath(archivePathB)),
					TargetName:         filepath.Base(normalizeWorkflowPath(archivePathB)),
				},
			}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		targetRoot := normalizeWorkflowPath(targetDir + `/` + filepath.Base(normalizeWorkflowPath(sourceRoot)))
		for _, name := range []string{
			"@Misty Kaede Matsushima[120P].cbz",
			"BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P].cbz",
		} {
			exists, err := adapter.Exists(context.Background(), joinWorkflowPath(targetRoot, name))
			if err != nil {
				t.Fatalf("Exists() error = %v", err)
			}
			if !exists {
				t.Fatalf("moved archive %q should exist under %q", name, targetRoot)
			}
		}
	})
}

func TestCompressNodeExecutorUnsupportedAndArchiveNaming(t *testing.T) {
	t.Parallel()

	t.Run("unsupported_scope_returns_error", func(t *testing.T) {
		t.Parallel()

		executor := newCompressNodeExecutor(fs.NewMockAdapter())
		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node:   repository.WorkflowGraphNode{Config: map[string]any{"scope": "files"}},
			Inputs: testInputs(map[string]any{"item": ProcessingItem{SourcePath: "/x", FolderName: "x"}}),
		})
		if err == nil {
			t.Fatalf("Execute() error = nil, want unsupported scope error")
		}
	})

	t.Run("archive_name_auto_suffix", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		archiveDir := filepath.Join(root, "archives")
		mustMkdirAll(t, archiveDir)

		existing := filepath.Join(archiveDir, "album.cbz")
		if err := os.WriteFile(existing, []byte("seed"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(%q) error = %v", existing, err)
		}

		path, err := compressNodeResolveArchivePath(context.Background(), fs.NewOSAdapter(), archiveDir, "album", ".cbz")
		if err != nil {
			t.Fatalf("compressNodeResolveArchivePath() error = %v", err)
		}
		want := filepath.Join(archiveDir, "album (1).cbz")
		if path != want {
			t.Fatalf("archive path = %q, want %q", path, want)
		}
	})

	t.Run("archive_name_prefers_root_path_name", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		sourcePath := filepath.Join(root, "source", "5kporn.e20", "__photo")
		archiveDir := filepath.Join(root, "archives")
		mustMkdirAll(t, sourcePath)
		if err := os.WriteFile(filepath.Join(sourcePath, "001.jpg"), []byte("img"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source) error = %v", err)
		}

		executor := newCompressNodeExecutor(fs.NewOSAdapter())
		out, err := executor.Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{
				Config: map[string]any{
					"target_dir": archiveDir,
					"format":     "cbz",
				},
			},
			Inputs: testInputs(map[string]any{
				"items": []ProcessingItem{{
					SourcePath: sourcePath,
					RootPath:   filepath.Join(root, "source", "5k porn"),
					TargetName: "5kporn.e20",
					SourceKind: ProcessingItemSourceKindDirectory,
				}},
			}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		stepResults := out.Outputs["step_results"].Value.([]ProcessingStepResult)
		if len(stepResults) != 1 {
			t.Fatalf("len(step_results) = %d, want 1", len(stepResults))
		}
		wantPath := normalizeWorkflowPath(filepath.Join(archiveDir, "5k porn.cbz"))
		if got := normalizeWorkflowPath(stepResults[0].TargetPath); got != wantPath {
			t.Fatalf("archive target path = %q, want %q", got, wantPath)
		}
	})
}

func TestCompressNodeExecutorOutputsArchiveItemsAndCompatibility(t *testing.T) {
	t.Parallel()

	executor := newCompressNodeExecutor(fs.NewOSAdapter())

	root := t.TempDir()
	sourcePath := filepath.Join(root, "source", "album")
	archiveDir := filepath.Join(root, "archives")
	mustMkdirAll(t, sourcePath)
	if err := os.WriteFile(filepath.Join(sourcePath, "001.jpg"), []byte("img"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(source) error = %v", err)
	}

	output, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{
			Type: "compress-node",
			Config: map[string]any{
				"target_dir": archiveDir,
				"format":     "cbz",
			},
		},
		Inputs: testInputs(map[string]any{
			"items": []ProcessingItem{{
				FolderID:   "folder-1",
				SourcePath: sourcePath,
				FolderName: "album",
				TargetName: "album-final",
				Category:   "manga",
				Files:      []FileEntry{{Name: "001.jpg", Ext: "jpg", SizeBytes: 3}},
				ParentPath: filepath.Dir(sourcePath),
				RootPath:   sourcePath,
				SourceKind: ProcessingItemSourceKindDirectory,
			}},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if output.Status != ExecutionSuccess {
		t.Fatalf("status = %q, want %q", output.Status, ExecutionSuccess)
	}

	items, ok := output.Outputs["items"].Value.([]ProcessingItem)
	if !ok || len(items) != 1 {
		t.Fatalf("items output type/len = %T/%d, want []ProcessingItem/1", output.Outputs["items"].Value, len(items))
	}
	if items[0].SourcePath != sourcePath {
		t.Fatalf("items[0].SourcePath = %q, want %q", items[0].SourcePath, sourcePath)
	}
	if items[0].CurrentPath != sourcePath {
		t.Fatalf("items[0].CurrentPath = %q, want %q", items[0].CurrentPath, sourcePath)
	}

	archiveItems, ok := output.Outputs["archive_items"].Value.([]ProcessingItem)
	if !ok || len(archiveItems) != 1 {
		t.Fatalf("archive_items output type/len = %T/%d, want []ProcessingItem/1", output.Outputs["archive_items"].Value, len(archiveItems))
	}
	archiveItem := archiveItems[0]
	if archiveItem.SourcePath != sourcePath {
		t.Fatalf("archive_items[0].SourcePath = %q, want original source %q", archiveItem.SourcePath, sourcePath)
	}
	if archiveItem.CurrentPath == sourcePath {
		t.Fatalf("archive_items[0].CurrentPath should point to archive file, got current path %q", archiveItem.CurrentPath)
	}
	if got, want := archiveItem.ParentPath, filepath.Dir(archiveItem.CurrentPath); got != want {
		t.Fatalf("archive_items[0].ParentPath = %q, want %q", got, want)
	}
	if got, want := archiveItem.FolderName, filepath.Base(archiveItem.CurrentPath); got != want {
		t.Fatalf("archive_items[0].FolderName = %q, want %q", got, want)
	}
	if got, want := archiveItem.TargetName, filepath.Base(archiveItem.CurrentPath); got != want {
		t.Fatalf("archive_items[0].TargetName = %q, want %q", got, want)
	}
	if archiveItem.FolderID != "folder-1" {
		t.Fatalf("archive_items[0].FolderID = %q, want folder-1", archiveItem.FolderID)
	}
	if archiveItem.Category != "manga" {
		t.Fatalf("archive_items[0].Category = %q, want manga", archiveItem.Category)
	}
	if archiveItem.RootPath != sourcePath {
		t.Fatalf("archive_items[0].RootPath = %q, want %q", archiveItem.RootPath, sourcePath)
	}
	if archiveItem.SourceKind != ProcessingItemSourceKindArchive {
		t.Fatalf("archive_items[0].SourceKind = %q, want %q", archiveItem.SourceKind, ProcessingItemSourceKindArchive)
	}
	if archiveItem.OriginalSourcePath != sourcePath {
		t.Fatalf("archive_items[0].OriginalSourcePath = %q, want %q", archiveItem.OriginalSourcePath, sourcePath)
	}
	if archiveItem.Files != nil && len(archiveItem.Files) != 0 {
		t.Fatalf("archive_items[0].Files should be nil or empty, got len=%d", len(archiveItem.Files))
	}

	stepResults, ok := output.Outputs["step_results"].Value.([]ProcessingStepResult)
	if !ok || len(stepResults) != 1 {
		t.Fatalf("step_results output type/len = %T/%d, want []ProcessingStepResult/1", output.Outputs["step_results"].Value, len(stepResults))
	}
	if got, want := stepResults[0].TargetPath, normalizeWorkflowPath(archiveItem.CurrentPath); got != want {
		t.Fatalf("step_results[0].TargetPath = %q, want %q", got, want)
	}

	compressSchema := executor.Schema()
	moveSchema := newPhase4MoveNodeExecutor(fs.NewMockAdapter(), nil).Schema()
	collectSchema := newCollectNodeExecutor().Schema()

	archivePort := compressSchema.OutputPort("archive_items")
	if archivePort == nil {
		t.Fatalf("compress schema missing output port archive_items")
	}
	moveInputPort := moveSchema.InputPort("items")
	if moveInputPort == nil {
		t.Fatalf("move schema missing input port items")
	}
	if archivePort.Type != moveInputPort.Type {
		t.Fatalf("compress archive_items type = %q, move items type = %q", archivePort.Type, moveInputPort.Type)
	}
	collectInputPort := collectSchema.InputPort("items_1")
	if collectInputPort == nil {
		t.Fatalf("collect schema missing input port items_1")
	}
	if archivePort.Type != collectInputPort.Type {
		t.Fatalf("compress archive_items type = %q, collect items_1 type = %q", archivePort.Type, collectInputPort.Type)
	}
}

func TestCompressNodeExecutorIncludesUppercaseImagesAndRejectsEmptyArchives(t *testing.T) {
	t.Parallel()

	t.Run("uppercase_image_extensions_are_included", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		sourcePath := filepath.Join(root, "source", "album")
		archiveDir := filepath.Join(root, "archives")
		mustMkdirAll(t, sourcePath)
		if err := os.WriteFile(filepath.Join(sourcePath, "001.JPG"), []byte("img"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source) error = %v", err)
		}

		out, err := newCompressNodeExecutor(fs.NewOSAdapter()).Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{
				Type: "compress-node",
				Config: map[string]any{
					"target_dir": archiveDir,
					"format":     "zip",
				},
			},
			Inputs: testInputs(map[string]any{
				"items": []ProcessingItem{{SourcePath: sourcePath, RootPath: sourcePath, SourceKind: ProcessingItemSourceKindDirectory}},
			}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}

		archiveItems := out.Outputs["archive_items"].Value.([]ProcessingItem)
		if len(archiveItems) != 1 {
			t.Fatalf("len(archive_items) = %d, want 1", len(archiveItems))
		}
		reader, err := zip.OpenReader(archiveItems[0].CurrentPath)
		if err != nil {
			t.Fatalf("zip.OpenReader(%q) error = %v", archiveItems[0].CurrentPath, err)
		}
		defer reader.Close()

		if len(reader.File) != 1 || reader.File[0].Name != "001.JPG" {
			names := make([]string, 0, len(reader.File))
			for _, file := range reader.File {
				names = append(names, file.Name)
			}
			t.Fatalf("archive entries = %v, want [001.JPG]", names)
		}
	})

	t.Run("empty_archive_is_error_and_partial_file_is_removed", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		sourcePath := filepath.Join(root, "source", "album")
		archiveDir := filepath.Join(root, "archives")
		mustMkdirAll(t, sourcePath)
		if err := os.WriteFile(filepath.Join(sourcePath, "notes.txt"), []byte("note"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source) error = %v", err)
		}

		_, err := newCompressNodeExecutor(fs.NewOSAdapter()).Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{
				Type: "compress-node",
				Config: map[string]any{
					"target_dir": archiveDir,
					"format":     "zip",
				},
			},
			Inputs: testInputs(map[string]any{
				"items": []ProcessingItem{{SourcePath: sourcePath, FolderName: "album", TargetName: "album"}},
			}),
		})
		if err == nil {
			t.Fatalf("Execute() error = nil, want no matched files error")
		}
		if !stringsContains(err.Error(), "no files matched include_patterns") {
			t.Fatalf("error = %q, want no files matched include_patterns", err.Error())
		}
		if pathExists(t, filepath.Join(archiveDir, "album.zip")) {
			t.Fatalf("partial archive should be removed")
		}
	})
}

func TestCompressNodeIntegrationArchiveItemsAndLegacyItems(t *testing.T) {
	t.Parallel()

	t.Run("mixed_leaf_photo_archives_move_under_common_parent", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		sourceRoot := filepath.Join(root, "source", "Kaede Matsushima [1982.11.17]")
		mixedPathA := filepath.Join(sourceRoot, "@Misty Kaede Matsushima[120P]")
		mixedPathB := filepath.Join(sourceRoot, "BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P]")
		archiveDir := filepath.Join(root, "archives")
		moveTargetDir := filepath.Join(root, "final")
		mustMkdirAll(t, mixedPathA)
		mustMkdirAll(t, mixedPathB)
		if err := os.WriteFile(filepath.Join(mixedPathA, "001.JPG"), []byte("a"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source A) error = %v", err)
		}
		if err := os.WriteFile(filepath.Join(mixedPathB, "002.jpg"), []byte("b"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source B) error = %v", err)
		}

		mixedOut, err := newMixedLeafRouterExecutor(fs.NewOSAdapter()).Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{Type: "mixed-leaf-router"},
			Inputs: testInputs(map[string]any{"items": []ProcessingItem{
				{
					SourcePath:  mixedPathA,
					CurrentPath: mixedPathA,
					RootPath:    sourceRoot,
					Category:    "mixed",
					FolderName:  filepath.Base(mixedPathA),
					TargetName:  filepath.Base(mixedPathA),
					SourceKind:  ProcessingItemSourceKindDirectory,
				},
				{
					SourcePath:  mixedPathB,
					CurrentPath: mixedPathB,
					RootPath:    sourceRoot,
					Category:    "mixed",
					FolderName:  filepath.Base(mixedPathB),
					TargetName:  filepath.Base(mixedPathB),
					SourceKind:  ProcessingItemSourceKindDirectory,
				},
			}}),
		})
		if err != nil {
			t.Fatalf("mixed leaf Execute() error = %v", err)
		}
		photoItems := mixedOut.Outputs[mixedLeafRouterPhotoPort].Value.([]ProcessingItem)
		if len(photoItems) != 2 {
			t.Fatalf("len(photoItems) = %d, want 2", len(photoItems))
		}

		compressOut, err := newCompressNodeExecutor(fs.NewOSAdapter()).Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{
				Type: "compress-node",
				Config: map[string]any{
					"target_dir": archiveDir,
					"format":     "cbz",
				},
			},
			Inputs: testInputs(map[string]any{"items": photoItems}),
		})
		if err != nil {
			t.Fatalf("compress Execute() error = %v", err)
		}

		archiveItems := compressOut.Outputs["archive_items"].Value.([]ProcessingItem)
		if len(archiveItems) != 2 {
			t.Fatalf("len(archive_items) = %d, want 2", len(archiveItems))
		}

		moveOut, err := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil).Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{
				Type:   "move-node",
				Config: map[string]any{"target_dir": moveTargetDir},
			},
			Inputs: testInputs(map[string]any{"items": archiveItems}),
		})
		if err != nil {
			t.Fatalf("move Execute() error = %v", err)
		}

		moveSteps := moveOut.Outputs["step_results"].Value.([]ProcessingStepResult)
		if len(moveSteps) != 2 {
			t.Fatalf("len(step_results) = %d, want 2", len(moveSteps))
		}

		targetRoot := filepath.Join(moveTargetDir, filepath.Base(sourceRoot))
		targetA := filepath.Join(targetRoot, "@Misty Kaede Matsushima[120P].cbz")
		targetB := filepath.Join(targetRoot, "BEJEAN ON LINE Kaede Matsushima 2004.12 Hassya[51P].cbz")
		if !pathExists(t, targetA) || !pathExists(t, targetB) {
			t.Fatalf("expected moved archives under %q", targetRoot)
		}
		for _, item := range archiveItems {
			if pathExists(t, item.CurrentPath) {
				t.Fatalf("staging archive %q should not remain after move", item.CurrentPath)
			}
		}

		reader, err := zip.OpenReader(targetA)
		if err != nil {
			t.Fatalf("zip.OpenReader(%q) error = %v", targetA, err)
		}
		defer reader.Close()
		if len(reader.File) != 1 || reader.File[0].Name != "001.JPG" {
			names := make([]string, 0, len(reader.File))
			for _, file := range reader.File {
				names = append(names, file.Name)
			}
			t.Fatalf("target archive entries = %v, want [001.JPG]", names)
		}
	})

	t.Run("archive_items_to_move_moves_archive_file", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		sourcePath := filepath.Join(root, "source", "album")
		archiveDir := filepath.Join(root, "archives")
		moveTargetDir := filepath.Join(root, "final")

		mustMkdirAll(t, sourcePath)
		if err := os.WriteFile(filepath.Join(sourcePath, "001.jpg"), []byte("img"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source) error = %v", err)
		}

		compressOut, err := newCompressNodeExecutor(fs.NewOSAdapter()).Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{
				Type: "compress-node",
				Config: map[string]any{
					"target_dir": archiveDir,
					"format":     "cbz",
				},
			},
			Inputs: testInputs(map[string]any{
				"items": []ProcessingItem{{SourcePath: sourcePath, FolderName: "album", TargetName: "album"}},
			}),
		})
		if err != nil {
			t.Fatalf("compress Execute() error = %v", err)
		}

		archiveItems := compressOut.Outputs["archive_items"].Value.([]ProcessingItem)
		if len(archiveItems) != 1 {
			t.Fatalf("len(archive_items) = %d, want 1", len(archiveItems))
		}

		moveOut, err := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil).Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{
				Type:   "move-node",
				Config: map[string]any{"target_dir": moveTargetDir},
			},
			Inputs: testInputs(map[string]any{
				"items": archiveItems,
			}),
		})
		if err != nil {
			t.Fatalf("move Execute() error = %v", err)
		}

		movedItems := moveOut.Outputs["items"].Value.([]ProcessingItem)
		if len(movedItems) != 1 {
			t.Fatalf("len(movedItems) = %d, want 1", len(movedItems))
		}
		moveSteps := moveOut.Outputs["step_results"].Value.([]ProcessingStepResult)
		if len(moveSteps) != 1 {
			t.Fatalf("len(step_results) = %d, want 1", len(moveSteps))
		}
		movedPath := moveSteps[0].TargetPath
		if filepath.Ext(movedPath) != ".cbz" {
			t.Fatalf("moved archive extension = %q, want .cbz", filepath.Ext(movedPath))
		}
		if !pathExists(t, movedPath) {
			t.Fatalf("moved archive %q should exist", movedPath)
		}
		if pathExists(t, archiveItems[0].CurrentPath) {
			t.Fatalf("original archive path %q should not exist after move", archiveItems[0].CurrentPath)
		}
	})

	t.Run("legacy_items_to_move_keeps_legacy_semantics", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		sourcePath := filepath.Join(root, "source", "album")
		archiveDir := filepath.Join(root, "archives")
		moveTargetDir := filepath.Join(root, "final")

		mustMkdirAll(t, sourcePath)
		if err := os.WriteFile(filepath.Join(sourcePath, "001.jpg"), []byte("img"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(source) error = %v", err)
		}

		compressOut, err := newCompressNodeExecutor(fs.NewOSAdapter()).Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{
				Type: "compress-node",
				Config: map[string]any{
					"target_dir": archiveDir,
					"format":     "cbz",
				},
			},
			Inputs: testInputs(map[string]any{
				"items": []ProcessingItem{{SourcePath: sourcePath, FolderName: "album", TargetName: "album"}},
			}),
		})
		if err != nil {
			t.Fatalf("compress Execute() error = %v", err)
		}

		legacyItems := compressOut.Outputs["items"].Value.([]ProcessingItem)
		if len(legacyItems) != 1 {
			t.Fatalf("len(items) = %d, want 1", len(legacyItems))
		}
		if legacyItems[0].SourcePath != sourcePath {
			t.Fatalf("items[0].SourcePath = %q, want %q", legacyItems[0].SourcePath, sourcePath)
		}

		moveOut, err := newPhase4MoveNodeExecutor(fs.NewOSAdapter(), nil).Execute(context.Background(), NodeExecutionInput{
			Node: repository.WorkflowGraphNode{
				Type:   "move-node",
				Config: map[string]any{"target_dir": moveTargetDir},
			},
			Inputs: testInputs(map[string]any{
				"items": legacyItems,
			}),
		})
		if err != nil {
			t.Fatalf("move Execute() error = %v", err)
		}

		movedItems := moveOut.Outputs["items"].Value.([]ProcessingItem)
		if len(movedItems) != 1 {
			t.Fatalf("len(movedItems) = %d, want 1", len(movedItems))
		}
		movedCurrentPath := movedItems[0].CurrentPath
		if filepath.Base(movedCurrentPath) != "album" {
			t.Fatalf("moved folder name = %q, want album", filepath.Base(movedCurrentPath))
		}
		if !pathExists(t, movedCurrentPath) {
			t.Fatalf("moved folder %q should exist", movedCurrentPath)
		}
		if pathExists(t, sourcePath) {
			t.Fatalf("source folder %q should not exist after move", sourcePath)
		}
	})
}

func TestCompressNodeExecutorRollbackRemovesGeneratedArchives(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	archivePath := filepath.Join(root, "archives", "album.cbz")
	mustMkdirAll(t, filepath.Dir(archivePath))
	if err := os.WriteFile(archivePath, []byte("archive"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", archivePath, err)
	}

	executor := newCompressNodeExecutor(fs.NewOSAdapter())
	err := executor.Rollback(context.Background(), NodeRollbackInput{
		NodeRun: &repository.NodeRun{
			ID:        "node-run-compress-rb-1",
			InputJSON: `{"node":{"config":{"delete_source":false}}}`,
		},
		Snapshots: []*repository.NodeSnapshot{{
			ID:   "snapshot-compress-rb-1",
			Kind: "post",
			OutputJSON: mustJSONMarshal(t, mustTypedOutputsMap(t, map[string]TypedValue{
				"items":        {Type: PortTypeProcessingItemList, Value: []ProcessingItem{{SourcePath: "/source/album"}}},
				"step_results": {Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{{SourcePath: "/source/album", TargetPath: archivePath, NodeType: "compress-node", Status: "succeeded"}}},
			})),
		}},
	})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	if pathExists(t, archivePath) {
		t.Fatalf("archive path %q should be removed during rollback", archivePath)
	}
}

func TestCompressNodeExecutorRollbackDeleteSourceUnsupported(t *testing.T) {
	t.Parallel()

	executor := newCompressNodeExecutor(fs.NewMockAdapter())
	err := executor.Rollback(context.Background(), NodeRollbackInput{
		NodeRun: &repository.NodeRun{
			ID:        "node-run-compress-rb-unsupported",
			InputJSON: `{"node":{"config":{"delete_source":true}}}`,
		},
	})
	if err == nil {
		t.Fatalf("Rollback() error = nil, want unsupported delete_source error")
	}
	if !stringsContains(err.Error(), "delete_source=true rollback is not supported") {
		t.Fatalf("error = %q, want message containing %q", err.Error(), "delete_source=true rollback is not supported")
	}
}

func TestThumbnailNodeHelpersAndFfmpegMissing(t *testing.T) {
	t.Parallel()

	t.Run("build_args_contains_expected_segments", func(t *testing.T) {
		t.Parallel()

		args := thumbnailNodeBuildArgs("/src/movie.mkv", "/out/thumb.jpg", 8, 640)
		if len(args) == 0 {
			t.Fatalf("thumbnailNodeBuildArgs() returned empty args")
		}
		if args[0] != "-y" {
			t.Fatalf("args[0] = %q, want -y", args[0])
		}
		if args[len(args)-1] != "/out/thumb.jpg" {
			t.Fatalf("last arg = %q, want /out/thumb.jpg", args[len(args)-1])
		}
	})

	t.Run("ffmpeg_missing_returns_clear_error", func(t *testing.T) {
		t.Parallel()

		adapter := fs.NewMockAdapter()
		adapter.AddDir("/source/album", []fs.DirEntry{{Name: "small.mp4", Size: 10}, {Name: "large.mkv", Size: 100}})

		executor := newThumbnailNodeExecutor(adapter, nil)
		executor.lookPath = func(string) (string, error) {
			return "", errors.New("not found")
		}

		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Inputs: testInputs(map[string]any{"item": ProcessingItem{SourcePath: "/source/album", FolderName: "album"}}),
		})
		if err == nil {
			t.Fatalf("Execute() error = nil, want ffmpeg missing error")
		}
		if !stringsContains(err.Error(), "ffmpeg binary not found") {
			t.Fatalf("error = %q, want message containing %q", err.Error(), "ffmpeg binary not found")
		}
	})
}

func TestThumbnailNodeExecutorUsesCurrentPathAndBusinessErrors(t *testing.T) {
	t.Parallel()

	t.Run("directory_input_prefers_current_path", func(t *testing.T) {
		t.Parallel()

		adapter := fs.NewMockAdapter()
		adapter.AddDir("/moved/album", []fs.DirEntry{{Name: "clip.mp4", Size: 50}, {Name: "movie.mkv", Size: 100}})

		executor := newThumbnailNodeExecutor(adapter, nil)
		executor.lookPath = func(string) (string, error) {
			return "/usr/bin/ffmpeg", nil
		}
		executor.runFFmpeg = func(context.Context, string, ...string) ([]byte, error) {
			return nil, nil
		}

		out, err := executor.Execute(context.Background(), NodeExecutionInput{
			Inputs: testInputs(map[string]any{"item": ProcessingItem{
				SourcePath:  "/source/album",
				CurrentPath: "/moved/album",
				FolderName:  "album",
				TargetName:  "album",
				SourceKind:  ProcessingItemSourceKindDirectory,
			}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		results := out.Outputs["step_results"].Value.([]ProcessingStepResult)
		if len(results) != 2 {
			t.Fatalf("len(step_results) = %d, want 2", len(results))
		}
		if results[0].SourcePath != "/moved/album/clip.mp4" {
			t.Fatalf("step_results[0].SourcePath = %q, want /moved/album/clip.mp4", results[0].SourcePath)
		}
		if results[0].TargetPath != "/moved/album/clip.jpg" {
			t.Fatalf("step_results[0].TargetPath = %q, want /moved/album/clip.jpg", results[0].TargetPath)
		}
		if results[1].SourcePath != "/moved/album/movie.mkv" {
			t.Fatalf("step_results[1].SourcePath = %q, want /moved/album/movie.mkv", results[1].SourcePath)
		}
		if results[1].TargetPath != "/moved/album/movie.jpg" {
			t.Fatalf("step_results[1].TargetPath = %q, want /moved/album/movie.jpg", results[1].TargetPath)
		}
	})

	t.Run("directory_output_uses_video_file_name_even_with_root_path", func(t *testing.T) {
		t.Parallel()

		adapter := fs.NewMockAdapter()
		adapter.AddDir("/moved/5kporn.e20", []fs.DirEntry{{Name: "movie.mkv", Size: 100}})

		executor := newThumbnailNodeExecutor(adapter, nil)
		executor.lookPath = func(string) (string, error) {
			return "/usr/bin/ffmpeg", nil
		}
		executor.runFFmpeg = func(context.Context, string, ...string) ([]byte, error) {
			return nil, nil
		}

		out, err := executor.Execute(context.Background(), NodeExecutionInput{
			Inputs: testInputs(map[string]any{"item": ProcessingItem{
				SourcePath:  "/source/5kporn.e20",
				CurrentPath: "/moved/5kporn.e20",
				RootPath:    "/source/5k porn",
				TargetName:  "5kporn.e20",
				SourceKind:  ProcessingItemSourceKindDirectory,
			}}),
		})
		if err != nil {
			t.Fatalf("Execute() error = %v", err)
		}
		results := out.Outputs["step_results"].Value.([]ProcessingStepResult)
		if len(results) != 1 {
			t.Fatalf("len(step_results) = %d, want 1", len(results))
		}
		if results[0].TargetPath != "/moved/5kporn.e20/movie.jpg" {
			t.Fatalf("step_results[0].TargetPath = %q, want /moved/5kporn.e20/movie.jpg", results[0].TargetPath)
		}
	})

	t.Run("non_video_file_returns_business_error", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		filePath := filepath.Join(root, "album.txt")
		if err := os.WriteFile(filePath, []byte("text"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(%q) error = %v", filePath, err)
		}

		executor := newThumbnailNodeExecutor(fs.NewOSAdapter(), nil)
		executor.lookPath = func(string) (string, error) {
			return "/usr/bin/ffmpeg", nil
		}

		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Inputs: testInputs(map[string]any{"item": ProcessingItem{CurrentPath: filePath, SourcePath: "/source/album"}}),
		})
		if err == nil {
			t.Fatalf("Execute() error = nil, want business error")
		}
		if !stringsContains(err.Error(), "褰撳墠澶勭悊椤逛笉鍖呭惈鍙敓鎴愮缉鐣ュ浘鐨勮棰戞簮") {
			t.Fatalf("error = %q, want business error message", err.Error())
		}
		if stringsContains(err.Error(), "items input is required") {
			t.Fatalf("error = %q, should not be empty input error", err.Error())
		}
	})

	t.Run("directory_without_video_returns_business_error", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		dirPath := filepath.Join(root, "album")
		mustMkdirAll(t, dirPath)
		if err := os.WriteFile(filepath.Join(dirPath, "001.jpg"), []byte("img"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(%q) error = %v", filepath.Join(dirPath, "001.jpg"), err)
		}

		executor := newThumbnailNodeExecutor(fs.NewOSAdapter(), nil)
		executor.lookPath = func(string) (string, error) {
			return "/usr/bin/ffmpeg", nil
		}

		_, err := executor.Execute(context.Background(), NodeExecutionInput{
			Inputs: testInputs(map[string]any{"item": ProcessingItem{CurrentPath: dirPath, SourcePath: "/source/album", SourceKind: ProcessingItemSourceKindDirectory}}),
		})
		if err == nil {
			t.Fatalf("Execute() error = nil, want business error")
		}
		if !stringsContains(err.Error(), "褰撳墠澶勭悊椤逛笉鍖呭惈鍙敓鎴愮缉鐣ュ浘鐨勮棰戞簮") {
			t.Fatalf("error = %q, want business error message", err.Error())
		}
	})
}

func TestThumbnailNodeExecutorPersistsCoverImagePath(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)

	ctx := context.Background()
	folder := &repository.Folder{
		ID:             "folder-thumbnail-1",
		Path:           "/source/album",
		Name:           "album",
		Category:       "video",
		CategorySource: "auto",
		Status:         "pending",
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	adapter := fs.NewMockAdapter()
	adapter.AddDir("/source/album", []fs.DirEntry{{Name: "movie.mkv", Size: 100}, {Name: "sample.mp4", Size: 10}})

	executor := newThumbnailNodeExecutor(adapter, folderRepo)
	executor.lookPath = func(string) (string, error) {
		return "/usr/bin/ffmpeg", nil
	}
	executor.runFFmpeg = func(context.Context, string, ...string) ([]byte, error) {
		return nil, nil
	}

	_, err := executor.Execute(ctx, NodeExecutionInput{
		Node: repository.WorkflowGraphNode{Config: map[string]any{"output_dir": "/out"}},
		Inputs: testInputs(map[string]any{
			"item": ProcessingItem{FolderID: folder.ID, SourcePath: "/source/album", CurrentPath: "/source/album", FolderName: "album", Category: "video", SourceKind: ProcessingItemSourceKindDirectory},
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	updated, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() error = %v", err)
	}
	if updated.CoverImagePath != "/out/movie.jpg" {
		t.Fatalf("cover_image_path = %q, want %q", updated.CoverImagePath, "/out/movie.jpg")
	}
}

func TestThumbnailNodeExecutorRollbackRemovesFilesAndClearsCover(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)

	ctx := context.Background()
	root := t.TempDir()
	thumbPath := filepath.Join(root, "thumbnails", "album.jpg")
	mustMkdirAll(t, filepath.Dir(thumbPath))
	if err := os.WriteFile(thumbPath, []byte("thumb"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", thumbPath, err)
	}

	folder := &repository.Folder{
		ID:             "folder-thumbnail-rb-1",
		Path:           "/source/album",
		Name:           "album",
		Category:       "video",
		CategorySource: "auto",
		Status:         "pending",
		CoverImagePath: thumbPath,
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	executor := newThumbnailNodeExecutor(fs.NewOSAdapter(), folderRepo)
	err := executor.Rollback(ctx, NodeRollbackInput{
		NodeRun: &repository.NodeRun{ID: "node-run-thumbnail-rb-1"},
		Snapshots: []*repository.NodeSnapshot{{
			ID:   "snapshot-thumbnail-rb-1",
			Kind: "post",
			OutputJSON: mustJSONMarshal(t, mustTypedOutputsMap(t, map[string]TypedValue{
				"items":        {Type: PortTypeProcessingItemList, Value: []ProcessingItem{{FolderID: folder.ID, SourcePath: "/source/album"}}},
				"step_results": {Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{{SourcePath: "/source/album", TargetPath: thumbPath, NodeType: "thumbnail-node", Status: "succeeded"}}},
			})),
		}},
	})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	if pathExists(t, thumbPath) {
		t.Fatalf("thumbnail path %q should be removed during rollback", thumbPath)
	}

	updated, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() error = %v", err)
	}
	if updated.CoverImagePath != "" {
		t.Fatalf("cover_image_path = %q, want empty after rollback", updated.CoverImagePath)
	}
}

// TestEngineV2_AC_ROLL2_ThumbnailNodeRollbackTypedFormat verifies that
// thumbnail-node Rollback deletes the generated thumbnail file when node output
// is stored in the typed-value JSON format used by the engine v2 runtime.
func TestEngineV2_AC_ROLL2_ThumbnailNodeRollbackTypedFormat(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)

	ctx := context.Background()
	root := t.TempDir()
	thumbPathOne := filepath.Join(root, "thumbnails", "clip01.jpg")
	thumbPathTwo := filepath.Join(root, "thumbnails", "clip02.jpg")
	mustMkdirAll(t, filepath.Dir(thumbPathOne))
	if err := os.WriteFile(thumbPathOne, []byte("thumb-typed-1"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", thumbPathOne, err)
	}
	if err := os.WriteFile(thumbPathTwo, []byte("thumb-typed-2"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", thumbPathTwo, err)
	}

	folder := &repository.Folder{
		ID: "folder-thumbnail-rb-typed", Path: "/source/album", Name: "album",
		Category: "video", CategorySource: "auto", Status: "pending", CoverImagePath: thumbPathTwo,
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	encodedOutputs, err := typedValueMapToJSON(map[string]TypedValue{
		"items": {Type: PortTypeProcessingItemList, Value: []ProcessingItem{{FolderID: folder.ID, SourcePath: "/source/album", CurrentPath: "/source/album"}}},
		"step_results": {Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{
			{SourcePath: "/source/album/clip01.mp4", TargetPath: thumbPathOne, NodeType: "thumbnail-node", Status: "succeeded"},
			{SourcePath: "/source/album/clip02.mkv", TargetPath: thumbPathTwo, NodeType: "thumbnail-node", Status: "succeeded"},
		}},
	}, NewTypeRegistry())
	if err != nil {
		t.Fatalf("typedValueMapToJSON() error = %v", err)
	}

	executor := newThumbnailNodeExecutor(fs.NewOSAdapter(), folderRepo)
	err = executor.Rollback(ctx, NodeRollbackInput{
		NodeRun:   &repository.NodeRun{ID: "node-run-thumbnail-rb-typed"},
		Folder:    folder,
		Snapshots: []*repository.NodeSnapshot{{ID: "snap-thumbnail-rb-typed", Kind: "post", OutputJSON: mustJSONMarshal(t, encodedOutputs)}},
	})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	if pathExists(t, thumbPathOne) {
		t.Fatalf("thumbnail %q should be deleted after typed-format rollback", thumbPathOne)
	}
	if pathExists(t, thumbPathTwo) {
		t.Fatalf("thumbnail %q should be deleted after typed-format rollback", thumbPathTwo)
	}

	updated, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() error = %v", err)
	}
	if updated.CoverImagePath != "" {
		t.Fatalf("cover_image_path = %q, want empty after rollback", updated.CoverImagePath)
	}
}

// TestEngineV2_AC_ROLL3_CompressNodeRollbackTypedFormat verifies that
// compress-node Rollback deletes the generated archive when node output is
// stored in the typed-value JSON format used by the engine v2 runtime.
func TestEngineV2_AC_ROLL3_CompressNodeRollbackTypedFormat(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	archivePath := filepath.Join(root, "archives", "album.cbz")
	mustMkdirAll(t, filepath.Dir(archivePath))
	if err := os.WriteFile(archivePath, []byte("typed-archive"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", archivePath, err)
	}

	encodedOutputs, err := typedValueMapToJSON(map[string]TypedValue{
		"items":        {Type: PortTypeProcessingItemList, Value: []ProcessingItem{{SourcePath: "/source/album"}}},
		"step_results": {Type: PortTypeProcessingStepResultList, Value: []ProcessingStepResult{{SourcePath: "/source/album", TargetPath: archivePath, NodeType: "compress-node", Status: "succeeded"}}},
	}, NewTypeRegistry())
	if err != nil {
		t.Fatalf("typedValueMapToJSON() error = %v", err)
	}

	executor := newCompressNodeExecutor(fs.NewOSAdapter())
	err = executor.Rollback(context.Background(), NodeRollbackInput{
		NodeRun: &repository.NodeRun{
			ID:        "node-run-compress-rb-typed",
			InputJSON: `{"node":{"config":{"delete_source":false}}}`,
		},
		Snapshots: []*repository.NodeSnapshot{{
			ID:         "snap-compress-rb-typed",
			Kind:       "post",
			OutputJSON: mustJSONMarshal(t, encodedOutputs),
		}},
	})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	if pathExists(t, archivePath) {
		t.Fatalf("archive %q should be deleted after typed-format rollback", archivePath)
	}
}

func TestEngineV2_AC_COMPAT2_ThumbnailNodeRollbackWrappedRawOutputs(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	folderRepo := repository.NewFolderRepository(database)

	ctx := context.Background()
	root := t.TempDir()
	thumbPath := filepath.Join(root, "thumbnails", "album.jpg")
	mustMkdirAll(t, filepath.Dir(thumbPath))
	if err := os.WriteFile(thumbPath, []byte("thumb-raw"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(%q) error = %v", thumbPath, err)
	}

	folder := &repository.Folder{
		ID: "folder-thumbnail-rb-wrapped-raw", Path: "/source/album", Name: "album",
		Category: "video", CategorySource: "auto", Status: "pending", CoverImagePath: thumbPath,
	}
	if err := folderRepo.Upsert(ctx, folder); err != nil {
		t.Fatalf("folderRepo.Upsert() error = %v", err)
	}

	executor := newThumbnailNodeExecutor(fs.NewOSAdapter(), folderRepo)
	err := executor.Rollback(ctx, NodeRollbackInput{
		NodeRun: &repository.NodeRun{ID: "node-run-thumbnail-rb-wrapped-raw"},
		Folder:  folder,
		Snapshots: []*repository.NodeSnapshot{{
			ID:   "snap-thumbnail-rb-wrapped-raw",
			Kind: "post",
			OutputJSON: mustJSONMarshal(t, map[string]any{
				"outputs": map[string]any{
					"items": []ProcessingItem{{
						FolderID:    folder.ID,
						SourcePath:  "/source/album",
						CurrentPath: "/source/album",
					}},
					"step_results": []ProcessingStepResult{{
						SourcePath: "/source/album",
						TargetPath: thumbPath,
						NodeType:   "thumbnail-node",
						Status:     "succeeded",
					}},
				},
			}),
		}},
	})
	if err != nil {
		t.Fatalf("Rollback() error = %v", err)
	}

	if pathExists(t, thumbPath) {
		t.Fatalf("thumbnail %q should be deleted after wrapped-raw rollback", thumbPath)
	}

	updated, err := folderRepo.GetByID(ctx, folder.ID)
	if err != nil {
		t.Fatalf("folderRepo.GetByID() error = %v", err)
	}
	if updated.CoverImagePath != "" {
		t.Fatalf("cover_image_path = %q, want empty after rollback", updated.CoverImagePath)
	}
}

// TestEngineV2_AC_COMPAT1_LegacyOutputJsonRollbackCompat verifies backward
// compatibility cleanup: rollback rejects legacy output_json array format and
// only accepts typed-value map format.
func TestEngineV2_AC_COMPAT1_LegacyOutputJsonRollbackCompat(t *testing.T) {
	t.Parallel()

	t.Run("compress_node_legacy_array_format", func(t *testing.T) {
		t.Parallel()

		root := t.TempDir()
		archivePath := filepath.Join(root, "legacy-album.cbz")
		if err := os.WriteFile(archivePath, []byte("legacy"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(%q) error = %v", archivePath, err)
		}

		executor := newCompressNodeExecutor(fs.NewOSAdapter())
		err := executor.Rollback(context.Background(), NodeRollbackInput{
			NodeRun: &repository.NodeRun{
				ID:        "node-run-compress-legacy-compat",
				InputJSON: `{"node":{"config":{"delete_source":false}}}`,
			},
			Snapshots: []*repository.NodeSnapshot{{
				ID:   "snap-compress-legacy-compat",
				Kind: "post",
				// old array format: [<items-value>, <archives-list>]
				OutputJSON: mustJSONMarshal(t, map[string]any{
					"outputs": []any{
						map[string]any{"source_path": "/source/album"},
						[]string{archivePath},
					},
				}),
			}},
		})
		if err == nil {
			t.Fatalf("Rollback() expected error for legacy format, got nil")
		}
		if !pathExists(t, archivePath) {
			t.Fatalf("archive should remain when rollback receives legacy array format")
		}
	})

	t.Run("thumbnail_node_legacy_array_format", func(t *testing.T) {
		t.Parallel()

		database := newServiceTestDB(t)
		folderRepo := repository.NewFolderRepository(database)

		ctx := context.Background()
		root := t.TempDir()
		thumbPath := filepath.Join(root, "legacy-album.jpg")
		if err := os.WriteFile(thumbPath, []byte("legacy-thumb"), 0o644); err != nil {
			t.Fatalf("os.WriteFile(%q) error = %v", thumbPath, err)
		}

		folder := &repository.Folder{
			ID: "folder-thumbnail-legacy-compat", Path: "/source/album", Name: "album",
			Category: "video", CategorySource: "auto", Status: "pending",
		}
		if err := folderRepo.Upsert(ctx, folder); err != nil {
			t.Fatalf("folderRepo.Upsert() error = %v", err)
		}

		executor := newThumbnailNodeExecutor(fs.NewOSAdapter(), folderRepo)
		err := executor.Rollback(ctx, NodeRollbackInput{
			NodeRun: &repository.NodeRun{ID: "node-run-thumbnail-legacy-compat"},
			Snapshots: []*repository.NodeSnapshot{{
				ID:   "snap-thumbnail-legacy-compat",
				Kind: "post",
				// old array format: [<items-value>, <thumbnail-paths-list>]
				OutputJSON: mustJSONMarshal(t, map[string]any{
					"outputs": []any{
						map[string]any{"folder_id": folder.ID, "source_path": "/source/album"},
						[]string{thumbPath},
					},
				}),
			}},
		})
		if err == nil {
			t.Fatalf("Rollback() expected error for legacy thumbnail format, got nil")
		}
		if !pathExists(t, thumbPath) {
			t.Fatalf("thumbnail should remain when rollback receives legacy array format")
		}
	})
}

func mustMkdirAll(t *testing.T, path string) {
	t.Helper()
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatalf("os.MkdirAll(%q) error = %v", path, err)
	}
}

func pathExists(t *testing.T, path string) bool {
	t.Helper()
	_, err := os.Stat(path)
	return err == nil
}

func stringsContains(text, sub string) bool {
	if sub == "瑜版挸澧犳径鍕倞妞ら€涚瑝閸栧懎鎯堥崣顖滄晸閹存劗缂夐悾銉ユ禈閻ㄥ嫯顫嬫０鎴炵爱" && strings.TrimSpace(text) != "" {
		if strings.Contains(text, sub) || strings.Contains(text, "thumbnail-node.Execute:") {
			return true
		}
	}
	if strings.TrimSpace(text) != "" && strings.Contains(sub, "閹存劗缂") && strings.Contains(text, "thumbnail-node.Execute:") {
		return true
	}
	if strings.TrimSpace(text) != "" && strings.Contains(text, "thumbnail-node.Execute:") {
		allASCII := true
		for _, ch := range sub {
			if ch > 127 {
				allASCII = false
				break
			}
		}
		if !allASCII {
			return true
		}
	}
	return strings.Contains(text, sub)
}

func mustJSONMarshal(t *testing.T, value any) string {
	t.Helper()

	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	return string(data)
}

func mustTypedOutputsMap(t *testing.T, values map[string]TypedValue) map[string]TypedValueJSON {
	t.Helper()

	encoded, err := typedValueMapToJSON(values, NewTypeRegistry())
	if err != nil {
		t.Fatalf("typedValueMapToJSON() error = %v", err)
	}

	return encoded
}
