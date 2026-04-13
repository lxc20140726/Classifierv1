package service

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

type folderTreeScannerTestFS struct {
	*fs.MockAdapter
	readErr map[string]error
}

func newFolderTreeScannerTestFS() *folderTreeScannerTestFS {
	return &folderTreeScannerTestFS{
		MockAdapter: fs.NewMockAdapter(),
		readErr:     make(map[string]error),
	}
}

func (a *folderTreeScannerTestFS) ReadDir(ctx context.Context, path string) ([]fs.DirEntry, error) {
	if err, ok := a.readErr[path]; ok {
		return nil, err
	}

	return a.MockAdapter.ReadDir(ctx, path)
}

func TestFolderTreeScannerExecutorSchema(t *testing.T) {
	t.Parallel()

	executor := newFolderTreeScannerExecutor(fs.NewMockAdapter())
	schema := executor.Schema()

	if schema.Type != "folder-tree-scanner" {
		t.Fatalf("schema.Type = %q, want folder-tree-scanner", schema.Type)
	}
	if schema.Label != "目录树扫描器" {
		t.Fatalf("schema.Label = %q, want 目录树扫描器", schema.Label)
	}
	if schema.Description != "递归扫描源目录，输出顶层子目录 FolderTree 列表" {
		t.Fatalf("schema.Description = %q, want expected Chinese description", schema.Description)
	}

	if len(schema.Inputs) != 1 {
		t.Fatalf("len(schema.Inputs) = %d, want 1", len(schema.Inputs))
	}
	if schema.Inputs[0].Name != "source_dir" || !schema.Inputs[0].Required {
		t.Fatalf("input port = %+v, want source_dir required", schema.Inputs[0])
	}

	if len(schema.Outputs) != 1 {
		t.Fatalf("len(schema.Outputs) = %d, want 1", len(schema.Outputs))
	}
	if schema.Outputs[0].Name != "tree" {
		t.Fatalf("output port name = %q, want tree", schema.Outputs[0].Name)
	}
}

func TestFolderTreeScannerExecutorExecuteRequiresSourceDir(t *testing.T) {
	t.Parallel()

	executor := newFolderTreeScannerExecutor(fs.NewMockAdapter())
	_, err := executor.Execute(context.Background(), NodeExecutionInput{Node: repository.WorkflowGraphNode{Config: map[string]any{}}})
	if err == nil {
		t.Fatalf("Execute() error = nil, want source_dir required error")
	}
}

func TestFolderTreeScannerExecutorExecuteUsesPortAndDefaultExcludes(t *testing.T) {
	t.Parallel()

	adapter := newFolderTreeScannerTestFS()
	root := "/source"
	albumPath := filepath.Join(root, "album")
	subPath := filepath.Join(albumPath, "sub")

	adapter.AddDir(root, []fs.DirEntry{
		{Name: "album", IsDir: true},
		{Name: "@eaDir", IsDir: true},
		{Name: "root.txt", IsDir: false},
	})
	adapter.AddDir(albumPath, []fs.DirEntry{
		{Name: "a.jpg", IsDir: false, Size: 10},
		{Name: ".DS_Store", IsDir: false, Size: 1},
		{Name: "sub", IsDir: true},
	})
	adapter.AddDir(subPath, []fs.DirEntry{{Name: "b.mp4", IsDir: false, Size: 20}})

	executor := newFolderTreeScannerExecutor(adapter)
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{Config: map[string]any{}},
		Inputs: testInputs(map[string]any{
			"source_dir": root,
		}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if out.Status != ExecutionSuccess {
		t.Fatalf("status = %q, want success", out.Status)
	}
	if len(out.Outputs) != 1 {
		t.Fatalf("len(outputs) = %d, want 1", len(out.Outputs))
	}

	trees, ok := out.Outputs["tree"].Value.([]FolderTree)
	if !ok {
		t.Fatalf("output type = %T, want []FolderTree", out.Outputs["tree"].Value)
	}
	if len(trees) != 1 {
		t.Fatalf("len(trees) = %d, want 1", len(trees))
	}
	tree := trees[0]
	if tree.Name != "album" || tree.Path != normalizeWorkflowPath(albumPath) {
		t.Fatalf("tree name/path = %q/%q, want album/%q", tree.Name, tree.Path, normalizeWorkflowPath(albumPath))
	}
	if len(tree.Files) != 1 || tree.Files[0].Name != "a.jpg" || tree.Files[0].Ext != ".jpg" {
		t.Fatalf("tree files = %+v, want only a.jpg", tree.Files)
	}
	if len(tree.Subdirs) != 1 || tree.Subdirs[0].Name != "sub" {
		t.Fatalf("subdirs = %+v, want one sub dir", tree.Subdirs)
	}
	if len(tree.Subdirs[0].Files) != 1 || tree.Subdirs[0].Files[0].Name != "b.mp4" {
		t.Fatalf("subdir files = %+v, want one b.mp4", tree.Subdirs[0].Files)
	}
}

func TestFolderTreeScannerExecutorExecuteRespectsMaxDepthAndMinFileCount(t *testing.T) {
	t.Parallel()

	adapter := newFolderTreeScannerTestFS()
	root := "/library"
	bigPath := filepath.Join(root, "big")
	deepPath := filepath.Join(bigPath, "deep")
	smallPath := filepath.Join(root, "small")

	adapter.AddDir(root, []fs.DirEntry{{Name: "big", IsDir: true}, {Name: "small", IsDir: true}})
	adapter.AddDir(bigPath, []fs.DirEntry{{Name: "cover.jpg", IsDir: false, Size: 3}, {Name: "deep", IsDir: true}})
	adapter.AddDir(deepPath, []fs.DirEntry{{Name: "clip.mp4", IsDir: false, Size: 6}})
	adapter.AddDir(smallPath, []fs.DirEntry{{Name: "x.jpg", IsDir: false, Size: 1}})

	executor := newFolderTreeScannerExecutor(adapter)
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{Config: map[string]any{
			"max_depth":      0,
			"min_file_count": 2,
		}},
		Inputs: testInputs(map[string]any{"source_dir": root}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	if len(out.Outputs) != 1 {
		t.Fatalf("len(outputs) = %d, want 1", len(out.Outputs))
	}
	trees, ok := out.Outputs["tree"].Value.([]FolderTree)
	if !ok {
		t.Fatalf("output type = %T, want []FolderTree", out.Outputs["tree"].Value)
	}
	if len(trees) != 0 {
		t.Fatalf("len(trees) = %d, want 0 because max_depth=0 drops deep files and min_file_count=2 filters all", len(trees))
	}

	out, err = executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{Config: map[string]any{
			"max_depth":      5,
			"min_file_count": 2,
		}},
		Inputs: testInputs(map[string]any{"source_dir": root}),
	})
	if err != nil {
		t.Fatalf("Execute() second call error = %v", err)
	}

	if len(out.Outputs) != 1 {
		t.Fatalf("len(outputs) = %d, want 1", len(out.Outputs))
	}
	trees, ok = out.Outputs["tree"].Value.([]FolderTree)
	if !ok {
		t.Fatalf("output type = %T, want []FolderTree", out.Outputs["tree"].Value)
	}
	if len(trees) != 1 {
		t.Fatalf("len(trees) = %d, want 1", len(trees))
	}
	tree := trees[0]
	if tree.Name != "big" {
		t.Fatalf("tree.Name = %q, want big", tree.Name)
	}
	if len(tree.Subdirs) != 1 || tree.Subdirs[0].Name != "deep" {
		t.Fatalf("tree.Subdirs = %+v, want deep subdir", tree.Subdirs)
	}
}

func TestFolderTreeScannerExecutorExecuteSkipsConfiguredOutputDirs(t *testing.T) {
	t.Parallel()

	adapter := newFolderTreeScannerTestFS()
	root := "/library"
	albumPath := filepath.Join(root, "album")
	videoOutputPath := filepath.Join(root, "video")
	mixedOutputPath := filepath.Join(root, "mixed")

	adapter.AddDir(root, []fs.DirEntry{
		{Name: "album", IsDir: true},
		{Name: "video", IsDir: true},
		{Name: "mixed", IsDir: true},
	})
	adapter.AddDir(albumPath, []fs.DirEntry{{Name: "cover.jpg", IsDir: false, Size: 3}})
	adapter.AddDir(videoOutputPath, []fs.DirEntry{{Name: "clip.mp4", IsDir: false, Size: 6}})
	adapter.AddDir(mixedOutputPath, []fs.DirEntry{{Name: "poster.jpg", IsDir: false, Size: 2}})

	executor := newFolderTreeScannerExecutor(adapter)
	out, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node: repository.WorkflowGraphNode{Config: map[string]any{}},
		AppConfig: &repository.AppConfig{
			OutputDirs: repository.AppConfigOutputDirs{
				Video: []string{videoOutputPath},
				Mixed: []string{mixedOutputPath},
			},
		},
		Inputs: testInputs(map[string]any{"source_dir": root}),
	})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}

	trees, ok := out.Outputs["tree"].Value.([]FolderTree)
	if !ok {
		t.Fatalf("output type = %T, want []FolderTree", out.Outputs["tree"].Value)
	}
	if len(trees) != 1 {
		t.Fatalf("len(trees) = %d, want 1", len(trees))
	}
	if trees[0].Path != normalizeWorkflowPath(albumPath) {
		t.Fatalf("tree.Path = %q, want %q", trees[0].Path, normalizeWorkflowPath(albumPath))
	}
}

func TestFolderTreeScannerExecutorExecuteReadErrors(t *testing.T) {
	t.Parallel()

	adapter := newFolderTreeScannerTestFS()
	root := "/broken"
	child := filepath.Join(root, "child")

	adapter.readErr[root] = fmt.Errorf("boom-root")

	executor := newFolderTreeScannerExecutor(adapter)
	_, err := executor.Execute(context.Background(), NodeExecutionInput{
		Node:   repository.WorkflowGraphNode{Config: map[string]any{}},
		Inputs: testInputs(map[string]any{"source_dir": root}),
	})
	if err == nil {
		t.Fatalf("Execute() root error = nil, want error")
	}

	delete(adapter.readErr, root)
	adapter.AddDir(root, []fs.DirEntry{{Name: "child", IsDir: true}})
	adapter.readErr[child] = fmt.Errorf("boom-child")

	_, err = executor.Execute(context.Background(), NodeExecutionInput{
		Node:   repository.WorkflowGraphNode{Config: map[string]any{}},
		Inputs: testInputs(map[string]any{"source_dir": root}),
	})
	if err == nil {
		t.Fatalf("Execute() child error = nil, want error")
	}
}
