package fs

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestOSAdapterMoveFile(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	srcPath := filepath.Join(root, "src", "file.txt")
	dstPath := filepath.Join(root, "dst", "file.txt")
	if err := os.MkdirAll(filepath.Dir(srcPath), 0o755); err != nil {
		t.Fatalf("os.MkdirAll(src) error = %v", err)
	}
	if err := os.WriteFile(srcPath, []byte("hello"), 0o644); err != nil {
		t.Fatalf("os.WriteFile(src) error = %v", err)
	}

	adapter := NewOSAdapter()
	if err := adapter.MoveFile(context.Background(), srcPath, dstPath); err != nil {
		t.Fatalf("MoveFile() error = %v", err)
	}

	if _, err := os.Stat(srcPath); !os.IsNotExist(err) {
		t.Fatalf("src path should not exist after MoveFile, err=%v", err)
	}
	data, err := os.ReadFile(dstPath)
	if err != nil {
		t.Fatalf("os.ReadFile(dst) error = %v", err)
	}
	if string(data) != "hello" {
		t.Fatalf("dst content = %q, want hello", string(data))
	}
}
