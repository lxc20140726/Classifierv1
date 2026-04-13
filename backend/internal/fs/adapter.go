package fs

import (
	"context"
	"io"
	"os"
	"time"
)

// FSAdapter abstracts all filesystem operations.
// Only this package may call os.* functions.
type FSAdapter interface {
	ReadDir(ctx context.Context, path string) ([]DirEntry, error)
	Stat(ctx context.Context, path string) (FileInfo, error)
	MoveDir(ctx context.Context, src, dst string) error
	MoveFile(ctx context.Context, src, dst string) error
	MkdirAll(ctx context.Context, path string, perm os.FileMode) error
	Remove(ctx context.Context, path string) error
	Exists(ctx context.Context, path string) (bool, error)
	OpenFileRead(ctx context.Context, path string) (io.ReadCloser, error)
	OpenFileWrite(ctx context.Context, path string, perm os.FileMode) (io.WriteCloser, error)
}

// DirEntry represents a directory entry.
type DirEntry struct {
	Name  string
	IsDir bool
	Size  int64
}

// FileInfo represents file metadata.
type FileInfo struct {
	Name    string
	IsDir   bool
	Size    int64
	ModTime time.Time
}
