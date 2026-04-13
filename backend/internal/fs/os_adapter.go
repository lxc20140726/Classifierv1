package fs

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
)

type OSAdapter struct{}

func NewOSAdapter() *OSAdapter {
	return &OSAdapter{}
}

func (a *OSAdapter) ReadDir(ctx context.Context, path string) ([]DirEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("OSAdapter.ReadDir: %w", err)
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, fmt.Errorf("OSAdapter.ReadDir: %w", err)
	}

	out := make([]DirEntry, 0, len(entries))
	for _, entry := range entries {
		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("OSAdapter.ReadDir: %w", err)
		}

		info, err := entry.Info()
		if err != nil {
			return nil, fmt.Errorf("OSAdapter.ReadDir: %w", err)
		}

		out = append(out, DirEntry{
			Name:  entry.Name(),
			IsDir: entry.IsDir(),
			Size:  info.Size(),
		})
	}

	return out, nil
}

func (a *OSAdapter) Stat(ctx context.Context, path string) (FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return FileInfo{}, fmt.Errorf("OSAdapter.Stat: %w", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		return FileInfo{}, fmt.Errorf("OSAdapter.Stat: %w", err)
	}

	return FileInfo{
		Name:    info.Name(),
		IsDir:   info.IsDir(),
		Size:    info.Size(),
		ModTime: info.ModTime(),
	}, nil
}

func (a *OSAdapter) MoveDir(ctx context.Context, src, dst string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("OSAdapter.MoveDir: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("OSAdapter.MoveDir: %w", err)
	}

	err := os.Rename(src, dst)
	if err == nil {
		return nil
	}

	if !errors.Is(err, syscall.EXDEV) {
		return fmt.Errorf("OSAdapter.MoveDir: %w", err)
	}

	if copyErr := a.copyTree(ctx, src, dst); copyErr != nil {
		return fmt.Errorf("OSAdapter.MoveDir: %w", copyErr)
	}

	if removeErr := os.RemoveAll(src); removeErr != nil {
		return fmt.Errorf("OSAdapter.MoveDir: %w", removeErr)
	}

	return nil
}

func (a *OSAdapter) MoveFile(ctx context.Context, src, dst string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("OSAdapter.MoveFile: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return fmt.Errorf("OSAdapter.MoveFile: %w", err)
	}

	err := os.Rename(src, dst)
	if err == nil {
		return nil
	}

	if !errors.Is(err, syscall.EXDEV) {
		return fmt.Errorf("OSAdapter.MoveFile: %w", err)
	}

	info, statErr := os.Stat(src)
	if statErr != nil {
		return fmt.Errorf("OSAdapter.MoveFile: %w", statErr)
	}
	if info.IsDir() {
		return fmt.Errorf("OSAdapter.MoveFile: source %q is a directory", src)
	}
	if copyErr := copyFile(src, dst, info.Mode()); copyErr != nil {
		return fmt.Errorf("OSAdapter.MoveFile: %w", copyErr)
	}
	if removeErr := os.Remove(src); removeErr != nil {
		return fmt.Errorf("OSAdapter.MoveFile: %w", removeErr)
	}

	return nil
}

func (a *OSAdapter) MkdirAll(ctx context.Context, path string, perm os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("OSAdapter.MkdirAll: %w", err)
	}

	if err := os.MkdirAll(path, perm); err != nil {
		return fmt.Errorf("OSAdapter.MkdirAll: %w", err)
	}

	return nil
}

func (a *OSAdapter) Remove(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("OSAdapter.Remove: %w", err)
	}

	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("OSAdapter.Remove: %w", err)
	}

	return nil
}

func (a *OSAdapter) Exists(ctx context.Context, path string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("OSAdapter.Exists: %w", err)
	}

	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}

	if os.IsNotExist(err) {
		return false, nil
	}

	return false, fmt.Errorf("OSAdapter.Exists: %w", err)
}

func (a *OSAdapter) OpenFileRead(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("OSAdapter.OpenFileRead: %w", err)
	}

	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("OSAdapter.OpenFileRead: %w", err)
	}

	return file, nil
}

func (a *OSAdapter) OpenFileWrite(ctx context.Context, path string, perm os.FileMode) (io.WriteCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("OSAdapter.OpenFileWrite: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("OSAdapter.OpenFileWrite: %w", err)
	}

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return nil, fmt.Errorf("OSAdapter.OpenFileWrite: %w", err)
	}

	return file, nil
}

func (a *OSAdapter) copyTree(ctx context.Context, src, dst string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	info, err := os.Stat(src)
	if err != nil {
		return err
	}

	if info.IsDir() {
		if err := os.MkdirAll(dst, info.Mode()); err != nil {
			return err
		}

		entries, err := os.ReadDir(src)
		if err != nil {
			return err
		}

		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}

			srcPath := filepath.Join(src, entry.Name())
			dstPath := filepath.Join(dst, entry.Name())
			if err := a.copyTree(ctx, srcPath, dstPath); err != nil {
				return err
			}
		}

		return nil
	}

	return copyFile(src, dst, info.Mode())
}

func copyFile(src, dst string, mode os.FileMode) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, mode)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		return err
	}

	return nil
}

var _ FSAdapter = (*OSAdapter)(nil)
