package fs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type MockAdapter struct {
	dirs         map[string][]DirEntry
	files        map[string]FileInfo
	fileContents map[string][]byte
	mu           sync.RWMutex
}

func NewMockAdapter() *MockAdapter {
	return &MockAdapter{
		dirs:         make(map[string][]DirEntry),
		files:        make(map[string]FileInfo),
		fileContents: make(map[string][]byte),
	}
}

func (m *MockAdapter) AddDir(path string, entries []DirEntry) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cloned := make([]DirEntry, len(entries))
	copy(cloned, entries)
	m.dirs[normalizePath(path)] = cloned
}

func (m *MockAdapter) ReadDir(ctx context.Context, path string) ([]DirEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	entries, ok := m.dirs[normalizePath(path)]
	if !ok {
		return nil, fmt.Errorf("MockAdapter.ReadDir: %w", os.ErrNotExist)
	}

	cloned := make([]DirEntry, len(entries))
	copy(cloned, entries)
	return cloned, nil
}

func (m *MockAdapter) AddFile(path string, content []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()

	normalizedPath := normalizePath(path)
	data := append([]byte(nil), content...)
	m.fileContents[normalizedPath] = data
	m.files[normalizedPath] = FileInfo{
		Name:    filepath.Base(normalizedPath),
		IsDir:   false,
		Size:    int64(len(data)),
		ModTime: time.Now().UTC(),
	}
}

func (m *MockAdapter) Stat(ctx context.Context, path string) (FileInfo, error) {
	if err := ctx.Err(); err != nil {
		return FileInfo{}, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	info, ok := m.files[normalizePath(path)]
	if ok {
		return info, nil
	}

	normalizedPath := normalizePath(path)
	if _, ok := m.dirs[normalizedPath]; ok {
		return FileInfo{
			Name:    filepath.Base(normalizedPath),
			IsDir:   true,
			Size:    0,
			ModTime: time.Now().UTC(),
		}, nil
	}

	return FileInfo{}, fmt.Errorf("MockAdapter.Stat: %w", os.ErrNotExist)
}

func (m *MockAdapter) MoveDir(ctx context.Context, src, dst string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	normalizedSrc := normalizePath(src)
	normalizedDst := normalizePath(dst)
	entries, ok := m.dirs[normalizedSrc]
	if !ok {
		return fmt.Errorf("MockAdapter.MoveDir: %w", os.ErrNotExist)
	}

	cloned := make([]DirEntry, len(entries))
	copy(cloned, entries)
	m.dirs[normalizedDst] = cloned
	delete(m.dirs, normalizedSrc)
	return nil
}

func (m *MockAdapter) MoveFile(ctx context.Context, src, dst string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	normalizedSrc := normalizePath(src)
	normalizedDst := normalizePath(dst)
	info, ok := m.files[normalizedSrc]
	if !ok {
		return fmt.Errorf("MockAdapter.MoveFile: %w", os.ErrNotExist)
	}
	if info.IsDir {
		return fmt.Errorf("MockAdapter.MoveFile: source %q is a directory", normalizedSrc)
	}

	content := append([]byte(nil), m.fileContents[normalizedSrc]...)
	info.Name = filepath.Base(normalizedDst)
	m.files[normalizedDst] = info
	m.fileContents[normalizedDst] = content
	delete(m.files, normalizedSrc)
	delete(m.fileContents, normalizedSrc)
	return nil
}

func (m *MockAdapter) MkdirAll(ctx context.Context, path string, _ os.FileMode) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	normalizedPath := normalizePath(path)
	if _, ok := m.dirs[normalizedPath]; !ok {
		m.dirs[normalizedPath] = []DirEntry{}
	}

	return nil
}

func (m *MockAdapter) Remove(ctx context.Context, path string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	normalizedPath := normalizePath(path)
	prefix := normalizedPath + "/"
	for key := range m.dirs {
		if key == normalizedPath || strings.HasPrefix(key, prefix) {
			delete(m.dirs, key)
		}
	}
	for key := range m.files {
		if key == normalizedPath || strings.HasPrefix(key, prefix) {
			delete(m.files, key)
			delete(m.fileContents, key)
		}
	}

	delete(m.files, normalizedPath)
	delete(m.fileContents, normalizedPath)
	return nil
}

func (m *MockAdapter) Exists(ctx context.Context, path string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	normalizedPath := normalizePath(path)
	if _, ok := m.dirs[normalizedPath]; ok {
		return true, nil
	}

	if _, ok := m.files[normalizedPath]; ok {
		return true, nil
	}

	return false, nil
}

func (m *MockAdapter) OpenFileRead(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.RLock()
	defer m.mu.RUnlock()

	normalizedPath := normalizePath(path)
	content, ok := m.fileContents[normalizedPath]
	if !ok {
		if _, exists := m.files[normalizedPath]; !exists {
			return nil, fmt.Errorf("MockAdapter.OpenFileRead: %w", os.ErrNotExist)
		}
		content = nil
	}

	return io.NopCloser(bytes.NewReader(append([]byte(nil), content...))), nil
}

func (m *MockAdapter) OpenFileWrite(ctx context.Context, path string, _ os.FileMode) (io.WriteCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return &mockWriteCloser{adapter: m, path: normalizePath(path)}, nil
}

type mockWriteCloser struct {
	adapter *MockAdapter
	path    string
	buffer  bytes.Buffer
	closed  bool
}

func (w *mockWriteCloser) Write(p []byte) (int, error) {
	if w.closed {
		return 0, fmt.Errorf("mockWriteCloser.Write: file is closed")
	}

	return w.buffer.Write(p)
}

func (w *mockWriteCloser) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true

	data := append([]byte(nil), w.buffer.Bytes()...)

	w.adapter.mu.Lock()
	defer w.adapter.mu.Unlock()

	normalizedPath := normalizePath(w.path)
	w.adapter.fileContents[normalizedPath] = data
	w.adapter.files[normalizedPath] = FileInfo{
		Name:    filepath.Base(normalizedPath),
		IsDir:   false,
		Size:    int64(len(data)),
		ModTime: time.Now().UTC(),
	}

	return nil
}

var _ FSAdapter = (*MockAdapter)(nil)

func normalizePath(path string) string {
	trimmed := strings.TrimSpace(path)
	if trimmed == "" {
		return ""
	}

	return filepath.ToSlash(filepath.Clean(trimmed))
}
