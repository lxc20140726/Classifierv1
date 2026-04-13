package fs

import (
	"context"
	"reflect"
	"testing"
	"time"
)

func TestMockAdapterReadDir(t *testing.T) {
	t.Parallel()

	entries := []DirEntry{{Name: "a", IsDir: true, Size: 0}, {Name: "b.txt", IsDir: false, Size: 10}}

	tests := []struct {
		name    string
		setup   func(*MockAdapter)
		path    string
		want    []DirEntry
		wantErr bool
	}{
		{
			name: "existing path returns entries",
			setup: func(m *MockAdapter) {
				m.AddDir("/data", entries)
			},
			path: "/data",
			want: entries,
		},
		{
			name:    "missing path returns error",
			setup:   func(*MockAdapter) {},
			path:    "/missing",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			adapter := NewMockAdapter()
			tc.setup(adapter)

			got, err := adapter.ReadDir(context.Background(), tc.path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ReadDir() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr {
				return
			}

			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("ReadDir() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestMockAdapterStat(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	info := FileInfo{Name: "movie.mkv", IsDir: false, Size: 1234, ModTime: now}

	tests := []struct {
		name    string
		setup   func(*MockAdapter)
		path    string
		want    FileInfo
		wantErr bool
	}{
		{
			name: "existing path returns file info",
			setup: func(m *MockAdapter) {
				m.files["/data/movie.mkv"] = info
			},
			path: "/data/movie.mkv",
			want: info,
		},
		{
			name: "existing dir returns dir info",
			setup: func(m *MockAdapter) {
				m.AddDir("/data/dir", nil)
			},
			path: "/data/dir",
			want: FileInfo{Name: "dir", IsDir: true, Size: 0},
		},
		{
			name:    "missing path returns error",
			setup:   func(*MockAdapter) {},
			path:    "/missing",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			adapter := NewMockAdapter()
			tc.setup(adapter)

			got, err := adapter.Stat(context.Background(), tc.path)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Stat() error = %v, wantErr %v", err, tc.wantErr)
			}

			if tc.wantErr {
				return
			}

			if got.Name != tc.want.Name || got.IsDir != tc.want.IsDir || got.Size != tc.want.Size {
				t.Fatalf("Stat() = %#v, want %#v", got, tc.want)
			}
		})
	}
}

func TestMockAdapterMoveDir(t *testing.T) {
	t.Parallel()

	adapter := NewMockAdapter()
	entries := []DirEntry{{Name: "episode-1.mkv", IsDir: false, Size: 1024}}
	adapter.AddDir("/src", entries)

	if err := adapter.MoveDir(context.Background(), "/src", "/dst"); err != nil {
		t.Fatalf("MoveDir() error = %v", err)
	}

	gotDst, err := adapter.ReadDir(context.Background(), "/dst")
	if err != nil {
		t.Fatalf("ReadDir(/dst) error = %v", err)
	}

	if !reflect.DeepEqual(gotDst, entries) {
		t.Fatalf("dst entries = %#v, want %#v", gotDst, entries)
	}

	if _, err := adapter.ReadDir(context.Background(), "/src"); err == nil {
		t.Fatalf("expected /src to be removed after MoveDir")
	}
}

func TestMockAdapterMoveFile(t *testing.T) {
	t.Parallel()

	adapter := NewMockAdapter()
	adapter.AddFile("/src/file.txt", []byte("hello"))

	if err := adapter.MoveFile(context.Background(), "/src/file.txt", "/dst/file.txt"); err != nil {
		t.Fatalf("MoveFile() error = %v", err)
	}

	existsSrc, err := adapter.Exists(context.Background(), "/src/file.txt")
	if err != nil {
		t.Fatalf("Exists(/src/file.txt) error = %v", err)
	}
	if existsSrc {
		t.Fatalf("source file should be removed after MoveFile")
	}

	existsDst, err := adapter.Exists(context.Background(), "/dst/file.txt")
	if err != nil {
		t.Fatalf("Exists(/dst/file.txt) error = %v", err)
	}
	if !existsDst {
		t.Fatalf("destination file should exist after MoveFile")
	}
}

func TestMockAdapterMkdirAll(t *testing.T) {
	t.Parallel()

	adapter := NewMockAdapter()
	if err := adapter.MkdirAll(context.Background(), "/new/path", 0o755); err != nil {
		t.Fatalf("MkdirAll() error = %v", err)
	}

	exists, err := adapter.Exists(context.Background(), "/new/path")
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}

	if !exists {
		t.Fatalf("expected /new/path to exist after MkdirAll")
	}
}

func TestMockAdapterRemove(t *testing.T) {
	t.Parallel()

	adapter := NewMockAdapter()
	adapter.AddDir("/to/remove", nil)

	if err := adapter.Remove(context.Background(), "/to/remove"); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}

	exists, err := adapter.Exists(context.Background(), "/to/remove")
	if err != nil {
		t.Fatalf("Exists() error = %v", err)
	}

	if exists {
		t.Fatalf("expected /to/remove to be absent after Remove")
	}
}

func TestMockAdapterExists(t *testing.T) {
	t.Parallel()

	adapter := NewMockAdapter()
	adapter.AddDir("/existing/dir", nil)
	adapter.files["/existing/file.txt"] = FileInfo{Name: "file.txt", IsDir: false, Size: 5}

	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "existing dir returns true", path: "/existing/dir", want: true},
		{name: "existing file returns true", path: "/existing/file.txt", want: true},
		{name: "missing path returns false", path: "/missing", want: false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := adapter.Exists(context.Background(), tc.path)
			if err != nil {
				t.Fatalf("Exists() error = %v", err)
			}

			if got != tc.want {
				t.Fatalf("Exists() = %v, want %v", got, tc.want)
			}
		})
	}
}
