package service

import "testing"

func TestFolderPickerParsePath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config map[string]any
		want   string
	}{
		{
			name:   "prefer single path field",
			config: map[string]any{"path": "/data/new", "paths": []any{"/data/old"}},
			want:   "/data/new",
		},
		{
			name:   "fallback to legacy array",
			config: map[string]any{"paths": []any{"/data/old"}},
			want:   "/data/old",
		},
		{
			name:   "empty when missing",
			config: map[string]any{},
			want:   "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := folderPickerParsePath(tt.config)
			if got != tt.want {
				t.Fatalf("folderPickerParsePath() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestFolderPickerParseSavedFolderID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		config map[string]any
		want   string
	}{
		{
			name:   "prefer single saved folder id",
			config: map[string]any{"saved_folder_id": "new-id", "saved_folder_ids": []any{"old-id"}, "folder_ids": []any{"legacy-id"}},
			want:   "new-id",
		},
		{
			name:   "fallback to saved folder ids",
			config: map[string]any{"saved_folder_ids": []any{"old-id"}, "folder_ids": []any{"legacy-id"}},
			want:   "old-id",
		},
		{
			name:   "fallback to legacy folder ids",
			config: map[string]any{"folder_ids": []any{"legacy-id"}},
			want:   "legacy-id",
		},
		{
			name:   "empty when missing",
			config: map[string]any{},
			want:   "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got := folderPickerParseSavedFolderID(tt.config)
			if got != tt.want {
				t.Fatalf("folderPickerParseSavedFolderID() = %q, want %q", got, tt.want)
			}
		})
	}
}
