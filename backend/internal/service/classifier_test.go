package service

import "testing"

func TestClassify(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		folderName string
		fileNames  []string
		want       string
	}{
		{
			name:       "manga keyword in folder name",
			folderName: "My Comic Collection",
			fileNames:  []string{"image.jpg", "video.mp4"},
			want:       "manga",
		},
		{
			name:       "manga extension in files",
			folderName: "regular folder",
			fileNames:  []string{"chapter01.cbz", "cover.jpg"},
			want:       "manga",
		},
		{
			name:       "pure photo folder",
			folderName: "vacation",
			fileNames:  []string{"a.jpg", "b.png", "c.heic", "d.webp"},
			want:       "photo",
		},
		{
			name:       "pure video folder",
			folderName: "movies",
			fileNames:  []string{"a.mp4", "b.mkv", "c.ts"},
			want:       "video",
		},
		{
			name:       "mixed folder",
			folderName: "events",
			fileNames:  []string{"a.jpg", "b.png", "c.mp4", "d.mkv"},
			want:       "mixed",
		},
		{
			name:       "no media files",
			folderName: "docs",
			fileNames:  []string{"readme.txt", "notes.md"},
			want:       "other",
		},
		{
			name:       "single image and many videos still mixed",
			folderName: "mostly videos but mixed",
			fileNames:  []string{"1.mp4", "2.mp4", "3.mp4", "cover.jpg"},
			want:       "mixed",
		},
		{
			name:       "single video and many images still mixed",
			folderName: "mostly photos but mixed",
			fileNames:  []string{"1.jpg", "2.jpg", "3.jpg", "clip.mp4"},
			want:       "mixed",
		},
		{
			name:       "below threshold fallback",
			folderName: "unsupported media-looking files",
			fileNames:  []string{"photo.jfif", "movie.mpeg", "archive.rar"},
			want:       "other",
		},
		{
			name:       "case insensitive extensions",
			folderName: "caps",
			fileNames:  []string{"A.JPEG", "B.MP4"},
			want:       "mixed",
		},
		{
			name:       "manga wins before ratio logic",
			folderName: "ratio would be video",
			fileNames:  []string{"chapter.cbr", "a.mp4", "b.mp4", "c.mp4", "d.mp4"},
			want:       "manga",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := Classify(tc.folderName, tc.fileNames)
			if got != tc.want {
				t.Fatalf("Classify(%q, %v) = %q, want %q", tc.folderName, tc.fileNames, got, tc.want)
			}
		})
	}
}
