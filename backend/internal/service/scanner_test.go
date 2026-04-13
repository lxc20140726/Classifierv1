package service

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	dbpkg "github.com/liqiye/classifier/internal/db"
	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/sse"
)

type testFSAdapter struct {
	*fs.MockAdapter
	stats   map[string]fs.FileInfo
	readErr map[string]error
	statErr map[string]error
}

func newTestFSAdapter() *testFSAdapter {
	return &testFSAdapter{
		MockAdapter: fs.NewMockAdapter(),
		stats:       make(map[string]fs.FileInfo),
		readErr:     make(map[string]error),
		statErr:     make(map[string]error),
	}
}

func (a *testFSAdapter) AddFile(path string, size int64) {
	a.stats[path] = fs.FileInfo{
		Name:    filepath.Base(path),
		IsDir:   false,
		Size:    size,
		ModTime: time.Now().UTC(),
	}
}

func (a *testFSAdapter) ReadDir(ctx context.Context, path string) ([]fs.DirEntry, error) {
	if err, ok := a.readErr[path]; ok {
		return nil, err
	}

	return a.MockAdapter.ReadDir(ctx, path)
}

func (a *testFSAdapter) Stat(ctx context.Context, path string) (fs.FileInfo, error) {
	if err, ok := a.statErr[path]; ok {
		return fs.FileInfo{}, err
	}

	if info, ok := a.stats[path]; ok {
		return info, nil
	}

	return a.MockAdapter.Stat(ctx, path)
}

func TestScan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		setup     func(t *testing.T, adapter *testFSAdapter, sourceDir string)
		wantCount int
		wantErr   bool
		assert    func(t *testing.T, repo repository.FolderRepository, sourceDir string)
	}{
		{
			name: "scans one photo folder successfully",
			setup: func(t *testing.T, adapter *testFSAdapter, sourceDir string) {
				t.Helper()

				photosPath := filepath.Join(sourceDir, "photos")
				adapter.AddDir(sourceDir, []fs.DirEntry{{Name: "photos", IsDir: true}})
				adapter.AddDir(photosPath, []fs.DirEntry{
					{Name: "a.jpg", IsDir: false},
					{Name: "b.png", IsDir: false},
					{Name: "readme.txt", IsDir: false},
				})
				adapter.AddFile(filepath.Join(photosPath, "a.jpg"), 100)
				adapter.AddFile(filepath.Join(photosPath, "b.png"), 200)
				adapter.AddFile(filepath.Join(photosPath, "readme.txt"), 50)
			},
			wantCount: 1,
			wantErr:   false,
			assert: func(t *testing.T, repo repository.FolderRepository, sourceDir string) {
				t.Helper()

				path := filepath.Join(sourceDir, "photos")
				folder, err := repo.GetByPath(context.Background(), path)
				if err != nil {
					t.Fatalf("GetByPath(%q) error = %v", path, err)
				}

				if folder.ID == "" {
					t.Fatalf("folder.ID is empty")
				}

				if folder.Category != "photo" {
					t.Fatalf("folder.Category = %q, want photo", folder.Category)
				}

				if folder.CategorySource != "auto" || folder.Status != "pending" {
					t.Fatalf("source/status = %q/%q, want auto/pending", folder.CategorySource, folder.Status)
				}

				if folder.ImageCount != 2 || folder.VideoCount != 0 || folder.TotalFiles != 3 {
					t.Fatalf("counts = image:%d video:%d total:%d, want 2/0/3", folder.ImageCount, folder.VideoCount, folder.TotalFiles)
				}
				if folder.OtherFileCount != 1 || !folder.HasOtherFiles {
					t.Fatalf("other stats = count:%d has:%v, want 1/true", folder.OtherFileCount, folder.HasOtherFiles)
				}

				if folder.TotalSize != 350 {
					t.Fatalf("folder.TotalSize = %d, want 350", folder.TotalSize)
				}

				if folder.MarkedForMove {
					t.Fatalf("folder.MarkedForMove = true, want false")
				}
			},
		},
		{
			name: "scans mixed media folder successfully",
			setup: func(t *testing.T, adapter *testFSAdapter, sourceDir string) {
				t.Helper()

				mixedPath := filepath.Join(sourceDir, "events")
				adapter.AddDir(sourceDir, []fs.DirEntry{{Name: "events", IsDir: true}})
				adapter.AddDir(mixedPath, []fs.DirEntry{
					{Name: "clip.mp4", IsDir: false},
					{Name: "cover.jpeg", IsDir: false},
				})
				adapter.AddFile(filepath.Join(mixedPath, "clip.mp4"), 300)
				adapter.AddFile(filepath.Join(mixedPath, "cover.jpeg"), 120)
			},
			wantCount: 1,
			assert: func(t *testing.T, repo repository.FolderRepository, sourceDir string) {
				t.Helper()

				path := filepath.Join(sourceDir, "events")
				folder, err := repo.GetByPath(context.Background(), path)
				if err != nil {
					t.Fatalf("GetByPath(%q) error = %v", path, err)
				}

				if folder.Category != "mixed" {
					t.Fatalf("folder.Category = %q, want mixed", folder.Category)
				}

				if folder.ImageCount != 1 || folder.VideoCount != 1 || folder.TotalFiles != 2 {
					t.Fatalf("counts = image:%d video:%d total:%d, want 1/1/2", folder.ImageCount, folder.VideoCount, folder.TotalFiles)
				}
				if folder.OtherFileCount != 0 || folder.HasOtherFiles {
					t.Fatalf("other stats = count:%d has:%v, want 0/false", folder.OtherFileCount, folder.HasOtherFiles)
				}

				if folder.TotalSize != 420 {
					t.Fatalf("folder.TotalSize = %d, want 420", folder.TotalSize)
				}
			},
		},
		{
			name: "scans nested files recursively",
			setup: func(t *testing.T, adapter *testFSAdapter, sourceDir string) {
				t.Helper()

				parentPath := filepath.Join(sourceDir, "album")
				childPath := filepath.Join(parentPath, "set-a")
				adapter.AddDir(sourceDir, []fs.DirEntry{{Name: "album", IsDir: true}})
				adapter.AddDir(parentPath, []fs.DirEntry{{Name: "set-a", IsDir: true}})
				adapter.AddDir(childPath, []fs.DirEntry{
					{Name: "nested.mp4", IsDir: false},
					{Name: "nested.jpg", IsDir: false},
				})
				adapter.AddFile(filepath.Join(childPath, "nested.mp4"), 1000)
				adapter.AddFile(filepath.Join(childPath, "nested.jpg"), 500)
			},
			wantCount: 1,
			assert: func(t *testing.T, repo repository.FolderRepository, sourceDir string) {
				t.Helper()

				path := filepath.Join(sourceDir, "album")
				folder, err := repo.GetByPath(context.Background(), path)
				if err != nil {
					t.Fatalf("GetByPath(%q) error = %v", path, err)
				}
				if folder.TotalSize != 1500 {
					t.Fatalf("folder.TotalSize = %d, want 1500", folder.TotalSize)
				}
				if folder.TotalFiles != 2 || folder.ImageCount != 1 || folder.VideoCount != 1 {
					t.Fatalf("counts = image:%d video:%d total:%d, want 1/1/2", folder.ImageCount, folder.VideoCount, folder.TotalFiles)
				}
				if folder.Category != "mixed" {
					t.Fatalf("folder.Category = %q, want mixed", folder.Category)
				}
			},
		},
		{
			name: "skips configured output directories during scan",
			setup: func(t *testing.T, adapter *testFSAdapter, sourceDir string) {
				t.Helper()

				albumPath := filepath.Join(sourceDir, "album")
				outputPath := filepath.Join(sourceDir, "output")
				adapter.AddDir(sourceDir, []fs.DirEntry{
					{Name: "album", IsDir: true},
					{Name: "output", IsDir: true},
				})
				adapter.AddDir(albumPath, []fs.DirEntry{{Name: "a.jpg", IsDir: false}})
				adapter.AddDir(outputPath, []fs.DirEntry{{Name: "b.jpg", IsDir: false}})
				adapter.AddFile(filepath.Join(albumPath, "a.jpg"), 120)
				adapter.AddFile(filepath.Join(outputPath, "b.jpg"), 220)
			},
			wantCount: 1,
			assert: func(t *testing.T, repo repository.FolderRepository, sourceDir string) {
				t.Helper()

				albumPath := filepath.Join(sourceDir, "album")
				outputPath := filepath.Join(sourceDir, "output")
				if _, err := repo.GetByPath(context.Background(), albumPath); err != nil {
					t.Fatalf("expected album folder to be scanned: %v", err)
				}
				if _, err := repo.GetByPath(context.Background(), outputPath); err == nil {
					t.Fatalf("expected output directory to be excluded from scan")
				}
			},
		},
		{
			name: "suppresses existing records under excluded output directories",
			setup: func(t *testing.T, adapter *testFSAdapter, sourceDir string) {
				t.Helper()

				albumPath := filepath.Join(sourceDir, "album")
				outputPath := filepath.Join(sourceDir, "video")
				outputFilePath := filepath.Join(outputPath, "clip.mp4")
				adapter.AddDir(sourceDir, []fs.DirEntry{
					{Name: "album", IsDir: true},
					{Name: "video", IsDir: true},
				})
				adapter.AddDir(albumPath, []fs.DirEntry{{Name: "a.jpg", IsDir: false}})
				adapter.AddDir(outputPath, []fs.DirEntry{{Name: "clip.mp4", IsDir: false}})
				adapter.AddFile(filepath.Join(albumPath, "a.jpg"), 120)
				adapter.AddFile(outputFilePath, 220)
			},
			wantCount: 1,
			assert: func(t *testing.T, repo repository.FolderRepository, sourceDir string) {
				t.Helper()

				outputPath := filepath.Join(sourceDir, "video")
				outputFilePath := filepath.Join(outputPath, "clip.mp4")

				outputFolder, err := repo.GetByID(context.Background(), "existing-output-folder")
				if err != nil {
					t.Fatalf("GetByID(existing-output-folder) error = %v", err)
				}
				if outputFolder.DeletedAt == nil {
					t.Fatalf("expected excluded output directory record to be suppressed")
				}

				outputFile, err := repo.GetByID(context.Background(), "existing-output-file")
				if err != nil {
					t.Fatalf("GetByID(existing-output-file) error = %v", err)
				}
				if outputFile.DeletedAt == nil {
					t.Fatalf("expected excluded output file record to be suppressed")
				}

				if _, err := repo.GetByPath(context.Background(), outputPath); err == nil {
					t.Fatalf("expected output directory to be hidden from current list")
				}
				if _, err := repo.GetByPath(context.Background(), outputFilePath); err == nil {
					t.Fatalf("expected output file record to be hidden from current list")
				}
			},
		},
		{
			name: "parent directory becomes mixed from photo and video subtrees",
			setup: func(t *testing.T, adapter *testFSAdapter, sourceDir string) {
				t.Helper()

				rootPath := filepath.Join(sourceDir, "library")
				photoPath := filepath.Join(rootPath, "photos")
				videoPath := filepath.Join(rootPath, "videos")
				adapter.AddDir(sourceDir, []fs.DirEntry{{Name: "library", IsDir: true}})
				adapter.AddDir(rootPath, []fs.DirEntry{
					{Name: "photos", IsDir: true},
					{Name: "videos", IsDir: true},
				})
				adapter.AddDir(photoPath, []fs.DirEntry{{Name: "a.jpg", IsDir: false}})
				adapter.AddDir(videoPath, []fs.DirEntry{{Name: "b.mp4", IsDir: false}, {Name: "note.txt", IsDir: false}})
				adapter.AddFile(filepath.Join(photoPath, "a.jpg"), 120)
				adapter.AddFile(filepath.Join(videoPath, "b.mp4"), 500)
				adapter.AddFile(filepath.Join(videoPath, "note.txt"), 20)
			},
			wantCount: 1,
			assert: func(t *testing.T, repo repository.FolderRepository, sourceDir string) {
				t.Helper()
				path := filepath.Join(sourceDir, "library")
				folder, err := repo.GetByPath(context.Background(), path)
				if err != nil {
					t.Fatalf("GetByPath(%q) error = %v", path, err)
				}
				if folder.Category != "mixed" {
					t.Fatalf("folder.Category = %q, want mixed", folder.Category)
				}
				if folder.ImageCount != 1 || folder.VideoCount != 1 || folder.OtherFileCount != 1 || folder.TotalFiles != 3 {
					t.Fatalf("counts = image:%d video:%d other:%d total:%d, want 1/1/1/3", folder.ImageCount, folder.VideoCount, folder.OtherFileCount, folder.TotalFiles)
				}
				if !folder.HasOtherFiles {
					t.Fatalf("folder.HasOtherFiles = false, want true")
				}
			},
		},
		{
			name: "skips non-directory entries in source root and returns processed count",
			setup: func(t *testing.T, adapter *testFSAdapter, sourceDir string) {
				t.Helper()

				first := filepath.Join(sourceDir, "folder-a")
				second := filepath.Join(sourceDir, "folder-b")

				adapter.AddDir(sourceDir, []fs.DirEntry{
					{Name: "folder-a", IsDir: true},
					{Name: "readme.txt", IsDir: false},
					{Name: "folder-b", IsDir: true},
				})

				adapter.AddDir(first, []fs.DirEntry{{Name: "a.jpg", IsDir: false}})
				adapter.AddFile(filepath.Join(first, "a.jpg"), 10)

				adapter.AddDir(second, []fs.DirEntry{{Name: "b.mp4", IsDir: false}})
				adapter.AddFile(filepath.Join(second, "b.mp4"), 20)
			},
			wantCount: 2,
			assert: func(t *testing.T, repo repository.FolderRepository, sourceDir string) {
				t.Helper()

				if _, err := repo.GetByPath(context.Background(), filepath.Join(sourceDir, "readme.txt")); err == nil {
					t.Fatalf("expected root file entry to be skipped")
				}
			},
		},
		{
			name: "skips suppressed folders during discovery",
			setup: func(t *testing.T, adapter *testFSAdapter, sourceDir string) {
				t.Helper()

				visiblePath := filepath.Join(sourceDir, "visible")
				hiddenPath := filepath.Join(sourceDir, "hidden")
				adapter.AddDir(sourceDir, []fs.DirEntry{{Name: "visible", IsDir: true}, {Name: "hidden", IsDir: true}})
				adapter.AddDir(visiblePath, []fs.DirEntry{{Name: "a.jpg", IsDir: false}})
				adapter.AddDir(hiddenPath, []fs.DirEntry{{Name: "b.jpg", IsDir: false}})
				adapter.AddFile(filepath.Join(visiblePath, "a.jpg"), 10)
				adapter.AddFile(filepath.Join(hiddenPath, "b.jpg"), 10)
			},
			wantCount: 1,
			wantErr:   false,
			assert: func(t *testing.T, repo repository.FolderRepository, sourceDir string) {
				t.Helper()
				hiddenPath := filepath.Join(sourceDir, "hidden")
				visiblePath := filepath.Join(sourceDir, "visible")
				if _, err := repo.GetByPath(context.Background(), visiblePath); err != nil {
					t.Fatalf("expected visible folder to be scanned: %v", err)
				}
				if _, err := repo.GetByPath(context.Background(), hiddenPath); err == nil {
					t.Fatalf("expected suppressed folder to be skipped")
				}
			},
		},
		{
			name: "propagates read errors",
			setup: func(t *testing.T, adapter *testFSAdapter, sourceDir string) {
				t.Helper()

				adapter.AddDir(sourceDir, []fs.DirEntry{{Name: "broken", IsDir: true}})
				adapter.readErr[filepath.Join(sourceDir, "broken")] = fmt.Errorf("boom")
			},
			wantCount: 0,
			wantErr:   false,
		},
		{
			name: "propagates stat errors",
			setup: func(t *testing.T, adapter *testFSAdapter, sourceDir string) {
				t.Helper()

				brokenPath := filepath.Join(sourceDir, "broken")
				adapter.AddDir(sourceDir, []fs.DirEntry{{Name: "broken", IsDir: true}})
				adapter.AddDir(brokenPath, []fs.DirEntry{{Name: "missing.jpg", IsDir: false}})
				adapter.statErr[filepath.Join(brokenPath, "missing.jpg")] = fmt.Errorf("missing stat")
			},
			wantCount: 0,
			wantErr:   false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			database := newServiceTestDB(t)
			repo := repository.NewFolderRepository(database)
			adapter := newTestFSAdapter()
			sourceDir := filepath.Join("/library", tc.name)

			tc.setup(t, adapter, sourceDir)

			jobRepo := repository.NewJobRepository(database)
			if tc.name == "skips suppressed folders during discovery" {
				hiddenPath := filepath.Join(sourceDir, "hidden")
				hiddenTime := time.Now().UTC()
				if err := repo.Upsert(context.Background(), &repository.Folder{
					ID:             "suppressed-hidden-folder",
					Path:           hiddenPath,
					SourceDir:      sourceDir,
					RelativePath:   "hidden",
					Name:           "hidden",
					Category:       "other",
					CategorySource: "auto",
					Status:         "pending",
					DeletedAt:      &hiddenTime,
				}); err != nil {
					t.Fatalf("seed suppressed folder error = %v", err)
				}
			}
			if tc.name == "suppresses existing records under excluded output directories" {
				now := time.Now().UTC()
				if err := repo.Upsert(context.Background(), &repository.Folder{
					ID:             "existing-output-folder",
					Path:           filepath.Join(sourceDir, "video"),
					SourceDir:      sourceDir,
					RelativePath:   "video",
					Name:           "video",
					Category:       "mixed",
					CategorySource: "workflow",
					Status:         "pending",
					ScannedAt:      now,
					UpdatedAt:      now,
				}); err != nil {
					t.Fatalf("seed output folder error = %v", err)
				}
				if err := repo.Upsert(context.Background(), &repository.Folder{
					ID:             "existing-output-file",
					Path:           filepath.Join(sourceDir, "video", "clip.mp4"),
					SourceDir:      sourceDir,
					RelativePath:   filepath.Join("video", "clip.mp4"),
					Name:           "clip.mp4",
					Category:       "video",
					CategorySource: "workflow",
					Status:         "pending",
					ScannedAt:      now,
					UpdatedAt:      now,
				}); err != nil {
					t.Fatalf("seed output file error = %v", err)
				}
			}
			snapshotRepo := repository.NewSnapshotRepository(database)
			auditRepo := repository.NewAuditRepository(database)
			auditSvc := NewAuditService(auditRepo)
			snapshotSvc := NewSnapshotService(adapter, snapshotRepo, repo)
			scanner := NewScannerService(adapter, repo, jobRepo, snapshotSvc, auditSvc, nil)
			scanInput := ScanInput{SourceDirs: []string{sourceDir}}
			if tc.name == "skips configured output directories during scan" {
				scanInput.ExcludeDirs = []string{filepath.Join(sourceDir, "output")}
			}
			if tc.name == "suppresses existing records under excluded output directories" {
				scanInput.ExcludeDirs = []string{filepath.Join(sourceDir, "video")}
			}
			gotCount, err := scanner.Scan(context.Background(), scanInput)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Scan() error = %v, wantErr %v", err, tc.wantErr)
			}

			if gotCount != tc.wantCount {
				logs, _, _ := auditRepo.List(context.Background(), repository.AuditListFilter{Page: 1, Limit: 20})
				if len(logs) > 0 {
					t.Fatalf("Scan() count = %d, want %d, err = %v, first audit = action:%s result:%s error:%s detail:%s", gotCount, tc.wantCount, err, logs[0].Action, logs[0].Result, logs[0].ErrorMsg, string(logs[0].Detail))
				}
				t.Fatalf("Scan() count = %d, want %d, err = %v, audits empty", gotCount, tc.wantCount, err)
			}

			if tc.assert != nil && err == nil {
				tc.assert(t, repo, sourceDir)
			}
		})
	}
}

func TestScanReusesFolderIdentityAcrossHistoricalPathMatches(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	repo := repository.NewFolderRepository(database)
	adapter := newTestFSAdapter()
	jobRepo := repository.NewJobRepository(database)
	snapshotRepo := repository.NewSnapshotRepository(database)
	auditRepo := repository.NewAuditRepository(database)
	auditSvc := NewAuditService(auditRepo)
	snapshotSvc := NewSnapshotService(adapter, snapshotRepo, repo)
	scanner := NewScannerService(adapter, repo, jobRepo, snapshotSvc, auditSvc, nil)

	sourceDir := "/library"
	firstPath := filepath.Join(sourceDir, "photos")
	currentPath := filepath.Join("/archive", "photos")

	adapter.AddDir(sourceDir, []fs.DirEntry{{Name: "photos", IsDir: true}})
	adapter.AddDir(firstPath, []fs.DirEntry{{Name: "a.jpg", IsDir: false}})
	adapter.AddFile(filepath.Join(firstPath, "a.jpg"), 10)

	if got, err := scanner.Scan(context.Background(), ScanInput{SourceDirs: []string{sourceDir}}); err != nil || got != 1 {
		t.Fatalf("first Scan() = (%d, %v), want (1, nil)", got, err)
	}

	firstFolder, err := repo.GetCurrentByPath(context.Background(), firstPath)
	if err != nil {
		t.Fatalf("GetCurrentByPath(first) error = %v", err)
	}

	if err := repo.UpdatePath(context.Background(), firstFolder.ID, currentPath, "/archive", "photos"); err != nil {
		t.Fatalf("UpdatePath() error = %v", err)
	}

	adapter.AddDir(sourceDir, []fs.DirEntry{{Name: "photos", IsDir: true}})
	adapter.AddDir(firstPath, []fs.DirEntry{{Name: "a.jpg", IsDir: false}})
	adapter.AddFile(filepath.Join(firstPath, "a.jpg"), 10)

	if got, err := scanner.Scan(context.Background(), ScanInput{SourceDirs: []string{sourceDir}}); err != nil || got != 1 {
		t.Fatalf("second Scan() = (%d, %v), want (1, nil)", got, err)
	}

	secondFolder, err := repo.GetCurrentByPath(context.Background(), firstPath)
	if err != nil {
		t.Fatalf("GetCurrentByPath(firstPath again) error = %v", err)
	}
	if secondFolder.ID != firstFolder.ID {
		t.Fatalf("folder ID changed from %q to %q", firstFolder.ID, secondFolder.ID)
	}

	historical, err := repo.GetByHistoricalPath(context.Background(), firstPath)
	if err != nil {
		t.Fatalf("GetByHistoricalPath(first) error = %v", err)
	}
	if historical.ID != firstFolder.ID {
		t.Fatalf("historical folder ID = %q, want %q", historical.ID, firstFolder.ID)
	}
}

func TestScanUpdatesIdentityFingerprintWhenContentChanges(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	repo := repository.NewFolderRepository(database)
	adapter := newTestFSAdapter()
	jobRepo := repository.NewJobRepository(database)
	snapshotRepo := repository.NewSnapshotRepository(database)
	auditRepo := repository.NewAuditRepository(database)
	auditSvc := NewAuditService(auditRepo)
	snapshotSvc := NewSnapshotService(adapter, snapshotRepo, repo)
	scanner := NewScannerService(adapter, repo, jobRepo, snapshotSvc, auditSvc, nil)

	sourceDir := "/library"
	folderPath := filepath.Join(sourceDir, "album")
	filePath := filepath.Join(folderPath, "a.jpg")

	adapter.AddDir(sourceDir, []fs.DirEntry{{Name: "album", IsDir: true}})
	adapter.AddDir(folderPath, []fs.DirEntry{{Name: "a.jpg", IsDir: false}})
	adapter.AddFile(filePath, 10)

	if got, err := scanner.Scan(context.Background(), ScanInput{SourceDirs: []string{sourceDir}}); err != nil || got != 1 {
		t.Fatalf("first Scan() = (%d, %v), want (1, nil)", got, err)
	}

	first, err := repo.GetCurrentByPath(context.Background(), folderPath)
	if err != nil {
		t.Fatalf("GetCurrentByPath(first) error = %v", err)
	}
	if first.IdentityFingerprint == "" {
		t.Fatalf("first identity_fingerprint is empty")
	}

	adapter.AddFile(filePath, 20)
	if got, err := scanner.Scan(context.Background(), ScanInput{SourceDirs: []string{sourceDir}}); err != nil || got != 1 {
		t.Fatalf("second Scan() = (%d, %v), want (1, nil)", got, err)
	}

	second, err := repo.GetCurrentByPath(context.Background(), folderPath)
	if err != nil {
		t.Fatalf("GetCurrentByPath(second) error = %v", err)
	}
	if second.IdentityFingerprint == "" {
		t.Fatalf("second identity_fingerprint is empty")
	}
	if second.IdentityFingerprint == first.IdentityFingerprint {
		t.Fatalf("identity_fingerprint unchanged: %q", second.IdentityFingerprint)
	}
}

func TestScanPublishesFolderClassificationUpdatedEvent(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	repo := repository.NewFolderRepository(database)
	adapter := newTestFSAdapter()
	jobRepo := repository.NewJobRepository(database)
	snapshotRepo := repository.NewSnapshotRepository(database)
	auditRepo := repository.NewAuditRepository(database)
	auditSvc := NewAuditService(auditRepo)
	snapshotSvc := NewSnapshotService(adapter, snapshotRepo, repo)
	broker := sse.NewBroker()
	events := broker.Subscribe()
	defer broker.Unsubscribe(events)

	sourceDir := "/library"
	albumPath := filepath.Join(sourceDir, "album")
	adapter.AddDir(sourceDir, []fs.DirEntry{{Name: "album", IsDir: true}})
	adapter.AddDir(albumPath, []fs.DirEntry{{Name: "a.jpg", IsDir: false}})
	adapter.AddFile(filepath.Join(albumPath, "a.jpg"), 12)

	scanner := NewScannerService(adapter, repo, jobRepo, snapshotSvc, auditSvc, broker)
	if _, err := scanner.Scan(context.Background(), ScanInput{SourceDirs: []string{sourceDir}}); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	timeout := time.After(3 * time.Second)
	for {
		select {
		case <-timeout:
			t.Fatal("timed out waiting folder.classification.updated event")
		case evt := <-events:
			if evt.Type != "folder.classification.updated" {
				continue
			}
			var payload struct {
				FolderID             string `json:"folder_id"`
				JobID                string `json:"job_id"`
				WorkflowRunID        string `json:"workflow_run_id"`
				FolderName           string `json:"folder_name"`
				FolderPath           string `json:"folder_path"`
				SourceDir            string `json:"source_dir"`
				RelativePath         string `json:"relative_path"`
				Category             string `json:"category"`
				CategorySource       string `json:"category_source"`
				ClassificationStatus string `json:"classification_status"`
				NodeID               string `json:"node_id"`
				NodeType             string `json:"node_type"`
				Error                string `json:"error"`
				UpdatedAt            string `json:"updated_at"`
			}
			if err := json.Unmarshal(evt.Data, &payload); err != nil {
				t.Fatalf("json.Unmarshal(event) error = %v", err)
			}
			if payload.JobID != "" {
				t.Fatalf("job_id = %q, want empty", payload.JobID)
			}
			if payload.WorkflowRunID != "" {
				t.Fatalf("workflow_run_id = %q, want empty", payload.WorkflowRunID)
			}
			if payload.FolderName != "album" {
				t.Fatalf("folder_name = %q, want album", payload.FolderName)
			}
			if payload.FolderPath != albumPath {
				t.Fatalf("folder_path = %q, want %q", payload.FolderPath, albumPath)
			}
			if payload.SourceDir != filepath.Clean(sourceDir) {
				t.Fatalf("source_dir = %q, want %q", payload.SourceDir, filepath.Clean(sourceDir))
			}
			if payload.RelativePath != "album" {
				t.Fatalf("relative_path = %q, want album", payload.RelativePath)
			}
			if payload.Category != "photo" {
				t.Fatalf("category = %q, want photo", payload.Category)
			}
			if payload.CategorySource != "auto" {
				t.Fatalf("category_source = %q, want auto", payload.CategorySource)
			}
			if payload.ClassificationStatus != "scanning" {
				t.Fatalf("classification_status = %q, want scanning", payload.ClassificationStatus)
			}
			if payload.NodeID != "" || payload.NodeType != "" || payload.Error != "" {
				t.Fatalf("node_id/node_type/error = %q/%q/%q, want empty", payload.NodeID, payload.NodeType, payload.Error)
			}
			if payload.UpdatedAt == "" {
				t.Fatalf("updated_at is empty")
			}
			if payload.FolderID == "" {
				t.Fatalf("folder_id is empty")
			}
			return
		}
	}
}

func TestScanErrorPublishesFolderClassificationUpdatedEvent(t *testing.T) {
	t.Parallel()

	database := newServiceTestDB(t)
	repo := repository.NewFolderRepository(database)
	adapter := newTestFSAdapter()
	jobRepo := repository.NewJobRepository(database)
	snapshotRepo := repository.NewSnapshotRepository(database)
	auditRepo := repository.NewAuditRepository(database)
	auditSvc := NewAuditService(auditRepo)
	snapshotSvc := NewSnapshotService(adapter, snapshotRepo, repo)
	broker := sse.NewBroker()
	events := broker.Subscribe()
	defer broker.Unsubscribe(events)

	sourceDir := "/library"
	brokenPath := filepath.Join(sourceDir, "broken")
	adapter.AddDir(sourceDir, []fs.DirEntry{{Name: "broken", IsDir: true}})
	adapter.readErr[brokenPath] = fmt.Errorf("boom")

	if err := jobRepo.Create(context.Background(), &repository.Job{
		ID:     "job-1",
		Type:   "scan",
		Status: "pending",
	}); err != nil {
		t.Fatalf("jobRepo.Create() error = %v", err)
	}

	scanner := NewScannerService(adapter, repo, jobRepo, snapshotSvc, auditSvc, broker)
	if _, err := scanner.Scan(context.Background(), ScanInput{
		JobID:      "job-1",
		SourceDirs: []string{sourceDir},
	}); err != nil {
		t.Fatalf("Scan() error = %v", err)
	}

	wantFolderID := "scan_error__library_broken"
	seenScanError := false
	seenFolderEvent := false
	timeout := time.After(3 * time.Second)

	for !(seenScanError && seenFolderEvent) {
		select {
		case <-timeout:
			t.Fatalf("timed out waiting scan.error and folder.classification.updated events, got scan.error=%v folder.classification.updated=%v", seenScanError, seenFolderEvent)
		case evt := <-events:
			switch evt.Type {
			case "scan.error":
				var payload struct {
					JobID      string `json:"job_id"`
					FolderID   string `json:"folder_id"`
					FolderPath string `json:"folder_path"`
					SourceDir  string `json:"source_dir"`
					Error      string `json:"error"`
				}
				if err := json.Unmarshal(evt.Data, &payload); err != nil {
					t.Fatalf("json.Unmarshal(scan.error) error = %v", err)
				}
				if payload.JobID != "job-1" {
					t.Fatalf("scan.error job_id = %q, want job-1", payload.JobID)
				}
				if payload.FolderID != wantFolderID {
					t.Fatalf("scan.error folder_id = %q, want %q", payload.FolderID, wantFolderID)
				}
				if payload.FolderPath != brokenPath {
					t.Fatalf("scan.error folder_path = %q, want %q", payload.FolderPath, brokenPath)
				}
				if payload.SourceDir != filepath.Clean(sourceDir) {
					t.Fatalf("scan.error source_dir = %q, want %q", payload.SourceDir, filepath.Clean(sourceDir))
				}
				if payload.Error == "" {
					t.Fatalf("scan.error error is empty")
				}
				seenScanError = true
			case "folder.classification.updated":
				var payload struct {
					FolderID             string `json:"folder_id"`
					JobID                string `json:"job_id"`
					WorkflowRunID        string `json:"workflow_run_id"`
					FolderName           string `json:"folder_name"`
					FolderPath           string `json:"folder_path"`
					SourceDir            string `json:"source_dir"`
					RelativePath         string `json:"relative_path"`
					Category             string `json:"category"`
					CategorySource       string `json:"category_source"`
					ClassificationStatus string `json:"classification_status"`
					Error                string `json:"error"`
					UpdatedAt            string `json:"updated_at"`
				}
				if err := json.Unmarshal(evt.Data, &payload); err != nil {
					t.Fatalf("json.Unmarshal(folder.classification.updated) error = %v", err)
				}
				if payload.FolderPath != brokenPath || payload.ClassificationStatus != "failed" {
					continue
				}
				if payload.FolderID != wantFolderID {
					t.Fatalf("folder.classification.updated folder_id = %q, want %q", payload.FolderID, wantFolderID)
				}
				if payload.JobID != "job-1" {
					t.Fatalf("folder.classification.updated job_id = %q, want job-1", payload.JobID)
				}
				if payload.WorkflowRunID != "" {
					t.Fatalf("folder.classification.updated workflow_run_id = %q, want empty", payload.WorkflowRunID)
				}
				if payload.FolderName != "broken" {
					t.Fatalf("folder.classification.updated folder_name = %q, want broken", payload.FolderName)
				}
				if payload.SourceDir != filepath.Clean(sourceDir) {
					t.Fatalf("folder.classification.updated source_dir = %q, want %q", payload.SourceDir, filepath.Clean(sourceDir))
				}
				if payload.RelativePath != "broken" {
					t.Fatalf("folder.classification.updated relative_path = %q, want broken", payload.RelativePath)
				}
				if payload.Category != "other" {
					t.Fatalf("folder.classification.updated category = %q, want other", payload.Category)
				}
				if payload.CategorySource != "auto" {
					t.Fatalf("folder.classification.updated category_source = %q, want auto", payload.CategorySource)
				}
				if payload.Error == "" {
					t.Fatalf("folder.classification.updated error is empty")
				}
				if payload.UpdatedAt == "" {
					t.Fatalf("folder.classification.updated updated_at is empty")
				}
				seenFolderEvent = true
			}
		}
	}
}

var serviceDBCounter uint64

func newServiceTestDB(t *testing.T) *sql.DB {
	t.Helper()

	id := atomic.AddUint64(&serviceDBCounter, 1)
	dsn := fmt.Sprintf("file:classifier_service_%d?cache=shared&mode=memory", id)

	database, err := dbpkg.Open(dsn)
	if err != nil {
		t.Fatalf("db.Open(%q) error = %v", dsn, err)
	}

	t.Cleanup(func() {
		_ = database.Close()
	})

	return database
}
