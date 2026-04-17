package repository

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestConfigRepositorySetGet(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewConfigRepository(database)
	ctx := context.Background()

	if err := repo.Set(ctx, "scan.path", "/media"); err != nil {
		t.Fatalf("Set() error = %v", err)
	}

	if err := repo.Set(ctx, "scan.path", "/media/new"); err != nil {
		t.Fatalf("Set(upsert) error = %v", err)
	}

	value, err := repo.Get(ctx, "scan.path")
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}

	if value != "/media/new" {
		t.Fatalf("Get() value = %q, want %q", value, "/media/new")
	}
}

func TestConfigRepositoryGetNotFound(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewConfigRepository(database)

	_, err := repo.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(missing) error = %v, want ErrNotFound", err)
	}
}

func TestConfigRepositorySaveAndGetAppConfig(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewConfigRepository(database)
	ctx := context.Background()

	err := repo.SaveAppConfig(ctx, &AppConfig{
		Version:       2,
		ScanInputDirs: []string{"/mnt/source", "/mnt/source-2"},
		ScanCron:      "0 * * * *",
		OutputDirs: AppConfigOutputDirs{
			Video: []string{"/mnt/out/video", "/mnt/out/video-2"},
			Manga: []string{"/mnt/out/manga"},
			Photo: []string{"/mnt/out/photo"},
			Other: []string{"/mnt/out/other"},
			Mixed: []string{"/mnt/out/mixed"},
		},
	})
	if err != nil {
		t.Fatalf("SaveAppConfig() error = %v", err)
	}

	got, err := repo.GetAppConfig(ctx)
	if err != nil {
		t.Fatalf("GetAppConfig() error = %v", err)
	}

	if got.Version != 2 {
		t.Fatalf("Version = %d, want 2", got.Version)
	}
	if !reflect.DeepEqual(got.ScanInputDirs, []string{"/mnt/source", "/mnt/source-2"}) {
		t.Fatalf("ScanInputDirs = %#v, want [/mnt/source /mnt/source-2]", got.ScanInputDirs)
	}
	if got.ScanCron != "0 * * * *" {
		t.Fatalf("ScanCron = %q, want 0 * * * *", got.ScanCron)
	}
	if !reflect.DeepEqual(got.OutputDirs.Video, []string{"/mnt/out/video", "/mnt/out/video-2"}) {
		t.Fatalf("OutputDirs.Video = %#v, want [/mnt/out/video /mnt/out/video-2]", got.OutputDirs.Video)
	}

	rawScanInputDirs, err := repo.Get(ctx, "scan_input_dirs")
	if err != nil {
		t.Fatalf("Get(scan_input_dirs) error = %v", err)
	}
	if rawScanInputDirs != `["/mnt/source","/mnt/source-2"]` {
		t.Fatalf("scan_input_dirs = %q, want %q", rawScanInputDirs, `["/mnt/source","/mnt/source-2"]`)
	}

	if _, err := repo.Get(ctx, "source_dir"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(source_dir) error = %v, want ErrNotFound", err)
	}
	if _, err := repo.Get(ctx, "target_dir"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(target_dir) error = %v, want ErrNotFound", err)
	}
}

func TestConfigRepositoryGetAppConfigFallsBackToLegacyKV(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewConfigRepository(database)
	ctx := context.Background()

	if err := repo.Set(ctx, "source_dir", "/legacy/source"); err != nil {
		t.Fatalf("Set(source_dir) error = %v", err)
	}
	if err := repo.Set(ctx, "target_dir", "/legacy/target"); err != nil {
		t.Fatalf("Set(target_dir) error = %v", err)
	}
	if err := repo.Set(ctx, "scan_input_dirs", `["/legacy/source","/legacy/source-2"]`); err != nil {
		t.Fatalf("Set(scan_input_dirs) error = %v", err)
	}

	got, err := repo.GetAppConfig(ctx)
	if err != nil {
		t.Fatalf("GetAppConfig() error = %v", err)
	}

	expectedVideoDir := filepath.Join("/legacy/target", "video")
	if !reflect.DeepEqual(got.OutputDirs.Video, []string{expectedVideoDir}) {
		t.Fatalf("OutputDirs.Video = %#v, want [%q]", got.OutputDirs.Video, expectedVideoDir)
	}
	if !reflect.DeepEqual(got.ScanInputDirs, []string{"/legacy/source", "/legacy/source-2"}) {
		t.Fatalf("ScanInputDirs = %#v, want [/legacy/source /legacy/source-2]", got.ScanInputDirs)
	}
}

func TestConfigRepositorySaveAppConfigRejectsInvalidValues(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewConfigRepository(database)

	t.Run("relative scan path", func(t *testing.T) {
		err := repo.SaveAppConfig(context.Background(), &AppConfig{
			ScanInputDirs: []string{"relative/source"},
			OutputDirs: AppConfigOutputDirs{
				Video: []string{"/out/video"},
				Manga: []string{"/out/manga"},
				Photo: []string{"/out/photo"},
				Other: []string{"/out/other"},
				Mixed: []string{"/out/mixed"},
			},
		})
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("SaveAppConfig() error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("relative output dir", func(t *testing.T) {
		err := repo.SaveAppConfig(context.Background(), &AppConfig{
			ScanInputDirs: []string{"/source"},
			OutputDirs: AppConfigOutputDirs{
				Video: []string{"relative/video"},
			},
		})
		if !errors.Is(err, ErrInvalidConfig) {
			t.Fatalf("SaveAppConfig() error = %v, want ErrInvalidConfig", err)
		}
	})

	t.Run("output dirs are deduplicated and empty values cleaned", func(t *testing.T) {
		err := repo.SaveAppConfig(context.Background(), &AppConfig{
			ScanInputDirs: []string{"/source"},
			OutputDirs: AppConfigOutputDirs{
				Video: []string{"  ", "/out/video", "/out/video", " /out/video-2 "},
			},
		})
		if err != nil {
			t.Fatalf("SaveAppConfig() error = %v", err)
		}

		got, getErr := repo.GetAppConfig(context.Background())
		if getErr != nil {
			t.Fatalf("GetAppConfig() error = %v", getErr)
		}
		if !reflect.DeepEqual(got.OutputDirs.Video, []string{"/out/video", "/out/video-2"}) {
			t.Fatalf("OutputDirs.Video = %#v, want [/out/video /out/video-2]", got.OutputDirs.Video)
		}
	})
}

func TestConfigRepositorySaveAppConfigNormalizesNASContainerPaths(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewConfigRepository(database)

	err := repo.SaveAppConfig(context.Background(), &AppConfig{
		ScanInputDirs: []string{`/12T\影视\写真`},
		OutputDirs: AppConfigOutputDirs{
			Video: []string{`/12T\整理\video`},
			Photo: []string{`/tmp/zfsv3/sata14/18500000000/data\整理\photo`},
		},
	})
	if err != nil {
		t.Fatalf("SaveAppConfig() error = %v", err)
	}

	got, err := repo.GetAppConfig(context.Background())
	if err != nil {
		t.Fatalf("GetAppConfig() error = %v", err)
	}
	if !reflect.DeepEqual(got.ScanInputDirs, []string{"/12T/影视/写真"}) {
		t.Fatalf("ScanInputDirs = %#v, want [/12T/影视/写真]", got.ScanInputDirs)
	}
	if !reflect.DeepEqual(got.OutputDirs.Video, []string{"/12T/整理/video"}) {
		t.Fatalf("OutputDirs.Video = %#v, want [/12T/整理/video]", got.OutputDirs.Video)
	}
	if !reflect.DeepEqual(got.OutputDirs.Photo, []string{"/tmp/zfsv3/sata14/18500000000/data/整理/photo"}) {
		t.Fatalf("OutputDirs.Photo = %#v, want normalized zfsv3 path", got.OutputDirs.Photo)
	}
}

func TestConfigRepositorySaveAppConfigRejectsClientUNCPathOnLinux(t *testing.T) {
	t.Parallel()

	if runtime.GOOS == "windows" {
		t.Skip("UNC path validation is Linux container specific")
	}

	database := newTestDB(t)
	repo := NewConfigRepository(database)

	err := repo.SaveAppConfig(context.Background(), &AppConfig{
		ScanInputDirs: []string{`\\ZSPACE\share\影视`},
	})
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("SaveAppConfig() error = %v, want ErrInvalidConfig", err)
	}
	if !strings.Contains(err.Error(), "Docker mounted container path") {
		t.Fatalf("SaveAppConfig() error = %q, want container path hint", err.Error())
	}
}

func TestConfigRepositoryEnsureAppConfig(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewConfigRepository(database)
	ctx := context.Background()

	if err := repo.Set(ctx, "source_dir", "/legacy/source"); err != nil {
		t.Fatalf("Set(source_dir) error = %v", err)
	}

	if err := repo.EnsureAppConfig(ctx); err != nil {
		t.Fatalf("EnsureAppConfig() error = %v", err)
	}

	var rawValue string
	if err := database.QueryRowContext(ctx, "SELECT value FROM app_config WHERE id = 1").Scan(&rawValue); err != nil {
		t.Fatalf("query app_config value error = %v", err)
	}

	var cfg AppConfig
	if err := json.Unmarshal([]byte(rawValue), &cfg); err != nil {
		t.Fatalf("json.Unmarshal(app_config.value) error = %v", err)
	}
	if !reflect.DeepEqual(cfg.ScanInputDirs, []string{"/legacy/source"}) {
		t.Fatalf("cfg.ScanInputDirs = %#v, want [/legacy/source]", cfg.ScanInputDirs)
	}
}

func TestConfigRepositoryGetAppConfigCompatOldOutputDirsString(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewConfigRepository(database)
	ctx := context.Background()

	raw := `{"version":1,"scan_input_dirs":["/source"],"output_dirs":{"video":"/out/video","manga":"/out/manga"}}`
	checksum := checksumHex([]byte(raw))
	_, err := database.ExecContext(
		ctx,
		`INSERT INTO app_config (id, version, value, checksum, updated_at)
VALUES (1, 1, ?, ?, CURRENT_TIMESTAMP)`,
		raw,
		checksum,
	)
	if err != nil {
		t.Fatalf("insert app_config error = %v", err)
	}

	got, err := repo.GetAppConfig(ctx)
	if err != nil {
		t.Fatalf("GetAppConfig() error = %v", err)
	}
	if !reflect.DeepEqual(got.OutputDirs.Video, []string{"/out/video"}) {
		t.Fatalf("OutputDirs.Video = %#v, want [/out/video]", got.OutputDirs.Video)
	}
	if !reflect.DeepEqual(got.OutputDirs.Manga, []string{"/out/manga"}) {
		t.Fatalf("OutputDirs.Manga = %#v, want [/out/manga]", got.OutputDirs.Manga)
	}
}

func TestConfigRepositoryGetAppConfigCompatLegacyKVOutputDirsString(t *testing.T) {
	t.Parallel()

	database := newTestDB(t)
	repo := NewConfigRepository(database)
	ctx := context.Background()

	if err := repo.Set(ctx, "output_dirs", `{"video":"/out/video","mixed":["/out/mixed"]}`); err != nil {
		t.Fatalf("Set(output_dirs) error = %v", err)
	}

	got, err := repo.GetAppConfig(ctx)
	if err != nil {
		t.Fatalf("GetAppConfig() error = %v", err)
	}
	if !reflect.DeepEqual(got.OutputDirs.Video, []string{"/out/video"}) {
		t.Fatalf("OutputDirs.Video = %#v, want [/out/video]", got.OutputDirs.Video)
	}
	if !reflect.DeepEqual(got.OutputDirs.Mixed, []string{"/out/mixed"}) {
		t.Fatalf("OutputDirs.Mixed = %#v, want [/out/mixed]", got.OutputDirs.Mixed)
	}
}
