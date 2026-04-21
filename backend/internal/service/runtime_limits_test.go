package service

import "testing"

func TestRuntimeLimitDefaultsAndValidation(t *testing.T) {
	t.Setenv("THUMBNAIL_MAX_PARALLEL", "")
	t.Setenv("COMPRESS_MAX_PARALLEL", "")
	if got := loadThumbnailMaxParallel(); got != 1 {
		t.Fatalf("loadThumbnailMaxParallel() = %d, want 1", got)
	}
	if got := loadCompressMaxParallel(); got != 2 {
		t.Fatalf("loadCompressMaxParallel() = %d, want 2", got)
	}

	t.Setenv("THUMBNAIL_MAX_PARALLEL", "0")
	t.Setenv("COMPRESS_MAX_PARALLEL", "-3")
	if got := loadThumbnailMaxParallel(); got != 1 {
		t.Fatalf("loadThumbnailMaxParallel() with invalid value = %d, want 1", got)
	}
	if got := loadCompressMaxParallel(); got != 2 {
		t.Fatalf("loadCompressMaxParallel() with invalid value = %d, want 2", got)
	}

	t.Setenv("THUMBNAIL_MAX_PARALLEL", "3")
	t.Setenv("COMPRESS_MAX_PARALLEL", "5")
	if got := loadThumbnailMaxParallel(); got != 3 {
		t.Fatalf("loadThumbnailMaxParallel() = %d, want 3", got)
	}
	if got := loadCompressMaxParallel(); got != 5 {
		t.Fatalf("loadCompressMaxParallel() = %d, want 5", got)
	}
}
