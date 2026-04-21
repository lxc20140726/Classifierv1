package service

import (
	"os"
	"strconv"
	"strings"
)

const (
	defaultThumbnailMaxParallel = 1
	defaultCompressMaxParallel  = 2
)

func loadThumbnailMaxParallel() int {
	return loadPositiveEnvInt("THUMBNAIL_MAX_PARALLEL", defaultThumbnailMaxParallel)
}

func loadCompressMaxParallel() int {
	return loadPositiveEnvInt("COMPRESS_MAX_PARALLEL", defaultCompressMaxParallel)
}

func loadPositiveEnvInt(key string, fallback int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
