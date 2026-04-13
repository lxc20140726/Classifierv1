package service

import (
	"path/filepath"
	"strings"
)

var imageExts = map[string]bool{
	".jpg":  true,
	".jpeg": true,
	".png":  true,
	".gif":  true,
	".webp": true,
	".bmp":  true,
	".tiff": true,
	".tif":  true,
	".heic": true,
	".heif": true,
	".avif": true,
	".raw":  true,
}

var videoExts = map[string]bool{
	".mp4":  true,
	".mkv":  true,
	".avi":  true,
	".mov":  true,
	".wmv":  true,
	".flv":  true,
	".m4v":  true,
	".ts":   true,
	".rmvb": true,
	".rm":   true,
	".webm": true,
	".3gp":  true,
}

var mangaExts = map[string]bool{
	".cbz": true,
	".cbr": true,
	".cb7": true,
	".cbt": true,
}

var mangaKeywords = []string{"漫画", "comic", "manga"}

type mediaFileStats struct {
	imageCount     int
	videoCount     int
	mangaCount     int
	otherFileCount int
}

func Classify(folderName string, fileNames []string) string {
	folderNameLower := strings.ToLower(folderName)
	for _, keyword := range mangaKeywords {
		if strings.Contains(folderNameLower, strings.ToLower(keyword)) {
			return "manga"
		}
	}

	stats := summarizeMediaFiles(fileNames)
	if stats.mangaCount > 0 {
		return "manga"
	}

	if stats.imageCount > 0 && stats.videoCount > 0 {
		return "mixed"
	}

	if stats.imageCount > 0 {
		return "photo"
	}

	if stats.videoCount > 0 {
		return "video"
	}

	return "other"
}

func summarizeMediaFiles(fileNames []string) mediaFileStats {
	stats := mediaFileStats{}
	for _, fileName := range fileNames {
		ext := strings.ToLower(filepath.Ext(fileName))
		switch {
		case mangaExts[ext]:
			stats.mangaCount++
		case imageExts[ext]:
			stats.imageCount++
		case videoExts[ext]:
			stats.videoCount++
		default:
			stats.otherFileCount++
		}
	}

	return stats
}
