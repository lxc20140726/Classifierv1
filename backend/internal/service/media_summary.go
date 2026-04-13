package service

import "strings"

type mediaSummary struct {
	imageCount     int
	videoCount     int
	otherFileCount int
	totalFiles     int
	hasImage       bool
	hasVideo       bool
	hasManga       bool
	hasOtherFiles  bool
}

func summarizeFolderTreeMedia(tree FolderTree) mediaSummary {
	summary := summarizeFolderFiles(tree.Name, tree.Files)

	for _, child := range tree.Subdirs {
		summary = mergeMediaSummary(summary, summarizeFolderTreeMedia(child))
	}

	return summary
}

func summarizeCurrentFolderMedia(tree FolderTree) mediaSummary {
	return summarizeFolderFiles(tree.Name, tree.Files)
}

func summarizeClassifiedEntryMedia(entry ClassifiedEntry) mediaSummary {
	summary := summarizeFolderFiles("", entry.Files)

	for _, child := range entry.Subtree {
		summary = mergeMediaSummary(summary, summarizeClassifiedEntryMedia(child))
	}

	if entry.HasOtherFiles {
		summary.hasOtherFiles = true
	}

	hasExplicitTreeData := len(entry.Files) > 0 || len(entry.Subtree) > 0
	if !hasExplicitTreeData {
		switch strings.TrimSpace(entry.Category) {
		case "photo":
			summary.hasImage = true
		case "video":
			summary.hasVideo = true
		case "mixed":
			summary.hasImage = true
			summary.hasVideo = true
		case "manga":
			summary.hasManga = true
		}
	}

	return summary
}

func summarizeFolderFiles(name string, files []FileEntry) mediaSummary {
	summary := mediaSummary{}
	if hasMangaKeyword(name) {
		summary.hasManga = true
	}

	for _, file := range files {
		summary.totalFiles++
		ext := strings.ToLower(strings.TrimSpace(file.Ext))
		if ext == "" {
			ext = strings.ToLower(filepathExt(file.Name))
		}

		switch {
		case mangaExts[ext]:
			summary.hasManga = true
		case imageExts[ext]:
			summary.imageCount++
			summary.hasImage = true
		case videoExts[ext]:
			summary.videoCount++
			summary.hasVideo = true
		default:
			summary.otherFileCount++
			summary.hasOtherFiles = true
		}
	}

	return summary
}

func mergeMediaSummary(base mediaSummary, next mediaSummary) mediaSummary {
	base.imageCount += next.imageCount
	base.videoCount += next.videoCount
	base.otherFileCount += next.otherFileCount
	base.totalFiles += next.totalFiles
	base.hasImage = base.hasImage || next.hasImage
	base.hasVideo = base.hasVideo || next.hasVideo
	base.hasManga = base.hasManga || next.hasManga
	base.hasOtherFiles = base.hasOtherFiles || next.hasOtherFiles
	return base
}

func hasMangaKeyword(name string) bool {
	nameLower := strings.ToLower(strings.TrimSpace(name))
	for _, keyword := range mangaKeywords {
		if strings.Contains(nameLower, strings.ToLower(keyword)) {
			return true
		}
	}
	return false
}

func filepathExt(name string) string {
	lastDot := strings.LastIndex(name, ".")
	if lastDot <= 0 || lastDot == len(name)-1 {
		return ""
	}
	return name[lastDot:]
}
