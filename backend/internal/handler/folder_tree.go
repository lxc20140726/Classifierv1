package handler

import (
	"errors"
	"net/http"
	"path/filepath"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/liqiye/classifier/internal/repository"
)

type folderClassificationTreeFile struct {
	Name      string `json:"name"`
	Ext       string `json:"ext"`
	Kind      string `json:"kind"`
	SizeBytes int64  `json:"size_bytes"`
}

type folderClassificationTreeEntry struct {
	FolderID       string                          `json:"folder_id"`
	Path           string                          `json:"path"`
	Name           string                          `json:"name"`
	Category       string                          `json:"category"`
	CategorySource string                          `json:"category_source"`
	Status         string                          `json:"status"`
	HasOtherFiles  bool                            `json:"has_other_files"`
	TotalFiles     int                             `json:"total_files"`
	ImageCount     int                             `json:"image_count"`
	VideoCount     int                             `json:"video_count"`
	OtherFileCount int                             `json:"other_file_count"`
	Files          []folderClassificationTreeFile  `json:"files"`
	Subtree        []folderClassificationTreeEntry `json:"subtree"`
}

func (h *FolderHandler) GetClassificationTree(c *gin.Context) {
	id := strings.TrimSpace(c.Param("id"))
	if id == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "folder id is required"})
		return
	}

	rootFolder, err := h.folders.GetByID(c.Request.Context(), id)
	if err != nil {
		if errors.Is(err, repository.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "folder not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get folder"})
		return
	}

	items, err := h.folders.ListByPathPrefix(c.Request.Context(), rootFolder.Path)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list folder subtree"})
		return
	}

	tree, ok := buildFolderClassificationTree(items, rootFolder.Path)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"error": "folder subtree not found"})
		return
	}
	tree = h.filterExcludedTree(c, tree)

	manifestFilesByDir, err := h.buildManifestFilesByDir(c, rootFolder.ID)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to load source manifest"})
		return
	}

	if err := h.populateTreeFiles(c, &tree, manifestFilesByDir); err != nil {
		return
	}

	c.JSON(http.StatusOK, gin.H{"data": tree})
}

func (h *FolderHandler) buildManifestFilesByDir(c *gin.Context, folderID string) (map[string][]folderClassificationTreeFile, error) {
	if h.manifestReader == nil || strings.TrimSpace(folderID) == "" {
		return nil, nil
	}

	manifests, err := h.manifestReader.ListLatestByFolderID(c.Request.Context(), folderID)
	if err != nil {
		return nil, err
	}
	if len(manifests) == 0 {
		return nil, nil
	}

	filesByDir := make(map[string][]folderClassificationTreeFile)
	for _, manifest := range manifests {
		if manifest == nil {
			continue
		}
		sourcePath := normalizeTreePath(manifest.SourcePath)
		if sourcePath == "" {
			continue
		}
		dirPath := normalizeTreePath(filepath.Dir(sourcePath))
		if dirPath == "" || dirPath == "." {
			continue
		}
		name := strings.TrimSpace(manifest.FileName)
		if name == "" {
			name = filepath.Base(sourcePath)
		}
		ext := strings.ToLower(strings.TrimSpace(filepath.Ext(name)))
		filesByDir[dirPath] = append(filesByDir[dirPath], folderClassificationTreeFile{
			Name:      name,
			Ext:       ext,
			Kind:      inferFileKindByExt(ext),
			SizeBytes: manifest.SizeBytes,
		})
	}

	for dirPath := range filesByDir {
		sort.Slice(filesByDir[dirPath], func(i, j int) bool {
			return filesByDir[dirPath][i].Name < filesByDir[dirPath][j].Name
		})
	}

	return filesByDir, nil
}

func (h *FolderHandler) populateTreeFiles(c *gin.Context, node *folderClassificationTreeEntry, manifestFilesByDir map[string][]folderClassificationTreeFile) error {
	if node == nil || h.fs == nil {
		return nil
	}

	if manifestFiles, ok := manifestFilesByDir[normalizeTreePath(node.Path)]; ok {
		node.Files = append([]folderClassificationTreeFile(nil), manifestFiles...)
		for i := range node.Subtree {
			if err := h.populateTreeFiles(c, &node.Subtree[i], manifestFilesByDir); err != nil {
				return err
			}
		}
		return nil
	}

	entries, err := h.fs.ReadDir(c.Request.Context(), node.Path)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to read folder files"})
		return err
	}

	files := make([]folderClassificationTreeFile, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir {
			continue
		}
		name := strings.TrimSpace(entry.Name)
		if name == "" {
			continue
		}
		ext := strings.ToLower(strings.TrimSpace(filepath.Ext(name)))
		sizeBytes := entry.Size
		info, statErr := h.fs.Stat(c.Request.Context(), filepath.Join(node.Path, name))
		if statErr == nil && !info.IsDir {
			sizeBytes = info.Size
		}
		files = append(files, folderClassificationTreeFile{
			Name:      name,
			Ext:       ext,
			Kind:      inferFileKindByExt(ext),
			SizeBytes: sizeBytes,
		})
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Name < files[j].Name
	})
	node.Files = files

	for i := range node.Subtree {
		if err := h.populateTreeFiles(c, &node.Subtree[i], manifestFilesByDir); err != nil {
			return err
		}
	}

	return nil
}

func buildFolderClassificationTree(folders []*repository.Folder, rootPath string) (folderClassificationTreeEntry, bool) {
	trimmedRoot := normalizeTreePath(rootPath)
	if trimmedRoot == "" {
		return folderClassificationTreeEntry{}, false
	}

	sort.Slice(folders, func(i, j int) bool {
		return len(normalizeTreePath(folders[i].Path)) < len(normalizeTreePath(folders[j].Path))
	})

	pathToNode := make(map[string]*folderClassificationTreeEntry, len(folders))
	for _, folder := range folders {
		if folder == nil {
			continue
		}
		path := normalizeTreePath(folder.Path)
		if path == "" {
			continue
		}
		name := strings.TrimSpace(folder.Name)
		if name == "" {
			name = filepath.Base(path)
		}
		category := strings.TrimSpace(folder.Category)
		if category == "" {
			category = "other"
		}
		categorySource := strings.TrimSpace(folder.CategorySource)
		if categorySource == "" {
			categorySource = "auto"
		}
		pathToNode[path] = &folderClassificationTreeEntry{
			FolderID:       folder.ID,
			Path:           path,
			Name:           name,
			Category:       category,
			CategorySource: categorySource,
			Status:         folder.Status,
			HasOtherFiles:  folder.HasOtherFiles,
			TotalFiles:     folder.TotalFiles,
			ImageCount:     folder.ImageCount,
			VideoCount:     folder.VideoCount,
			OtherFileCount: folder.OtherFileCount,
			Files:          []folderClassificationTreeFile{},
			Subtree:        []folderClassificationTreeEntry{},
		}
	}

	rootNode, ok := pathToNode[trimmedRoot]
	if !ok {
		return folderClassificationTreeEntry{}, false
	}

	childrenByParent := make(map[string][]*folderClassificationTreeEntry, len(pathToNode))
	for path, node := range pathToNode {
		if path == trimmedRoot {
			continue
		}
		parentPath := normalizeTreePath(filepath.Dir(path))
		if parentPath == "." || parentPath == path {
			continue
		}
		if _, ok := pathToNode[parentPath]; !ok {
			continue
		}
		childrenByParent[parentPath] = append(childrenByParent[parentPath], node)
	}
	for parentPath := range childrenByParent {
		sort.Slice(childrenByParent[parentPath], func(i, j int) bool {
			return childrenByParent[parentPath][i].Path < childrenByParent[parentPath][j].Path
		})
	}

	var build func(*folderClassificationTreeEntry) folderClassificationTreeEntry
	build = func(node *folderClassificationTreeEntry) folderClassificationTreeEntry {
		if node == nil {
			return folderClassificationTreeEntry{}
		}
		out := *node
		children := childrenByParent[node.Path]
		out.Subtree = make([]folderClassificationTreeEntry, 0, len(children))
		for _, child := range children {
			out.Subtree = append(out.Subtree, build(child))
		}
		return out
	}

	return build(rootNode), true
}

func (h *FolderHandler) filterExcludedTree(c *gin.Context, root folderClassificationTreeEntry) folderClassificationTreeEntry {
	if h.config == nil {
		return root
	}

	appConfig, err := h.config.GetAppConfig(c.Request.Context())
	if err != nil || appConfig == nil {
		return root
	}

	excludeDirs := compactTreePaths(flattenOutputDirsForTree(appConfig.OutputDirs))
	if len(excludeDirs) == 0 {
		return root
	}

	root.Subtree = filterExcludedTreeChildren(root.Subtree, excludeDirs)
	return root
}

func filterExcludedTreeChildren(children []folderClassificationTreeEntry, excludeDirs []string) []folderClassificationTreeEntry {
	if len(children) == 0 {
		return children
	}

	filtered := make([]folderClassificationTreeEntry, 0, len(children))
	for _, child := range children {
		if isExcludedTreePath(child.Path, excludeDirs) {
			continue
		}
		child.Subtree = filterExcludedTreeChildren(child.Subtree, excludeDirs)
		filtered = append(filtered, child)
	}

	return filtered
}

func flattenOutputDirsForTree(outputDirs repository.AppConfigOutputDirs) []string {
	out := make([]string, 0, len(outputDirs.Video)+len(outputDirs.Manga)+len(outputDirs.Photo)+len(outputDirs.Other)+len(outputDirs.Mixed))
	out = append(out, outputDirs.Video...)
	out = append(out, outputDirs.Manga...)
	out = append(out, outputDirs.Photo...)
	out = append(out, outputDirs.Other...)
	out = append(out, outputDirs.Mixed...)
	return out
}

func compactTreePaths(paths []string) []string {
	if len(paths) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(paths))
	out := make([]string, 0, len(paths))
	for _, path := range paths {
		normalized := normalizeTreeComparePath(path)
		if normalized == "" {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}

	return out
}

func isExcludedTreePath(path string, excludeDirs []string) bool {
	normalizedPath := normalizeTreeComparePath(path)
	if normalizedPath == "" {
		return false
	}

	for _, excludedDir := range excludeDirs {
		if normalizedPath == excludedDir {
			return true
		}
		if strings.HasPrefix(normalizedPath, excludedDir+"/") {
			return true
		}
	}

	return false
}

func normalizeTreeComparePath(path string) string {
	normalized := normalizeTreePath(path)
	if normalized == "" {
		return ""
	}
	return strings.ToLower(strings.ReplaceAll(normalized, "\\", "/"))
}

func normalizeTreePath(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	return filepath.ToSlash(filepath.Clean(trimmed))
}

var imageExtSet = map[string]struct{}{
	".jpg": {}, ".jpeg": {}, ".png": {}, ".gif": {}, ".webp": {}, ".bmp": {}, ".avif": {},
}

var videoExtSet = map[string]struct{}{
	".mp4": {}, ".mkv": {}, ".avi": {}, ".mov": {}, ".wmv": {}, ".flv": {}, ".webm": {}, ".m4v": {},
}

var mangaExtSet = map[string]struct{}{
	".zip": {}, ".cbz": {}, ".rar": {}, ".cbr": {}, ".7z": {}, ".cb7": {}, ".tar": {}, ".gz": {},
}

func inferFileKindByExt(ext string) string {
	if _, ok := imageExtSet[ext]; ok {
		return "photo"
	}
	if _, ok := videoExtSet[ext]; ok {
		return "video"
	}
	if _, ok := mangaExtSet[ext]; ok {
		return "manga"
	}
	return "other"
}
