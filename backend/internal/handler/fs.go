package handler

import (
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/liqiye/classifier/internal/fs"
)

type FSHandler struct {
	fsAdapter fs.FSAdapter
}

func NewFSHandler(fsAdapter fs.FSAdapter) *FSHandler {
	return &FSHandler{fsAdapter: fsAdapter}
}

type dirEntry struct {
	Name string `json:"name"`
	Path string `json:"path"`
}

// ListDirs returns immediate subdirectories of `path`.
// If path is empty, it falls back to a platform-appropriate root path.
func (h *FSHandler) ListDirs(c *gin.Context) {
	rawPath := normalizeBrowsePath(c.Query("path"))

	// Normalise and clean to prevent path-traversal tricks.
	cleanPath := filepath.Clean(rawPath)
	if !filepath.IsAbs(cleanPath) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "path must be absolute"})
		return
	}

	ctx := c.Request.Context()

	info, err := h.fsAdapter.Stat(ctx, cleanPath)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "path not accessible: " + err.Error()})
		return
	}
	if !info.IsDir {
		c.JSON(http.StatusBadRequest, gin.H{"error": "path is not a directory"})
		return
	}

	entries, err := h.fsAdapter.ReadDir(ctx, cleanPath)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to list directory"})
		return
	}

	dirs := make([]dirEntry, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir {
			continue
		}
		// Skip hidden directories.
		if strings.HasPrefix(entry.Name, ".") {
			continue
		}
		dirs = append(dirs, dirEntry{
			Name: entry.Name,
			Path: filepath.Join(cleanPath, entry.Name),
		})
	}

	sort.Slice(dirs, func(i, j int) bool {
		return dirs[i].Name < dirs[j].Name
	})

	parent := filepath.Dir(cleanPath)
	if parent == cleanPath {
		parent = ""
	}

	c.JSON(http.StatusOK, gin.H{
		"path":    cleanPath,
		"parent":  parent,
		"entries": dirs,
	})
}

func normalizeBrowsePath(rawPath string) string {
	trimmed := strings.TrimSpace(rawPath)
	if trimmed == "" {
		return defaultBrowseRoot()
	}

	// On Windows users may still input "/" from old UI defaults.
	if runtime.GOOS == "windows" && (trimmed == "/" || trimmed == `\`) {
		return defaultBrowseRoot()
	}

	return trimmed
}

func defaultBrowseRoot() string {
	if runtime.GOOS != "windows" {
		return "/"
	}

	if cwd, err := os.Getwd(); err == nil {
		if volume := filepath.VolumeName(cwd); volume != "" {
			return volume + `\`
		}
	}

	return `C:\`
}
