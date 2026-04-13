package service

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"github.com/liqiye/classifier/internal/fs"
	"github.com/liqiye/classifier/internal/repository"
)

const folderPickerExecutorType = "folder-picker"

type folderPickerNodeExecutor struct {
	fs      fs.FSAdapter
	folders repository.FolderRepository
}

func newFolderPickerNodeExecutor(fsAdapter fs.FSAdapter, folderRepo repository.FolderRepository) *folderPickerNodeExecutor {
	return &folderPickerNodeExecutor{fs: fsAdapter, folders: folderRepo}
}

func (e *folderPickerNodeExecutor) Type() string {
	return folderPickerExecutorType
}

func (e *folderPickerNodeExecutor) Schema() NodeSchema {
	return NodeSchema{
		Type:        e.Type(),
		Label:       "文件夹选择器",
		Description: "静态源节点：支持自选路径目录或从媒体文件夹记录选择，运行时直接输出为目录树",
		Inputs:      []PortDef{},
		Outputs: []PortDef{
			{Name: "folders", Type: PortTypeFolderTreeList, Description: "选定的目录树列表（可直接接分类器）"},
			{Name: "path", Type: PortTypePath, Description: "第一个配置路径（可接目录树扫描器的 source_dir）"},
		},
		ConfigSchema: map[string]any{
			"source_mode": map[string]any{
				"type":        "string",
				"enum":        []string{"path", "folders"},
				"default":     "path",
				"description": "path=手动单路径；folders=从数据库已保存文件夹中选择单条记录",
			},
			"path": map[string]any{
				"type":        "string",
				"description": "路径模式下使用的目录路径（单值）",
			},
			"saved_folder_id": map[string]any{
				"type":        "string",
				"description": "记录模式下使用的数据库 folder.id（单值）",
			},
			"paths": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "兼容旧配置：路径模式下的目录路径列表，仅取第一条",
			},
			"saved_folder_ids": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "兼容旧配置：记录模式下的数据库 folder.id 列表，仅取第一条",
			},
			"folder_ids": map[string]any{
				"type":        "array",
				"items":       map[string]any{"type": "string"},
				"description": "兼容旧配置：记录模式使用的数据库 folder.id 列表，仅取第一条",
			},
		},
	}
}

func (e *folderPickerNodeExecutor) Execute(ctx context.Context, input NodeExecutionInput) (NodeExecutionOutput, error) {
	mode := folderPickerSourceMode(input.Node.Config)
	paths := []string{}
	switch mode {
	case "folders":
		folderID := folderPickerParseSavedFolderID(input.Node.Config)
		paths = e.pathFromFolderRecord(ctx, folderID)
	default:
		path := folderPickerParsePath(input.Node.Config)
		if path != "" {
			paths = append(paths, path)
		}
	}

	trees := make([]FolderTree, 0, len(paths))
	for _, p := range paths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}

		info, err := e.fs.Stat(ctx, p)
		if err != nil {
			continue
		}
		if !info.IsDir {
			continue
		}

		trees = append(trees, FolderTree{
			Path:    p,
			Name:    filepath.Base(p),
			Files:   []FileEntry{},
			Subdirs: []FolderTree{},
		})
	}

	primaryPath := ""
	if len(paths) > 0 {
		primaryPath = strings.TrimSpace(paths[0])
	}

	return NodeExecutionOutput{
		Outputs: map[string]TypedValue{
			"folders": {Type: PortTypeFolderTreeList, Value: trees},
			"path":    {Type: PortTypePath, Value: primaryPath},
		},
		Status: ExecutionSuccess,
	}, nil
}

func (e *folderPickerNodeExecutor) Resume(_ context.Context, _ NodeExecutionInput, _ map[string]any) (NodeExecutionOutput, error) {
	return NodeExecutionOutput{}, fmt.Errorf("%s: Resume not supported", e.Type())
}

func (e *folderPickerNodeExecutor) Rollback(_ context.Context, _ NodeRollbackInput) error {
	return nil
}

func (e *folderPickerNodeExecutor) pathFromFolderRecord(ctx context.Context, folderID string) []string {
	if e.folders == nil {
		return []string{}
	}

	trimmedID := strings.TrimSpace(folderID)
	if trimmedID == "" {
		return []string{}
	}
	folder, err := e.folders.GetByID(ctx, trimmedID)
	if err != nil || folder == nil {
		return []string{}
	}
	trimmedPath := strings.TrimSpace(folder.Path)
	if trimmedPath == "" {
		return []string{}
	}

	return []string{trimmedPath}
}

func folderPickerSourceMode(config map[string]any) string {
	mode := strings.ToLower(strings.TrimSpace(anyString(config["source_mode"])))
	if mode == "folders" {
		return mode
	}

	return "path"
}

func folderPickerParsePath(config map[string]any) string {
	if path := strings.TrimSpace(anyString(config["path"])); path != "" {
		return path
	}
	return folderPickerParseFirstString(config, "paths")
}

func folderPickerParseSavedFolderID(config map[string]any) string {
	if id := strings.TrimSpace(anyString(config["saved_folder_id"])); id != "" {
		return id
	}
	// 兼容旧字段 folder_ids，优先使用新字段 saved_folder_ids。
	if id := folderPickerParseFirstString(config, "saved_folder_ids"); id != "" {
		return id
	}
	return folderPickerParseFirstString(config, "folder_ids")
}

func folderPickerParseFirstString(config map[string]any, key string) string {
	raw, ok := config[key]
	if !ok {
		return ""
	}

	switch v := raw.(type) {
	case []string:
		if len(v) == 0 {
			return ""
		}
		return strings.TrimSpace(v[0])
	case []any:
		for _, item := range v {
			if s, ok := item.(string); ok {
				trimmed := strings.TrimSpace(s)
				if trimmed != "" {
					return trimmed
				}
			}
		}
		return ""
	case string:
		return strings.TrimSpace(v)
	}

	return ""
}
