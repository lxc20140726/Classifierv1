# 可视化重命名编辑器 v3.0

> 版本：v3.0 | 日期：2026-03-20
> **重大变更**：同时支持文件重命名与文件夹重命名，重命名成为工作流节点的一部分

## 设计目标

- 同时支持 `files`、`folder`、`both` 三种应用范围
- 执行前必须预览
- 重命名结果必须可回退
- 支持冲突检测和策略选择

## 节点输入结构

```json
{
  "type": "rename",
  "data": {
    "apply_to": "both",
    "file_template": "{foldername}_{index}{ext}",
    "folder_template": "{category}_{date}_{foldername}",
    "sort_by": "name",
    "sort_asc": true,
    "index_start": 1,
    "index_padding": 3,
    "conflict_strategy": "skip"
  }
}
```

## Token 体系

### 文件级 Token

| Token | 说明 |
|------|------|
| `{filename}` | 原文件名（不含扩展名） |
| `{ext}` | 扩展名 |
| `{index}` | 顺序号 |
| `{date}` | 修改日期 |
| `{year}` | 年 |
| `{month}` | 月 |
| `{day}` | 日 |

### 文件夹级 Token

| Token | 说明 |
|------|------|
| `{foldername}` | 当前文件夹名 |
| `{parent_folder}` | 父目录名 |
| `{category}` | 上游分类节点输出 |
| `{job_id}` | 当前 Job ID |
| `{workflow_id}` | 当前 WorkflowRun ID |
| `{seq}` | 当前批次中的文件夹序号 |

## 冲突策略

- `skip`：跳过冲突项并记录 warning
- `overwrite`：覆盖目标（默认不推荐）
- `append_suffix`：自动追加 `_1`, `_2`
- `fail_fast`：发现冲突立即失败并停止该 WorkflowRun

## 预览响应

```json
{
  "file_previews": [
    {"from": "IMG_001.jpg", "to": "album_001.jpg"}
  ],
  "folder_preview": {
    "from": "raw_album",
    "to": "photo_20260320_raw_album"
  },
  "conflicts": [],
  "warnings": []
}
```

## 回退要求

- rename 节点执行前必须记录 pre snapshot
- snapshot 中保存 `original_path -> current_path` 映射
- 后续节点失败时，rename 节点不一定要回退；是否回退由 WorkflowRun 的补偿策略决定
- 用户手动触发 workflow rollback 时，rename 节点按映射反向执行
