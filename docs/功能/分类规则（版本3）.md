# 文件分类设计 v3.0

> 版本：v3.0 | 日期：2026-03-20
> **重大变更**：分类从固定算法升级为可组合的分类器节点集合

## 设计目标

- 分类不再是单一硬编码函数，而是工作流中的节点能力
- 用户可基于文件树、扩展名、关键词、自定义规则组合分类流程
- 分类结果必须输出 `category + confidence + reason`
- 分类错误时可通过人工确认节点兜底

## 分类器节点类型

### `ext-ratio-classifier`

- 基于扩展名统计 `image_count / video_count / total_media`
- 适合快速粗分类
- 输出：`photo | video | mixed | unknown`

```json
{
  "type": "ext-ratio-classifier",
  "data": {
    "photo_ratio": 0.85,
    "video_ratio": 0.85,
    "mixed_min_ratio": 0.15
  }
}
```

### `name-keyword-classifier`

- 基于文件夹名关键词匹配
- 适合识别漫画、写真、特定系列
- 输出带 `matched_keyword`

```json
{
  "type": "name-keyword-classifier",
  "data": {
    "rules": [
      {"keywords": ["漫画", "manga", "comic"], "category": "manga"},
      {"keywords": ["写真", "photobook"], "category": "photo"}
    ]
  }
}
```

### `file-tree-classifier`

- 基于文件树结构做判断
- 例：
  - 含 `.cbz/.cbr/.zip` 且目录扁平 -> `manga`
  - 全图片且无子目录 -> `photo`
  - 视频文件为主且有字幕/封面 -> `video`

### `presence-classifier`

- 基于特定文件存在性判断
- 例：存在 `folder.jpg` / `poster.jpg` / `backdrop.jpg` 不应直接把视频目录误判为 mixed

### `manual-classifier`

- 无法自动确定时暂停流程
- 用户在前端选择最终分类
- 超时可失败或落到默认分支

## 推荐组合策略

```
trigger
  -> name-keyword-classifier
  -> file-tree-classifier
  -> ext-ratio-classifier
  -> condition(category/confidence)
  -> manual-classifier (fallback)
```

## 输出协议

```go
type ClassifierOutput struct {
    Category   string   `json:"category"`
    Confidence float64  `json:"confidence"`
    Reason     string   `json:"reason"`
    Signals    []string `json:"signals,omitempty"`
}
```

## 设计约束

- Scanner 阶段只做轻量元数据采集，不做最终业务分类
- 最终分类以工作流内分类器节点输出为准
- 分类节点必须可审计，`reason` 写入 `audit_logs.detail`
- 分类节点默认无文件系统副作用，因此无需文件级回退，只需回滚数据库分类结果
