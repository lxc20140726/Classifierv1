# 审计日志系统

> 版本：v2.0 | 日期：2026-03-19

## 设计原则

- **全量记录**：所有文件操作均写入日志
- **不可篡改**：日志只追加，不修改，不删除
- **结构化存储**：JSON 格式，方便查询和导出

## 数据模型

```go
type AuditAction string
const (
    ActionScan      AuditAction = "scan"
    ActionClassify  AuditAction = "classify"
    ActionRename    AuditAction = "rename"
    ActionCompress  AuditAction = "compress"
    ActionThumbnail AuditAction = "thumbnail"
    ActionMove      AuditAction = "move"
    ActionRevert    AuditAction = "revert"
)

type AuditLog struct {
    ID         string          `json:"id"`
    JobID      string          `json:"job_id"`
    FolderID   string          `json:"folder_id"`
    FolderPath string          `json:"folder_path"`
    Action     AuditAction     `json:"action"`
    Level      string          `json:"level"` // info|warn|error
    Detail     json.RawMessage `json:"detail"`
    Result     string          `json:"result"` // success|failure
    Error      string          `json:"error,omitempty"`
    DurationMs int64           `json:"duration_ms"`
    CreatedAt  time.Time       `json:"created_at"`
}
```

## 日志写入时机

| 操作 | 写入时机 | Detail 内容 |
|------|----------|-------------|
| 扫描 | 完成后 | 发现文件夹数、新增数 |
| 分类 | 每个文件夹后 | 分类结果、依据 |
| 重命名 | 每个文件操作后 | from/to 路径、规则名 |
| 压缩 | 完成后 | 输出文件、原始大小、压缩大小 |
| 缩略图 | 每个视频完成后 | 输出路径、耗时 |
| 移动 | 每个文件夹后 | from/to 路径 |
| 回退 | 完成后 | 关联 snapshot_id |

## API

```
GET  /api/logs?action=&result=&folder_id=&from=&to=&page=&limit=
GET  /api/logs/:id
GET  /api/logs/export?format=json|csv&from=&to=
```

## 前端设计

- **路由 `/logs`** 专属日志页面
- 表格：时间、操作类型、文件夹路径、结果、耗时
- 筛选：操作类型、结果（成功/失败）、时间范围
- 点击展开 Detail 面板
- 导出：下载 JSON 或 CSV
- 错误行红色高亮（level=error）
- 关联跳转：点击文件夹路径跳转 FolderListPage
