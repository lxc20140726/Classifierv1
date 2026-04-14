# 数据模型设计 v3.1

> 版本：v3.1 | 日期：2026-03-23
> 本文以仓库**实际已实现**的 Schema 为准。带 `[规划]` 的表或字段尚未在代码中实现。

## 实际 SQLite Schema

数据库迁移文件位于 `backend/internal/db/migrations/`，按序执行：

### 001_initial.sql — 基础表

```sql
CREATE TABLE folders (
    id              TEXT PRIMARY KEY,
    path            TEXT NOT NULL UNIQUE,          -- 绝对路径（active 记录唯一；隐藏记录仍保留原始 path）
    name            TEXT NOT NULL,
    category        TEXT NOT NULL DEFAULT 'other', -- photo|video|mixed|manga|other
    category_source TEXT NOT NULL DEFAULT 'auto',  -- auto|manual
    status          TEXT NOT NULL DEFAULT 'pending',
    image_count     INTEGER NOT NULL DEFAULT 0,
    video_count     INTEGER NOT NULL DEFAULT 0,
    total_files     INTEGER NOT NULL DEFAULT 0,
    total_size      INTEGER NOT NULL DEFAULT 0,
    marked_for_move INTEGER NOT NULL DEFAULT 0,
    scanned_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE snapshots (
    id             TEXT PRIMARY KEY,
    job_id         TEXT NOT NULL,
    folder_id      TEXT NOT NULL,
    operation_type TEXT NOT NULL,   -- classify|move|rename
    before_state   TEXT NOT NULL,   -- JSON: [{original_path, current_path}]
    after_state    TEXT,            -- JSON: [{original_path, current_path}]
    status         TEXT NOT NULL DEFAULT 'pending', -- pending|committed|reverted
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE audit_logs (
    id          TEXT PRIMARY KEY,
    job_id      TEXT,
    folder_id   TEXT,
    folder_path TEXT NOT NULL,
    action      TEXT NOT NULL,   -- scan|move|suppress|unsuppress|revert
    level       TEXT NOT NULL DEFAULT 'info',
    detail      TEXT,            -- JSON 元数据
    result      TEXT NOT NULL,   -- success|failed
    error_msg   TEXT,
    duration_ms INTEGER NOT NULL DEFAULT 0,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
```

### 003_v3_jobs.sql — Jobs 与记录隐藏状态（复用 deleted_at 字段表示 suppression）

```sql
CREATE TABLE jobs (
    id          TEXT PRIMARY KEY,
    type        TEXT NOT NULL,           -- scan|move
    status      TEXT NOT NULL DEFAULT 'pending', -- pending|running|succeeded|failed|partial|cancelled
    folder_ids  TEXT NOT NULL DEFAULT '[]',  -- JSON 数组
    total       INTEGER NOT NULL DEFAULT 0,
    done        INTEGER NOT NULL DEFAULT 0,
    failed      INTEGER NOT NULL DEFAULT 0,
    error       TEXT,
    started_at  DATETIME,
    finished_at DATETIME,
    created_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- folders 表扩展
ALTER TABLE folders ADD COLUMN deleted_at DATETIME;
ALTER TABLE folders ADD COLUMN delete_staging_path TEXT;

-- 当前实现仍复用 deleted_at 表示“已在软件内隐藏/抑制”，并保留 active path 唯一约束
CREATE UNIQUE INDEX ux_folders_path_active ON folders(path) WHERE deleted_at IS NULL;
```

### 004_scan_history.sql — 扫描溯源

```sql
-- folders 表扩展
ALTER TABLE folders ADD COLUMN source_dir    TEXT NOT NULL DEFAULT '';
ALTER TABLE folders ADD COLUMN relative_path TEXT NOT NULL DEFAULT '';

-- snapshots 表扩展
ALTER TABLE snapshots ADD COLUMN detail TEXT;  -- JSON 操作元数据
```

## Go Struct（已实现）

### Folder

```go
type Folder struct {
    ID               string     `db:"id"`
    Path             string     `db:"path"`
    SourceDir        string     `db:"source_dir"`        // 来源扫描目录
    RelativePath     string     `db:"relative_path"`     // 相对于 SourceDir 的路径
    Name             string     `db:"name"`
    Category         string     `db:"category"`
    CategorySource   string     `db:"category_source"`
    Status           string     `db:"status"`
    ImageCount       int        `db:"image_count"`
    VideoCount       int        `db:"video_count"`
    TotalFiles       int        `db:"total_files"`
    TotalSize        int64      `db:"total_size"`
    MarkedForMove    bool       `db:"marked_for_move"`
    DeletedAt        *time.Time `db:"deleted_at"`       // 当前实现中表示“已隐藏/抑制”
    DeleteStagingPath string    `db:"delete_staging_path"` // 历史遗留字段，当前隐藏语义下不再用于文件系统移动
    ScannedAt        time.Time  `db:"scanned_at"`
    UpdatedAt        time.Time  `db:"updated_at"`
}
```

### Job

```go
type Job struct {
    ID         string     `db:"id"`
    Type       string     `db:"type"`       // "scan" | "move"
    Status     string     `db:"status"`
    FolderIDs  string     `db:"folder_ids"` // JSON array string
    Total      int        `db:"total"`
    Done       int        `db:"done"`
    Failed     int        `db:"failed"`
    Error      string     `db:"error"`
    StartedAt  *time.Time `db:"started_at"`
    FinishedAt *time.Time `db:"finished_at"`
    CreatedAt  time.Time  `db:"created_at"`
    UpdatedAt  time.Time  `db:"updated_at"`
}
```

### Snapshot

```go
type Snapshot struct {
    ID            string          `db:"id"`
    JobID         string          `db:"job_id"`
    FolderID      string          `db:"folder_id"`
    OperationType string          `db:"operation_type"`  // classify|move|rename
    Before        json.RawMessage `db:"before_state"`    // []snapshotPathState
    After         json.RawMessage `db:"after_state"`
    Detail        json.RawMessage `db:"detail"`          // 操作元数据（category、source_dir、target_dir 等）
    Status        string          `db:"status"`
    CreatedAt     time.Time       `db:"created_at"`
}

type snapshotPathState struct {
    OriginalPath string `json:"original_path"`
    CurrentPath  string `json:"current_path"`
}
```

**Snapshot.Detail 示例（classify 类型）：**
```json
{
  "source_dir": "/data/source",
  "relative_path": "MyFolder",
  "category": "video",
  "category_source": "auto",
  "total_files": 12,
  "image_count": 0,
  "video_count": 8
}
```

**Snapshot.Detail 示例（move 类型）：**
```json
{
  "source_path": "/data/source/MyFolder",
  "target_dir": "/data/target/video",
  "folder_name": "MyFolder",
  "category": "video"
}
```

### AuditLog

```go
type AuditLog struct {
    ID         string          `db:"id"`
    JobID      string          `db:"job_id"`
    FolderID   string          `db:"folder_id"`
    FolderPath string          `db:"folder_path"`
    Action     string          `db:"action"`
    Level      string          `db:"level"`
    Detail     json.RawMessage `db:"detail"`
    Result     string          `db:"result"`
    ErrorMsg   string          `db:"error_msg"`
    DurationMs int64           `db:"duration_ms"`
    CreatedAt  time.Time       `db:"created_at"`
}

type AuditListFilter struct {
    JobID    string
    Action   string
    Result   string
    FolderID string
    Page     int
    Limit    int
    From     time.Time
}
```

## Repository 接口（已实现）

```go
type FolderRepository interface {
    Upsert(ctx, *Folder) error
    GetByID(ctx, id) (*Folder, error)
    GetByPath(ctx, path) (*Folder, error)
    List(ctx, FolderListFilter) ([]*Folder, int, error)
    UpdateCategory(ctx, id, category, source) error
    UpdateStatus(ctx, id, status) error
    UpdatePath(ctx, id, newPath) error
    Suppress(ctx, id, currentPath, originalPath) error
    Unsuppress(ctx, id) error
    Delete(ctx, id) error
}

type JobRepository interface {
    Create(ctx, *Job) error
    GetByID(ctx, id) (*Job, error)
    List(ctx, JobListFilter) ([]*Job, int, error)
    UpdateTotal(ctx, id, total) error
    UpdateStatus(ctx, id, status, errMsg) error
    IncrementProgress(ctx, id, successDelta, failedDelta) error
}

type SnapshotRepository interface {
    Create(ctx, *Snapshot) error
    GetByID(ctx, id) (*Snapshot, error)
    ListByFolderID(ctx, folderID) ([]*Snapshot, error)
    ListByJobID(ctx, jobID) ([]*Snapshot, error)
    CommitAfter(ctx, id, after json.RawMessage) error
    UpdateDetail(ctx, id, detail json.RawMessage) error
    UpdateStatus(ctx, id, status) error
}

type AuditRepository interface {
    Write(ctx, *AuditLog) error
    List(ctx, AuditListFilter) ([]*AuditLog, int, error)
    GetByID(ctx, id) (*AuditLog, error)
}

type ConfigRepository interface {
    Set(ctx, key, value) error
    Get(ctx, key) (string, error)
    GetAll(ctx) (map[string]string, error)
}
```

## 规划中（未实现）

```sql
-- [规划] WorkflowRun
CREATE TABLE workflow_runs (
    id              TEXT PRIMARY KEY,
    job_id          TEXT NOT NULL,
    folder_id       TEXT NOT NULL,
    workflow_def_id TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    resume_node_id  TEXT,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- [规划] NodeRun
CREATE TABLE node_runs (
    id              TEXT PRIMARY KEY,
    workflow_run_id TEXT NOT NULL,
    node_id         TEXT NOT NULL,
    node_type       TEXT NOT NULL,
    sequence        INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    error           TEXT,
    started_at      DATETIME,
    finished_at     DATETIME,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- [规划] NodeSnapshot
CREATE TABLE node_snapshots (
    id              TEXT PRIMARY KEY,
    node_run_id     TEXT NOT NULL,
    workflow_run_id TEXT NOT NULL,
    kind            TEXT NOT NULL,  -- pre|post|rollback_base
    fs_manifest     TEXT,           -- JSON 文件系统清单
    output_json     TEXT,
    compensation    TEXT,
    created_at      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- [规划] WorkflowDefinition
CREATE TABLE workflow_definitions (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    graph_json TEXT NOT NULL,  -- DAG 节点配置
    created_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```
