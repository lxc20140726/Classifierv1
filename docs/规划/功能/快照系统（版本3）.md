# 可回退操作（Snapshot）v3.0

> 版本：v3.0 | 日期：2026-03-20
> **重大变更**：快照粒度从「操作级」升级为「节点级」，新增各操作类型的专属回退策略

## 设计原则

- **节点级快照**：每个节点执行前后各创建一次快照（pre/post），回退精确到节点
- **部分回退**：rename 节点成功、后续节点失败时，只回退后续节点；rename 结果保留
- **断点续传**：失败后记录最后成功节点 ID，下次从该节点之后重新开始
- **各操作独立补偿策略**：不同节点类型有明确的回退操作，不再是「不可回退」的例外

## 快照数据模型

```go
type SnapshotKind string
const (
    SnapshotPre       SnapshotKind = "pre"           // 节点执行前状态
    SnapshotPost      SnapshotKind = "post"          // 节点执行后状态
    SnapshotRollbackBase SnapshotKind = "rollback_base" // 回退基准（首个成功节点前）
)

type NodeSnapshot struct {
    ID             string       `json:"id"`
    NodeRunID      string       `json:"node_run_id"`
    WorkflowRunID  string       `json:"workflow_run_id"`
    Kind           SnapshotKind `json:"kind"`
    FSManifest     FSManifest   `json:"fs_manifest"`   // 文件系统状态
    OutputJSON     string       `json:"output_json,omitempty"` // 节点成功输出
    Compensation   string       `json:"compensation,omitempty"` // 回退操作描述 JSON
    CreatedAt      time.Time    `json:"created_at"`
}

// 文件系统清单
type FSManifest struct {
    Files   []FileEntry   `json:"files"`
    Folders []FolderEntry `json:"folders"`
}

type FileEntry struct {
    Path  string    `json:"path"`
    Size  int64     `json:"size"`
    MTime time.Time `json:"mtime"`
}

type FolderEntry struct {
    Path string `json:"path"`
}
```

## 各节点回退策略

| 节点类型 | 回退操作 | 回退后状态 | 说明 |
|---------|---------|-----------|------|
| `classifier` | 清除分类结果，恢复 unknown | 数据库字段回滚，无文件系统变更 | 无破坏性，代价最低 |
| `rename` | 用 pre-snapshot 的路径列表将文件/文件夹重命名回去 | 文件名/文件夹名恢复 | 从快照中读 original_path，执行反向 rename |
| `compress` | 删除产物压缩包，保留原始文件 | 压缩包消失，原文件完好 | 仅删除 post-snapshot 中记录的输出文件 |
| `thumbnail` | 删除所有生成的缩略图文件 | 缩略图目录删除 | 从 post-snapshot 的 output_json 读取路径列表 |
| `move` | 将文件夹移回 pre-snapshot 中的原始路径 | 文件夹回到原位 | 更新 folders.path，同时发送 SSE 进度 |

## 工作流执行状态机

```
WorkflowRun 状态:
  pending → running
    → succeeded (所有节点成功)
    → failed (某节点失败，已触发回退)
    → partial (部分节点成功，回退完成，等待用户决策)

NodeRun 状态:
  pending → running
    → succeeded → (触发下一节点)
    → failed → rollback_pending → rolled_back
```

## 节点执行与快照流程

```
执行节点 N：
  1. 更新 NodeRun.status = "running"
  2. 记录 NodeSnapshot(kind=pre)：当前文件系统状态
  3. 执行节点逻辑
  4. 成功：
     a. 记录 NodeSnapshot(kind=post)：执行后状态
     b. 更新 NodeRun.status = "succeeded"
     c. 更新 WorkflowRun.last_node_id = NodeRun.ID
     d. 触发下一节点
  5. 失败：
     a. 更新 NodeRun.status = "failed"
     b. 更新 WorkflowRun.resume_node_id = NodeRun.ID
     c. 触发已成功节点的逆序回退
```

## 断点续传流程

```
用户点击「重试」：
  1. 读取 WorkflowRun.resume_node_id
  2. 从 resume_node_id 对应节点的 sequence 开始
  3. 跳过 sequence 更小的已成功节点
  4. 从 resume_node_id 节点重新执行
  5. 执行完成后清除 resume_node_id
```

## 回退流程

```
节点 N 失败，触发回退：
  1. 查询 WorkflowRun 下所有 status=succeeded 的 NodeRun
  2. 按 sequence 逆序排列
  3. 对每个已成功 NodeRun：
     a. 读取其 NodeSnapshot(kind=post) 或 NodeSnapshot(kind=pre)
     b. 执行对应的补偿操作（见上方策略表）
     c. 更新 NodeRun.rollback_status = "succeeded"
  4. 更新 WorkflowRun.status = "partial"
  5. 写入 AuditLog（action=rollback, result=success/failure）
```

## API

```
# 节点快照查询
GET /api/jobs/:job_id/workflow-runs           # 列出所有 workflow_runs
GET /api/jobs/:job_id/workflow-runs/:run_id   # 单个 workflow_run 详情（含 node_runs）
GET /api/node-runs/:id/snapshots              # 节点的 pre/post 快照

# 回退与恢复
POST /api/workflow-runs/:id/rollback          # 回退整个 workflow_run（逆序补偿）
POST /api/workflow-runs/:id/resume            # 从断点继续执行
POST /api/node-runs/:id/rollback              # 回退单个节点
```

## 前端设计

- **JobsPage**：每个 Job 展开后显示各文件夹的 WorkflowRun 状态
- **WorkflowRun 详情**：节点执行时间线，每个节点显示状态和耗时
- **节点快照查看**：点击节点可查看 pre/post 文件系统差异
- **局部回退**：对单个 WorkflowRun 提供「回退」按钮，回退至初始状态
- **断点续传**：对 failed/partial 状态的 WorkflowRun 显示「从断点继续」按钮
- **状态说明**：`partial` 状态显示哪些节点成功、哪些已回退，让用户清楚当前状态

## 快照清理策略

- 保留最近 `storage.snapshot_retention_days`（默认 30 天）的快照
- `succeeded` 状态的 WorkflowRun 快照可在用户确认后提前清理
- `partial` 状态的快照保留至用户手动清理或超过保留期
