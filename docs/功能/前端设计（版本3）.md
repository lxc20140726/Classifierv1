# 前端设计 v3.1

> 版本：v3.1 | 日期：2026-03-23
> 本文描述仓库当前**已实现**的前端架构。带 `[规划]` 标记的部分尚未实现。

## 页面路由

| 路由 | 组件 | 功能 |
|------|------|------|
| `/` | `FolderListPage` | 主仪表盘：目录列表/网格、扫描、任务、日志、快照 |
| `/jobs` | `JobsPage` | 任务历史列表，可展开查看详情 |
| `/settings` | `SettingsPage` | 多扫描目录配置（图形目录选择器）|
| `*` | `NotFoundPage` | 404 页 |

## 组件树

```
App
  ├── Layout
  │     ├── Sidebar（导航：目录 / 任务 / 设置）
  │     ├── ToastList（通知 toast，右上角浮动）
  │     └── <Outlet>（页面内容区）
  │
  ├── FolderListPage
  │     ├── ScanProgressBanner（扫描中进度条）
  │     ├── RecentJobsPanel（最近任务列表）
  │     ├── RecentLogsPanel（最近审计日志）
  │     ├── FolderCard（网格模式卡片）
  │     ├── FolderRow（列表模式行）
  │     ├── MoveModal（批量移动弹窗）
  │     └── SnapshotDrawer（快照时间线抽屉）
  │
  ├── JobsPage
  │     └── JobItem（可展开的任务详情卡）
  │
  └── SettingsPage
        └── DirPicker（图形目录选择器 Modal）
```

## 状态管理（Zustand stores）

### useFolderStore

```typescript
interface FolderStore {
  folders: Folder[]
  total: number
  page: number
  limit: number
  isLoading: boolean
  error: string | null
  filters: FolderFilters
  scanProgress: ScanProgressState | null   // null = 未在扫描
  isScanning: boolean
  viewMode: 'grid' | 'list'

  fetchFolders(): Promise<void>
  setFilters(filters: FolderFilters): void
  setPage(page: number): void
  setViewMode(mode: 'grid' | 'list'): void
  triggerScan(): Promise<void>
  handleScanStarted(payload: ScanStartResponse): void
  handleScanProgress(progress: ScanProgressEvent): void
  handleScanError(progress: ScanProgressEvent): void
  handleScanDone(): void
  updateFolderCategory(id, category): Promise<void>
  updateFolderStatus(id, status): Promise<void>
  suppressFolder(id): Promise<void>
  unsuppressFolder(id): Promise<void>
}
```

### useJobStore

```typescript
interface JobStore {
  jobs: Job[]
  total: number
  isLoading: boolean

  fetchJobs(params?): Promise<void>
  addJob(job: Job): void
  updateJob(jobId, updates): void
  handleJobProgress(progress: JobProgress): void
  handleJobDone(payload: JobDoneEvent): void
  handleJobError(jobId, error): void
  startPolling(jobId): void
  stopPolling(jobId): void
  stopAllPolling(): void
}
```

### useSnapshotStore

```typescript
interface SnapshotStore {
  snapshots: Snapshot[]
  isLoading: boolean
  error: string | null

  fetchSnapshots(folderId): Promise<void>
  handleRevertDone(snapshotId): void
}
```

### useNotificationStore

```typescript
interface NotificationStore {
  notifications: AppNotification[]   // 最多 6 条
  pushNotification(notification): void
  dismissNotification(id): void
  clearNotifications(): void
}
```

### useActivityStore

```typescript
interface ActivityStore {
  logs: AuditLog[]
  total: number
  isLoading: boolean
  fetchLogs(params?): Promise<void>
}
```

## SSE 集成（useSSE hook）

```typescript
// 挂载在 Layout，整个应用生命周期内保持一个 SSE 连接
// 断线后 3 秒自动重连
// 监听以下事件并更新对应 store：

scan.started   → useFolderStore.handleScanStarted()
scan.progress  → useFolderStore.handleScanProgress()
scan.error     → useFolderStore.handleScanError()
scan.done      → useFolderStore.handleScanDone() + fetchFolders() + fetchLogs()
job.progress   → useJobStore.handleJobProgress()
job.done       → useJobStore.handleJobDone()
               → useFolderStore.handleScanDone()
               → useNotificationStore.pushNotification()
               → fetchFolders() + fetchLogs()
job.error      → useJobStore.handleJobError()
               → useNotificationStore.pushNotification()
               → fetchLogs()
```

## DirPicker 目录选择器

```
打开方式：SettingsPage 点击「添加目录」按钮
功能：
  - 展示当前路径下的所有子目录（过滤隐藏目录）
  - 上级目录导航按钮
  - 手动输入路径 + Enter / 「前往」跳转
  - 点击目录进入，点击 ChevronRight 图标快速进入子目录
  - 确认后回调 onConfirm(path: string)
数据源：GET /api/fs/dirs?path=
```

## SnapshotDrawer 快照时间线

```
打开方式：FolderListPage 目录卡片上的「快照时间线」按钮
功能：
  - 时间线展示该文件夹的所有 Snapshot（classify / move / rename）
  - 每条显示操作类型、状态、时间、前/后状态条数
  - detail 元数据展示（源目录、分类结果、输出目录、原始路径等）
  - 路径变化展示（before → after）
  - move 类型可点击「回退到此节点」
  - 回退失败后展示 RevertFailurePanel：
      - preflight 失败原因（中文描述）
      - 当前文件实际位置（current_state）
      - 明确提示「回退失败不会导致文件丢失」
```

## ToastList 通知系统

```
位置：Layout 右上角固定浮动
触发：job.done（成功/部分完成）、job.error
内容：标题 + 消息文字 + 关闭按钮
行为：6 秒后自动消失，最多同时显示 6 条
```

## API 层规范

- 所有 API 调用在 `frontend/src/api/` 下
- 公共 fetch 封装在 `api/client.ts`（包含 `ApiRequestError` 携带结构化 body）
- 响应解析统一做字段兼容（大写/小写 JSON key 均处理）
- 目录浏览：`api/fs.ts` → `listDirs(path)`
- 审计日志：`api/auditLogs.ts` → `listAuditLogs(params)`

## 规划中（未实现）

- `[规划]` `/workflows` WorkflowDefsPage 节点编辑器
- `[规划]` `/hidden` 已隐藏文件夹列表页（应用级隐藏记录）
- `[规划]` JobsPage 三层展开（Job → WorkflowRun → NodeRun）
- `[规划]` 工作流节点输出目录配置 UI
- `[规划]` 独立审计日志页（高级过滤 / 导出）
