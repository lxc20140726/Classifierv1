# 前端设计文档

> 版本：v2.0 | 日期：2026-03-19

## 技术栈

- **框架**: React 18 + TypeScript + Vite
- **样式**: Tailwind CSS + shadcn/ui
- **状态管理**: Zustand
- **路由**: React Router v6
- **节点图**: @xyflow/react (React Flow)
- **拖拽**: @dnd-kit/core + @dnd-kit/sortable
- **实时通信**: SSE (EventSource)

## 页面路由

| 路由 | 页面 | 说明 |
|------|------|------|
| `/` | FolderListPage | 主页，文件夹列表+批量操作 |
| `/jobs` | JobsPage | 任务队列监控 |
| `/workflows` | WorkflowsPage | 节点式工作流配置 [NEW] |
| `/logs` | LogsPage | 审计日志 [NEW] |
| `/rules` | RulesPage | 重命名规则管理 |
| `/settings` | SettingsPage | 全局配置 |

## 核心组件树

### Layout
```
App
+-- Layout
    +-- Sidebar
    |   +-- NavItem (/, /jobs, /workflows, /logs, /rules, /settings)
    +-- TopBar
    |   +-- ScanButton
    |   +-- GlobalProgressIndicator
    +-- <Outlet> (页面内容区)
```

### FolderListPage
```
FolderListPage
+-- FolderToolbar
|   +-- ScanButton
|   +-- FilterBar (status / category / search)
|   +-- BulkActionMenu
|       +-- BulkRenameButton
|       +-- BulkCompressButton
|       +-- BulkThumbnailButton
|       +-- BulkMoveButton
|       +-- RunWorkflowButton
+-- FolderTable (虚拟滚动)
    +-- FolderRow
        +-- CategoryBadge
        +-- StatusBadge
        +-- ActionMenu
            +-- CategoryEditDialog
            +-- SnapshotDrawer
```

### WorkflowsPage
```
WorkflowsPage
+-- WorkflowList (左侧)
|   +-- WorkflowCard (category badge + node count + edit)
+-- WorkflowEditor (右侧，React Flow)
    +-- 节点面板 (左)
    |   +-- NodePalette (可拖拽到画布的节点类型列表)
    +-- ReactFlow 画布 (中)
    |   +-- TriggerNode
    |   +-- RenameNode
    |   +-- CompressNode
    |   +-- ThumbnailNode
    |   +-- MoveNode
    |   +-- ConditionNode
    |   +-- WaitNode
    +-- NodeConfigPanel (右，选中节点时显示)
        +-- RenameNodeConfig
        +-- CompressNodeConfig
        +-- ThumbnailNodeConfig
        +-- MoveNodeConfig
```

### JobsPage
```
JobsPage
+-- JobFilters (status: running/done/failed)
+-- JobList
    +-- JobCard
        +-- JobProgressBar (SSE 实时更新)
        +-- JobDetailDrawer
            +-- StepList (每个步骤的进度)
            +-- CancelButton
```

### LogsPage
```
LogsPage
+-- LogFilters (action / level / date range / folder path)
+-- LogTable (虚拟滚动)
|   +-- LogRow (level badge + timestamp + action + detail)
+-- ExportButton (CSV 导出)
```

## Zustand Store 设计

```typescript
// stores/folderStore.ts
interface FolderStore {
    folders: Folder[]
    total: number
    page: number
    filters: FolderFilters
    selectedIds: Set<string>
    loading: boolean
    // actions
    fetchFolders: () => Promise<void>
    triggerScan: () => Promise<void>
    updateCategory: (id: string, category: Category) => Promise<void>
    updateStatus: (id: string, status: FolderStatus) => Promise<void>
    toggleSelect: (id: string) => void
    selectAll: () => void
    clearSelect: () => void
}

// stores/jobStore.ts
interface JobStore {
    jobs: Job[]
    // actions
    fetchJobs: () => Promise<void>
    cancelJob: (id: string) => Promise<void>
    // SSE 更新
    updateJobProgress: (jobId: string, progress: JobProgress) => void
}

// stores/workflowStore.ts
interface WorkflowStore {
    workflows: Workflow[]
    activeWorkflowId: string | null
    // React Flow state
    nodes: Node[]
    edges: Edge[]
    // actions
    fetchWorkflows: () => Promise<void>
    loadWorkflow: (id: string) => void
    saveWorkflow: () => Promise<void>
    setNodes: (nodes: Node[]) => void
    setEdges: (edges: Edge[]) => void
}

// stores/sseStore.ts
interface SSEStore {
    connected: boolean
    connect: () => void
    disconnect: () => void
    // 内部：分发 SSE 事件到各 store
}
```

## SSE 事件处理

```typescript
// hooks/useSSE.ts
export function useSSE() {
    useEffect(() => {
        const es = new EventSource("/api/events")

        es.addEventListener("scan.progress", (e) => {
            const data = JSON.parse(e.data)
            useFolderStore.getState().handleScanProgress(data)
        })

        es.addEventListener("job.progress", (e) => {
            const data = JSON.parse(e.data)
            useJobStore.getState().updateJobProgress(data.job_id, data)
        })

        es.addEventListener("workflow.node.start", (e) => {
            const data = JSON.parse(e.data)
            useWorkflowStore.getState().updateNodeStatus(data.node_id, "running")
        })

        es.addEventListener("workflow.node.done", (e) => {
            const data = JSON.parse(e.data)
            useWorkflowStore.getState().updateNodeStatus(data.node_id, "done")
        })

        return () => es.close()
    }, [])
}
```
