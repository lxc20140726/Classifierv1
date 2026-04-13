# 节点式工作流系统设计 v3.0

> 版本：v3.0 | 日期：2026-03-20
> **重大变更**：工作流执行与 Job-WorkflowRun-NodeRun 三层模型打通，分类器成为节点，支持断点续传与节点级回退

## 核心模型

```
Job
 └── WorkflowRun (per folder)
      └── NodeRun (per node execution)
           └── NodeSnapshot (pre/post)
```

## 设计原则

- Jobs 是用户在 `JobsPage` 看到的顶层对象
- 一个 Job 可包含多个文件夹，因此一个 Job 会生成多个 WorkflowRun
- 每个 WorkflowRun 对应一个文件夹的一次完整工作流执行
- 每个 NodeRun 是单个节点的一次执行记录
- 节点失败后记录断点，支持从下一个待执行节点继续

## 节点类型

- `trigger`
- `name-keyword-classifier`
- `file-tree-classifier`
- `ext-ratio-classifier`
- `manual-classifier`
- `condition`
- `rename`
- `compress`
- `thumbnail`
- `move`
- `wait`

## 执行流程

```
1. 用户提交 Job
2. 为每个 folder 创建 WorkflowRun
3. 读取 workflow definition，拓扑排序
4. 按顺序或按可并行 wave 执行 NodeRun
5. 每个 NodeRun:
   - create pre snapshot
   - execute
   - create post snapshot
   - persist outputs
6. 任一节点失败:
   - mark WorkflowRun.resume_node_id
   - 按补偿策略决定是否回退已成功节点
7. 全部完成后汇总 Job.status
```

## 断点续传

- `WorkflowRun.last_node_id`：最后成功节点
- `WorkflowRun.resume_node_id`：下次恢复时从哪个节点重新开始
- 继续执行时跳过所有已成功且无需重试的节点

## 补偿策略

### 默认策略

- `rename`：可补偿
- `compress`：可补偿（删除压缩产物）
- `thumbnail`：可补偿（删除缩略图）
- `move`：可补偿（移动回原路径）
- `classifier`：逻辑回滚（清空分类结果）

### 两种恢复模式

1. `resume_only`
   - 不回退已成功节点
   - 下次从 `resume_node_id` 继续
2. `rollback_then_resume`
   - 先回退指定节点范围
   - 再从指定节点重跑

## Workflow 定义结构

```go
type WorkflowDefinition struct {
    ID          string
    Name        string
    Description string
    GraphJSON   string
    Version     int
    IsActive    bool
}

type WorkflowGraph struct {
    Nodes []WorkflowNode `json:"nodes"`
    Edges []WorkflowEdge `json:"edges"`
}

type WorkflowNode struct {
    ID       string `json:"id"`
    Type     string `json:"type"`
    Data     any    `json:"data"`
    Enabled  bool   `json:"enabled"`
}

type WorkflowEdge struct {
    Source    string `json:"source"`
    Target    string `json:"target"`
    Condition string `json:"condition,omitempty"`
}
```

## 调度器接口

```go
type WorkflowRunner interface {
    StartJob(ctx context.Context, req StartJobRequest) (string, error)
    ResumeWorkflowRun(ctx context.Context, workflowRunID string) error
    RollbackWorkflowRun(ctx context.Context, workflowRunID string) error
}

type NodeExecutor interface {
    Type() string
    Execute(ctx context.Context, input NodeExecutionInput) (NodeExecutionOutput, error)
    Rollback(ctx context.Context, input NodeRollbackInput) error
}
```

## API

```
GET    /api/workflow-defs
POST   /api/workflow-defs
GET    /api/workflow-defs/:id
PUT    /api/workflow-defs/:id
DELETE /api/workflow-defs/:id

GET    /api/jobs
POST   /api/jobs
GET    /api/jobs/:id
GET    /api/jobs/:id/progress
POST   /api/jobs/:id/cancel

GET    /api/jobs/:job_id/workflow-runs
GET    /api/jobs/:job_id/workflow-runs/:run_id
POST   /api/workflow-runs/:id/resume
POST   /api/workflow-runs/:id/rollback

GET    /api/node-runs/:id/snapshots
```

## 前端编辑器要求

- 左侧节点面板必须包含分类器节点与动作节点
- 右侧配置面板按节点类型动态渲染表单
- 运行中的 WorkflowRun 要在编辑器中可回放节点状态
- 节点状态来源于 SSE，页面恢复时用 HTTP 拉取历史状态补齐
