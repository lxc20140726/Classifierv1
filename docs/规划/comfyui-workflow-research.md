# ComfyUI 节点工作流调研与融合分析

> 版本：v1.0 | 日期：2026-03-23
> 目标：系统整理 ComfyUI 节点工作流的核心实现模式，并针对 Classifier Phase 2（工作流引擎核心）给出具体的融合建议。

---

## 一、ComfyUI 节点工作流核心机制

### 1.1 Workflow JSON 格式

ComfyUI 将工作流表示为一个扁平 JSON 字典，键是节点 ID（字符串），值是节点定义对象。

```json
{
  "1": {
    "class_type": "CheckpointLoaderSimple",
    "inputs": {
      "ckpt_name": "v1-5-pruned.safetensors"
    }
  },
  "3": {
    "class_type": "KSampler",
    "inputs": {
      "model":          ["1", 0],
      "positive":       ["2", 0],
      "negative":       ["2", 1],
      "latent_image":   ["4", 0],
      "seed":           42,
      "steps":          20,
      "cfg":            7.0
    }
  }
}
```

**关键设计**：
- **输入分两种**：常量值（直接嵌入）和 **链接数组** `[source_node_id, output_socket_index]`
- 拓扑关系直接编码在 `inputs` 字段里，不需要单独的 `edges` 列表
- `class_type` 字符串对应注册表里的节点类，执行引擎通过它找到对应的实现

### 1.2 执行引擎架构

ComfyUI 执行引擎由三个主要组件构成：

```
Workflow JSON (用户提交)
         │
         ▼
┌─────────────────────┐
│   DynamicPrompt      │  维护两张图：原始图 + 运行时动态扩展图（ephemeral）
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  ExecutionList       │  TopologicalSort 基类 + 拓扑溶解（stage/unstage/complete）
│  (TopologicalSort)   │  维护 pendingNodes / blockCount / blocking 三个结构
└─────────┬───────────┘
          │
          ▼
┌─────────────────────┐
│  PromptExecutor      │  主执行循环，驱动 ExecutionList，处理缓存、错误、WebSocket 推送
└─────────────────────┘
```

#### 1.2.1 DynamicPrompt：双图表示

`DynamicPrompt` 同时持有两张图：

| 字段 | 内容 |
|------|------|
| `original_prompt` | 用户提交的原始 JSON |
| `ephemeral_prompt` | 运行时动态创建的临时节点（子图展开） |
| `ephemeral_parents` | 临时节点 → 父节点 ID 映射（用于错误定位） |

当一个节点返回 `{"expand": {...}}` 时，新节点被添加到 `ephemeral_prompt`，该机制支撑了**循环展开**和**条件分支**等动态控制流。

#### 1.2.2 ExecutionList：拓扑溶解

相比传统"先排序后执行"的静态拓扑排序，ComfyUI 使用**拓扑溶解（Topological Dissolve）**：

```
pendingNodes:  等待执行的节点集合
blockCount:    每个节点被依赖阻塞的数量
blocking:      node_id → {被阻塞节点 → {出口 socket → True}}
staged_node_id: 当前正在执行的节点（可以被 unstage）
externalBlocks: 外部异步操作阻塞计数器
```

节点状态流转：
```
PENDING ──(stage)──► EXECUTING ──(SUCCESS)──► COMPLETE（从队列移除）
                              └──(FAILURE)──► 中止
                              └──(PENDING) ──► 重新 unstage（等待新依赖）
```

`PENDING` 结果的三种触发场景：
1. **Async 任务**：节点函数是异步的，返回了未完成的 `asyncio.Task`
2. **懒输入**：`check_lazy_status()` 声明需要但尚未加载的输入
3. **子图展开**：节点返回 `{"expand": {...}}` 创建了新的临时节点

#### 1.2.3 节点调度优先级

`ux_friendly_pick_node()` 对就绪节点按以下优先级排序：

1. **输出节点**（`OUTPUT_NODE = True`）
2. **异步节点**（尽早启动以减少等待时间）
3. 阻塞输出节点的节点（1 跳）
4. 阻塞阻塞输出节点的节点（2 跳）
5. 任意可用节点（fallback）

### 1.3 节点 Schema 与类型系统

#### 1.3.1 V1 经典 API（类属性声明）

```python
class KSampler:
    CATEGORY = "sampling"
    INPUT_TYPES = classmethod(lambda cls: {
        "required": {
            "model":   ("MODEL",),
            "steps":   ("INT", {"default": 20, "min": 1, "max": 10000}),
            "sampler_name": (comfy.samplers.KSampler.SAMPLERS,),
        },
        "optional": {
            "latent_image_opt": ("LATENT",),
        }
    })
    RETURN_TYPES = ("LATENT",)
    FUNCTION     = "sample"
    OUTPUT_NODE  = False

    def sample(self, model, steps, sampler_name, **kwargs):
        ...
        return (result,)
```

#### 1.3.2 V3 现代 API（声明式 Schema）

```python
class LoadAudio(ComfyNode):
    @classmethod
    def define_schema(cls):
        return IO.Schema(
            node_id="LoadAudio",
            display_name="Load Audio",
            category="audio",
            inputs=[
                IO.AudioInput("audio"),
            ],
            outputs=[
                IO.Audio.Output(),
            ],
            is_output_node=False,
        )

    def execute(self, audio):
        return IO.NodeOutput(audio)
```

#### 1.3.3 强类型系统

每个输入/输出都绑定到一个 `io_type` 字符串，连线时强制类型匹配。内置类型包括：

| 类型 | 用途 |
|------|------|
| `MODEL` | 扩散模型对象 |
| `IMAGE` | 图像张量 `[B,H,W,C]` |
| `LATENT` | 潜在空间表示 |
| `STRING` | 文本字符串 |
| `INT` / `FLOAT` / `BOOLEAN` | 基本类型 |
| `COMBO` | 枚举下拉 |
| `*` | 通配符（接受任意类型） |

自定义类型可通过 `@comfytype` 装饰器扩展。

### 1.4 节点注册机制（插件架构）

```python
# 内置节点：nodes.py 直接定义，启动时自动注册
NODE_CLASS_MAPPINGS = {
    "KSampler": KSampler,
    "CheckpointLoaderSimple": CheckpointLoaderSimple,
}
NODE_DISPLAY_NAME_MAPPINGS = {
    "KSampler": "K-Sampler",
}

# 自定义节点：custom_nodes/ 目录下任意模块，只需导出上述两个字典
# 框架在启动时扫描并合并到全局注册表
```

### 1.5 输入缓存系统

ComfyUI 使用**输入签名缓存**：若节点的所有输入（含祖先节点的签名）与上次执行完全相同，则直接返回缓存结果，跳过执行。

缓存键生成流程：
1. 收集当前节点的所有祖先节点 ID（确定性顺序）
2. 对每个祖先生成签名：`class_type + IS_CHANGED结果 + 所有常量输入 + 上游链接引用`
3. 递归哈希得到当前节点的完整签名
4. 签名变化 → 缓存失效 → 重新执行

提供四种缓存策略：

| 策略 | 适用场景 |
|------|----------|
| `CLASSIC`（HierarchicalCache） | 默认，每次新提示后立即驱逐 |
| `LRU` | 固定大小，按最近使用顺序驱逐 |
| `RAM_PRESSURE` | 内存感知，低内存时自动驱逐 |
| `NONE`（NullCache） | 调试用，完全禁用缓存 |

### 1.6 懒加载（Lazy Evaluation）

节点可以将某些输入标记为 `lazy: true`，表示"如果不需要可以不执行上游节点"。

```python
# 节点声明懒输入
INPUT_TYPES = {
    "required": {...},
    "optional": {
        "mask": ("MASK", {"lazy": True}),  # 可能不需要
    }
}

def check_lazy_status(self, **kwargs):
    # 返回当前确实需要的输入名列表
    if self.needs_mask:
        return ["mask"]
    return []
```

弱链接不加入依赖图，只有 `check_lazy_status()` 明确要求时，才调用 `make_input_strong_link()` 将其转为强依赖。

### 1.7 子图展开（Subgraph Expansion）

节点可以在运行时动态替换自身为一组新节点：

```python
def execute(self, items):
    graph = GraphBuilder()
    for item in items:
        process_node = graph.node("ProcessItem", input=item)
    return {"expand": graph.finalize()}
```

执行系统：
1. 将新节点加入 `ephemeral_prompt`
2. 父节点返回 `PENDING` 并被 unstage
3. 等待所有子节点完成后，将子节点输出合并为父节点的输出

### 1.8 环检测

若无节点就绪且无外部异步阻塞，则说明 DAG 存在依赖环。系统通过**逆向拓扑排序**找出环中涉及的所有节点并报错。

---

## 二、与 Classifier 现有设计的对比

### 2.1 Workflow JSON 格式对比

| 维度 | ComfyUI | Classifier v3（现有设计） |
|------|---------|--------------------------|
| 节点存储 | `{node_id: {class_type, inputs}}` 扁平字典 | `{nodes: [{id, type, data}], edges: [{source, target}]}` 列表 |
| 边（连接）存储 | 内嵌在 `inputs` 字段：`[source_id, output_index]` | 独立 `edges` 列表 + `condition` 字段 |
| 依赖解析 | 解析 `inputs` 中的链接数组即可 | 需要额外遍历 `edges` 列表建图 |
| 支持多输出 | 通过 `output_index` 区分多个输出 socket | 当前只有 `condition` 字段做条件路由 |
| 动态图修改 | 支持 `ephemeral_prompt`（子图展开） | 尚未设计 |

**当前设计的局限**：Classifier 用节点 + 边的双列表格式，虽然易于在前端可视化（ReactFlow 原生格式），但在后端做依赖解析时需额外建图；且无法表达"从节点 A 的第 2 个输出口连到节点 B"这样的多输出场景。

### 2.2 执行状态对比

| 维度 | ComfyUI | Classifier v3（现有设计） |
|------|---------|--------------------------|
| 节点执行结果 | `SUCCESS / FAILURE / PENDING` 三态 | `pending / running / succeeded / failed`，无 waiting 状态 |
| 等待用户输入 | `PENDING` 状态 + `externalBlocks` 机制 | 规划中的 `manual-classifier` 尚无实现机制 |
| 异步节点支持 | 内置 `asyncio.Task` 支持 + 外部阻塞计数 | Go goroutine，无对等的挂起机制 |
| 节点级重试 | `PENDING` → `unstage` → 重新入队 | 仅有 WorkflowRun 级别的 `resume_node_id` |

### 2.3 节点注册机制对比

| 维度 | ComfyUI | Classifier v3（现有设计） |
|------|---------|--------------------------|
| 节点注册方式 | `NODE_CLASS_MAPPINGS` 字典，扫描 `custom_nodes/` | `NodeExecutor` 接口，尚未实现注册机制 |
| 节点发现 | 启动时自动扫描目录 | 需手动注册（规划中） |
| 节点 Schema | 声明式 `INPUT_TYPES` / `define_schema()` | 仅有 `WorkflowNode.Type` 字符串 + `Data any` |
| 类型验证 | 编译期强类型 + 运行时校验 | 无 Schema 层，类型未定义 |

### 2.4 缓存机制对比

| 维度 | ComfyUI | Classifier v3（现有设计） |
|------|---------|--------------------------|
| 缓存粒度 | 节点级，基于输入签名 | 仅有快照（snapshot），无计算缓存 |
| 跳过条件 | 输入签名未变化则跳过 | 无对等机制，每次执行都重跑所有节点 |
| 持久化 | 可通过外部 CacheProvider 持久化 | SQLite snapshot 可复用，但未对接执行引擎 |

---

## 三、融合建议

### 3.1 Workflow JSON 格式增强（GraphJSON）

**建议**：保持现有 `{nodes, edges}` 格式用于前端编辑器展示（与 ReactFlow 兼容），但在后端解析和持久化时，增加一个"后端执行图"结构，将边展开为节点 inputs 中的链接引用。

具体设计：在 `WorkflowGraph` 中新增 `NodePort` 概念：

```go
// 在 GraphJSON 中表示节点输入端口
type NodeInputPort struct {
    // 常量值（适用于简单参数）
    ConstValue any `json:"const_value,omitempty"`
    // 来自上游节点的链接
    LinkSource *NodeLink `json:"link_source,omitempty"`
}

type NodeLink struct {
    SourceNodeID    string `json:"source_node_id"`
    OutputPortIndex int    `json:"output_port_index"` // 支持多输出节点
}

// 节点输出端口描述
type NodeOutputPort struct {
    PortIndex int    `json:"port_index"`
    PortType  string `json:"port_type"` // FOLDER_CONTEXT | CATEGORY | PATH | BOOL | ...
}

// 升级后的 WorkflowNode
type WorkflowNode struct {
    ID           string                     `json:"id"`
    Type         string                     `json:"type"`
    Label        string                     `json:"label,omitempty"`
    Enabled      bool                       `json:"enabled"`
    Inputs       map[string]NodeInputPort   `json:"inputs"`  // 参数名 → 输入端口
    OutputPorts  []NodeOutputPort           `json:"output_ports,omitempty"`
    Config       any                        `json:"config,omitempty"` // 节点私有配置
    UIPosition   *NodePosition              `json:"ui_position,omitempty"` // 前端坐标，后端忽略
}

type NodePosition struct {
    X float64 `json:"x"`
    Y float64 `json:"y"`
}
```

`WorkflowEdge` 字段保留用于前端渲染，但后端执行引擎只读取 `WorkflowNode.Inputs` 中的 `LinkSource`。

### 3.2 引入 FolderContext 作为节点间数据载体

ComfyUI 每种类型有明确的数据契约（`IMAGE`、`LATENT` 等）。Classifier 应定义对等的类型系统。

**核心数据类型：`FolderContext`**

```go
// FolderContext 是节点间传递的核心数据对象，等同于 ComfyUI 中的 IMAGE/LATENT
type FolderContext struct {
    FolderID     string            `json:"folder_id"`
    FolderPath   string            `json:"folder_path"`
    FolderName   string            `json:"folder_name"`
    SourceDir    string            `json:"source_dir"`
    RelativePath string            `json:"relative_path"`
    FileList     []FileEntry       `json:"file_list,omitempty"` // 由 scan 阶段填充
    Category     string            `json:"category,omitempty"`  // 由分类节点填充
    Confidence   float64           `json:"confidence,omitempty"`
    Tags         map[string]string `json:"tags,omitempty"`      // 节点间传递的扩展属性
}

type FileEntry struct {
    Name      string `json:"name"`
    Extension string `json:"ext"`
    SizeBytes int64  `json:"size_bytes"`
}
```

节点类型系统（对应 ComfyUI 的 `io_type`）：

| Classifier 类型 | 对应内容 |
|----------------|---------|
| `FOLDER_CONTEXT` | 单个文件夹的完整上下文 |
| `CATEGORY` | 分类结果字符串（photo/video/manga/mixed/other） |
| `PATH` | 目标路径字符串 |
| `BOOLEAN` | 条件分支结果 |
| `VOID` | 无输出（用于动作节点：move、compress 等） |

### 3.3 NodeExecutor 接口增强

借鉴 ComfyUI 的三态执行结果，为 `NodeExecutor` 接口增加 `PENDING` 状态支持：

```go
type ExecutionStatus int

const (
    ExecutionSuccess ExecutionStatus = iota
    ExecutionFailure
    ExecutionPending // 等待外部输入或异步操作，相当于 ComfyUI 的 PENDING
)

type NodeExecutionResult struct {
    Status  ExecutionStatus
    Outputs []any  // 按输出端口索引排列，对应 ComfyUI 的 tuple 输出
    Error   error
    // PENDING 时填充：等待什么
    PendingReason  string // "waiting_user_input" | "async_task" | "subgraph"
    ResumeToken    string // 用于恢复时识别挂起点
}

type NodeExecutor interface {
    // Type 返回节点类型字符串，用于注册表查找
    Type() string

    // Schema 返回节点的输入/输出 Schema，用于前端渲染属性面板和类型验证
    Schema() NodeSchema

    // Execute 执行节点逻辑，返回三态结果
    Execute(ctx context.Context, input NodeExecutionInput) NodeExecutionResult

    // Resume 从 PENDING 状态恢复执行（例如用户确认后）
    Resume(ctx context.Context, input NodeExecutionInput, resumeToken string) NodeExecutionResult

    // Rollback 撤销已执行的操作
    Rollback(ctx context.Context, input NodeRollbackInput) error
}
```

#### NodeSchema 定义

```go
type NodeSchema struct {
    TypeID      string            `json:"type_id"`      // 唯一类型标识符
    DisplayName string            `json:"display_name"`
    Category    string            `json:"category"`     // "classifier" | "action" | "control"
    Description string            `json:"description,omitempty"`
    Inputs      []NodePortSchema  `json:"inputs"`
    Outputs     []NodePortSchema  `json:"outputs"`
}

type NodePortSchema struct {
    Name        string `json:"name"`
    PortType    string `json:"port_type"`  // FOLDER_CONTEXT | CATEGORY | PATH | BOOLEAN | VOID
    Required    bool   `json:"required"`
    Lazy        bool   `json:"lazy"`       // 借鉴 ComfyUI 懒输入，用于条件分支节点
    Description string `json:"description,omitempty"`
    // 若 PortType 为基础类型，可附带 Widget 配置（供前端渲染表单）
    Widget      *PortWidgetConfig `json:"widget,omitempty"`
}

type PortWidgetConfig struct {
    WidgetType string         `json:"widget_type"` // "text" | "select" | "number" | "path"
    Default    any            `json:"default,omitempty"`
    Options    []string       `json:"options,omitempty"` // 用于 select 类型
    Min        *float64       `json:"min,omitempty"`
    Max        *float64       `json:"max,omitempty"`
}
```

### 3.4 NodeExecutor 注册表

借鉴 ComfyUI 的 `NODE_CLASS_MAPPINGS`，实现 Go 版插件注册表：

```go
// NodeExecutorRegistry 管理所有已注册的节点执行器
type NodeExecutorRegistry struct {
    executors map[string]NodeExecutor // type_id → executor 实例
}

func NewNodeExecutorRegistry() *NodeExecutorRegistry {
    return &NodeExecutorRegistry{
        executors: make(map[string]NodeExecutor),
    }
}

func (registry *NodeExecutorRegistry) Register(executor NodeExecutor) {
    registry.executors[executor.Type()] = executor
}

func (registry *NodeExecutorRegistry) Get(typeID string) (NodeExecutor, bool) {
    executor, found := registry.executors[typeID]
    return executor, found
}

func (registry *NodeExecutorRegistry) ListSchemas() []NodeSchema {
    schemas := make([]NodeSchema, 0, len(registry.executors))
    for _, executor := range registry.executors {
        schemas = append(schemas, executor.Schema())
    }
    return schemas
}

// 暴露给前端：GET /api/node-types
// 前端通过此接口获得所有节点的 Schema，动态渲染属性面板
```

初始内置节点注册（Phase 2）：

```go
func buildDefaultRegistry(deps NodeDeps) *NodeExecutorRegistry {
    registry := NewNodeExecutorRegistry()
    registry.Register(NewExtRatioClassifierExecutor(deps.FolderFS))
    registry.Register(NewMoveExecutor(deps.MoveService))
    registry.Register(NewConditionExecutor())
    registry.Register(NewWaitExecutor()) // 对应 manual-classifier，PENDING 状态
    return registry
}
```

### 3.5 WorkflowRunner 执行循环

借鉴 ComfyUI 的 `PromptExecutor` + `ExecutionList`，设计 Go 版工作流执行循环：

```go
type WorkflowRunner struct {
    registry           *NodeExecutorRegistry
    workflowRunRepo    WorkflowRunRepository
    nodeRunRepo        NodeRunRepository
    nodeSnapshotRepo   NodeSnapshotRepository
    sseBroker          *sse.Broker
}

func (runner *WorkflowRunner) executeWorkflowRun(
    ctx context.Context,
    workflowRun *WorkflowRun,
    graph *WorkflowGraph,
) error {
    // 1. 构建拓扑执行列表（参考 ComfyUI ExecutionList）
    executionList := buildExecutionList(graph)

    // 2. 处理断点续传：跳过已成功的节点
    if workflowRun.ResumeNodeID != "" {
        executionList.skipCompletedNodes(workflowRun.CompletedNodeIDs)
    }

    // 3. 主执行循环
    for {
        readyNodes := executionList.getReadyNodes()
        if len(readyNodes) == 0 {
            if executionList.hasPendingExternalBlocks() {
                // 存在等待用户输入的节点，挂起整个 WorkflowRun
                return runner.suspendWorkflowRun(ctx, workflowRun)
            }
            if executionList.allComplete() {
                break
            }
            // 检测到环或死锁
            cycleNodes := executionList.findCycleNodes()
            return fmt.Errorf("workflow contains cycle or deadlock: %v", cycleNodes)
        }

        // 4. 按优先级选取节点（参考 ComfyUI ux_friendly_pick_node）
        nextNode := executionList.pickNode()
        result := runner.executeNode(ctx, workflowRun, nextNode, executionList)

        switch result.Status {
        case ExecutionSuccess:
            executionList.markComplete(nextNode.ID, result.Outputs)
        case ExecutionFailure:
            return runner.handleNodeFailure(ctx, workflowRun, nextNode, result.Error)
        case ExecutionPending:
            // 节点挂起（等待用户输入），增加外部阻塞计数
            executionList.addExternalBlock(nextNode.ID, result.ResumeToken)
            workflowRun.ResumeNodeID = nextNode.ID
        }
    }

    return runner.completeWorkflowRun(ctx, workflowRun)
}
```

### 3.6 NodeRun 状态扩展

在数据库 `node_runs` 表中增加 `waiting_input` 状态（对应 ComfyUI 的 `PENDING`）：

```sql
-- 节点运行状态：pending | running | succeeded | failed | waiting_input | skipped
-- waiting_input: 等待用户提供 manual-classifier 的分类确认
-- skipped:       断点续传时跳过的已完成节点
ALTER TABLE node_runs ADD COLUMN resume_token TEXT; -- PENDING 状态时填充，用于 Resume API
```

对应 Go struct 扩展：

```go
const (
    NodeRunStatusPending      = "pending"
    NodeRunStatusRunning      = "running"
    NodeRunStatusSucceeded    = "succeeded"
    NodeRunStatusFailed       = "failed"
    NodeRunStatusWaitingInput = "waiting_input" // 新增：等待用户输入
    NodeRunStatusSkipped      = "skipped"       // 新增：断点续传跳过
)
```

### 3.7 工作流定义验证（借鉴 ComfyUI 的 validate_prompt）

在保存或提交 `WorkflowDefinition` 时，后端应执行图合法性校验：

```go
type WorkflowValidationError struct {
    NodeID    string `json:"node_id"`
    ErrorType string `json:"error_type"` // missing_required_input | type_mismatch | unknown_node_type | cycle_detected
    Detail    string `json:"detail"`
}

func (runner *WorkflowRunner) ValidateGraph(graph *WorkflowGraph) []WorkflowValidationError {
    var errors []WorkflowValidationError

    // 1. 检查所有节点的 type 是否在注册表中存在
    for _, node := range graph.Nodes {
        if _, found := runner.registry.Get(node.Type); !found {
            errors = append(errors, WorkflowValidationError{
                NodeID:    node.ID,
                ErrorType: "unknown_node_type",
                Detail:    fmt.Sprintf("node type %q is not registered", node.Type),
            })
        }
    }

    // 2. 检查必填输入是否都有来源（常量值或连线）
    for _, node := range graph.Nodes {
        executor, found := runner.registry.Get(node.Type)
        if !found {
            continue
        }
        schema := executor.Schema()
        for _, inputSchema := range schema.Inputs {
            if !inputSchema.Required || inputSchema.Lazy {
                continue
            }
            port, exists := node.Inputs[inputSchema.Name]
            if !exists || (port.ConstValue == nil && port.LinkSource == nil) {
                errors = append(errors, WorkflowValidationError{
                    NodeID:    node.ID,
                    ErrorType: "missing_required_input",
                    Detail:    fmt.Sprintf("required input %q is not connected", inputSchema.Name),
                })
            }
        }
    }

    // 3. 检查类型兼容性
    // （遍历所有 LinkSource，对比源节点输出类型与目标节点输入类型）

    // 4. 环检测（反向拓扑排序）
    if cycles := detectCycles(graph); len(cycles) > 0 {
        for _, nodeID := range cycles {
            errors = append(errors, WorkflowValidationError{
                NodeID:    nodeID,
                ErrorType: "cycle_detected",
                Detail:    "node is part of a dependency cycle",
            })
        }
    }

    return errors
}
```

### 3.8 懒输入应用于条件节点

`condition` 节点（对应 ComfyUI 的条件分支）应使用懒输入，避免执行不会走到的分支：

```go
// ConditionNodeSchema 示例
NodeSchema{
    TypeID:      "condition",
    DisplayName: "条件分支",
    Category:    "control",
    Inputs: []NodePortSchema{
        {Name: "folder_context", PortType: "FOLDER_CONTEXT", Required: true},
        {Name: "condition_expr", PortType: "STRING",  Required: true,
            Widget: &PortWidgetConfig{WidgetType: "text", Default: "category == 'video'"}},
        {Name: "true_branch",   PortType: "FOLDER_CONTEXT", Required: false, Lazy: true},
        {Name: "false_branch",  PortType: "FOLDER_CONTEXT", Required: false, Lazy: true},
    },
    Outputs: []NodePortSchema{
        {Name: "result", PortType: "FOLDER_CONTEXT"},
    },
}
```

执行时，`condition` 节点先评估 `condition_expr`，再通过 `check_lazy_status()` 的等价机制，只请求与结果对应的那条分支的输入。

### 3.9 节点输出缓存（跳过未变更节点）

借鉴 ComfyUI 的输入签名缓存，为 Classifier 实现轻量级**节点结果缓存**，避免反复对同一文件夹跑相同分类器。

设计：在 `node_runs` 表中增加 `input_signature` 字段：

```sql
ALTER TABLE node_runs ADD COLUMN input_signature TEXT; -- SHA256(folder_path + node_config + file_list_hash)
```

当发起新 WorkflowRun 时，对每个节点计算 `input_signature`，若与上次成功的 NodeRun 的签名一致，则直接复用结果，将该节点标记为 `skipped`。

**签名计算要素**：

```
input_signature = SHA256(
  node_type +
  node_config_json +           // 节点参数（如 target_dir）
  folder_path +                // 文件夹路径
  file_list_hash +             // 文件列表 hash（文件名+大小+mtime）
  upstream_node_signatures...  // 上游节点的签名（递归，与 ComfyUI 一致）
)
```

---

## 四、对现有 v3 设计的修订建议

### 4.1 WorkflowGraph JSON 格式调整

**现有设计**（`docs/功能/工作流设计（版本3）.md`）：
```go
type WorkflowGraph struct {
    Nodes []WorkflowNode `json:"nodes"`
    Edges []WorkflowEdge `json:"edges"`
}
```

**建议调整**：
- 保留 `Edges` 用于前端 ReactFlow 渲染
- 节点的 `Inputs` 字段采用 ComfyUI 的链接数组语义（含 ConstValue 和 LinkSource）
- 增加 `Outputs []NodeOutputPort` 字段，支持多输出节点（例如 `condition` 节点的真/假两个输出）

### 4.2 NodeRun 状态补充

**现有设计** 的 `status` 只有 `pending | running | succeeded | failed`，

**建议新增**：
- `waiting_input`：等待用户通过 `manual-classifier` 确认
- `skipped`：断点续传或缓存命中时跳过

### 4.3 WorkflowRun 增加外部阻塞计数

在 `workflow_runs` 表增加 `external_blocks` 字段（对应 ComfyUI 的 `externalBlocks`），用于记录当前有多少节点处于 `waiting_input` 状态：

```sql
ALTER TABLE workflow_runs ADD COLUMN external_blocks INTEGER NOT NULL DEFAULT 0;
```

当 `external_blocks > 0` 时，`WorkflowRun.status` 为 `waiting_input`，此时其他 WorkflowRun 不受阻塞可继续执行（对应 Phase 3 中 `manual-classifier` 的并发要求）。

### 4.4 新增 GET /api/node-types 端点

参考 ComfyUI 的 `/object_info` 端点，暴露所有已注册节点的 Schema，供前端编辑器动态渲染属性面板：

```
GET /api/node-types
Response: {
  "types": [
    {
      "type_id": "ext-ratio-classifier",
      "display_name": "扩展名比例分类器",
      "category": "classifier",
      "inputs": [...],
      "outputs": [{"port_index": 0, "port_type": "CATEGORY"}]
    },
    ...
  ]
}
```

---

## 五、ComfyUI 不适合直接移植的部分

以下 ComfyUI 机制在 Classifier 场景中**不适合**直接应用：

| ComfyUI 机制 | 原因 |
|-------------|------|
| 子图展开（Subgraph Expansion） | Classifier 工作流是对"文件夹"的处理，不需要动态生成节点拓扑；静态 DAG 足够 |
| 内存压力驱逐缓存（RAMPressureCache） | NAS 环境的主要资源约束是磁盘 IO 而非内存；文件系统操作不占大量内存 |
| Batch 广播（`_map_node_over_list`） | Classifier 每个 WorkflowRun 对应一个文件夹，批处理在 Job 层面实现，不需要节点内 batch |
| PNG/Video 元数据嵌入 | Classifier 的结果存储在 SQLite，不需要把工作流元数据嵌入产物文件 |
| GPU 内存管理 | Classifier 无深度学习推理，无需此机制 |

---

## 六、实施优先级建议

根据对 Phase 2 验收标准的分析，建议按以下顺序融合 ComfyUI 理念：

### 第一批（Phase 2A 核心，必须）

1. **NodeExecutor 注册表** → 解耦节点实现与执行引擎
2. **FolderContext 数据类型** → 明确节点间的数据契约
3. **NodeExecutionResult 三态** → 支撑断点续传和 manual-classifier
4. **拓扑排序执行循环** → 替代当前的硬编码 goroutine

### 第二批（Phase 2B，API 扩展）

5. **NodeSchema + GET /api/node-types** → 支撑前端属性面板
6. **WorkflowGraph 格式升级**（增加 `Inputs.LinkSource`）→ 支持多输出节点
7. **环检测 + 图校验** → 在 POST /api/workflow-defs 时做前置验证

### 第三批（Phase 3 分类器节点化）

8. **懒输入语义** → 用于 `condition` 节点的分支求值
9. **NodeRun 输入签名缓存** → 避免重复分类，提升扫描性能
10. **`waiting_input` 状态 + external_blocks** → 支持 `manual-classifier` 的挂起/恢复

---

## 七、参考资料

- [ComfyUI Graph Execution System - DeepWiki](https://deepwiki.com/Comfy-Org/ComfyUI/2.2-graph-execution-system)
- [ComfyUI Caching System - DeepWiki](https://deepwiki.com/Comfy-Org/ComfyUI/2.3-caching-system)
- [ComfyUI Node System - DeepWiki](https://deepwiki.com/Comfy-Org/ComfyUI/2.4-node-system)
- [ComfyUI Workflow JSON Format - DeepWiki](https://deepwiki.com/Comfy-Org/ComfyUI/7.3-workflow-json-format)
- [ComfyUI Execution Model Inversion Guide](https://docs.comfy.org/development/comfyui-server/execution_model_inversion_guide)
- [ComfyUI 源码 execution.py](https://github.com/Comfy-Org/ComfyUI/blob/4a8cf359/execution.py)
- [ComfyUI 源码 comfy_execution/graph.py](https://github.com/Comfy-Org/ComfyUI/blob/4a8cf359/comfy_execution/graph.py)
