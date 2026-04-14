# 技术栈选型

> 版本：v2.0 | 日期：2026-03-19

## 技术栈总览

| 层次 | 选型 |
|------|------|
| 后端语言 | Go 1.23 |
| 后端框架 | Gin |
| 数据库 | SQLite (modernc.org/sqlite) |
| 视频处理 | FFmpeg (exec 调用) |
| 前端框架 | React 18 + TypeScript |
| 前端构建 | Vite |
| 状态管理 | Zustand |
| 组件库 | shadcn/ui + Tailwind CSS |
| 工作流 UI | React Flow |
| 拖拽 | @dnd-kit/core |
| 部署 | Docker + docker-compose |

## 后端框架：Gin

选 **Gin** 而非 Echo/Fiber/stdlib：
- 生态最成熟，middleware 丰富（CORS、日志、recover 开箱即用）
- 路由性能足够，NAS 场景不是瓶颈
- 社区文档最多，单人开发友好
- Fiber 基于 fasthttp，与 net/http 生态不兼容，引入不必要风险

## 数据持久化：SQLite（modernc.org/sqlite）

- 纯 Go 实现，无 CGO，Docker 构建无额外依赖
- 比 JSON 文件更易查询/更新，比 PostgreSQL 轻量百倍
- 数据量极小（千级 Folder 记录），SQLite 完全够用
- 文件存于 /config/classifier.db，随 named volume 持久化

## 前端状态管理：Zustand

选 **Zustand** 而非 Redux/Jotai：
- API 极简，无 boilerplate，单人项目最合适
- 支持 slice 模式，可按模块拆分 store
- 比 Jotai 更适合管理复杂异步任务状态

## 前端组件库：shadcn/ui + Tailwind CSS

- shadcn/ui 是复制到项目模式，无版本锁定风险
- Tailwind 构建产物小，适合 NAS 低配机器
- 组件质量高（Table、Dialog、Progress、Badge 都需要）

## 工作流编辑器：React Flow

- 专为节点图/DAG 设计，开箱即用
- 支持自定义节点渲染、连线类型、缩放/拖拽
- 与 Zustand 配合良好
- 替代方案：Rete.js（重）、自研 Canvas（成本高）

## 视频处理：FFmpeg（exec.Command）

- NAS 镜像通过 apk 安装 ffmpeg，无需 CGO
- 通过 exec.Command 调用，输出解析简单
- 替代方案：ffmpeg-go（需 CGO，Docker 构建复杂）

## 实时推送：SSE（Server-Sent Events）

- 单向推送（服务端→客户端），适合进度通知场景
- 比 WebSocket 实现简单，无需握手协议
- 浏览器原生支持，无额外依赖

## 压缩：archive/zip（标准库）

- Go 标准库，无外部依赖
- 快速压缩（Store 模式）满足需求
- 兼容性最好，Windows/Mac 均可直接解压
- 替代方案：zstd（更快但格式不通用）

## 技术决策记录

| 决策 | 选择 | 放弃 | 理由 |
|------|------|------|------|
| 后端框架 | Gin | Echo, Fiber | 生态最成熟，单人开发友好 |
| 数据库 | SQLite | JSON文件, PostgreSQL | 无 CGO，轻量，够用 |
| 前端状态 | Zustand | Redux, Jotai | 极简 API，无 boilerplate |
| 实时推送 | SSE | WebSocket | 单向推送，实现更简单 |
| 压缩 | archive/zip | zstd, 7z | 标准库，兼容性最好 |
| 视频处理 | FFmpeg exec | ffmpeg-go | 无 CGO 依赖，更易 Docker 化 |
| 组件库 | shadcn/ui | Ant Design, MUI | 无运行时，构建产物小 |
| 工作流 UI | React Flow | Rete.js, 自研 | 专为 DAG 设计，社区活跃 |
| 工作流引擎 | 自研 DAG 执行器 | Temporal, Airflow | 轻量场景，无需分布式调度 |
| 回退机制 | Snapshot 表记录路径 | 文件副本备份 | 省磁盘空间 |
| 审计日志 | SQLite append-only | 文件日志 | 可查询、可导出 |
| 重命名 UX | Token 变量组合 | 正则表达式 | 面向普通用户 |
