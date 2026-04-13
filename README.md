# Classifier

一款面向 NAS Docker 部署的 Web 端媒体文件夹整理工具。

## 功能特性

已实现：

- 媒体分类：写真、视频、混合、漫画自动识别（扩展名 + 比例分析）
- 文件夹扫描与列表管理（扫描任务持久化 + 实时进度）
- 文件夹移动任务（Job 持久化 + SSE 实时进度）
- Snapshot 快照：创建、查询、回滚、时间线 detail 元数据
- AuditLog 审计日志：记录所有文件操作并提供 HTTP 查询接口
- Config 配置读取与保存（支持多扫描目录）
- Folder 记录隐藏与恢复扫描（不改动真实文件）
- 中文前端仪表盘：目录网格/列表、最近任务、最近日志、通知提示
- 前端 Job 历史查看与轮询
- NAS 友好的 Docker / docker-compose 部署

规划中（Phase 2-5）：

- 节点式可视化工作流编辑器（ComfyUI 风格 DAG）
- 批量重命名（token 模板，无须正则）
- 快速 ZIP 压缩（图片目录）
- Emby 规范视频缩略图生成（FFmpeg）

## 文档

- [文档分类索引](docs/文档目录.md)

### 系统设计
- [系统架构总览（当前实现）](docs/架构/系统架构总览（当前实现）.md)
- [架构概览](docs/架构/架构概览.md)
- [技术栈](docs/架构/技术栈.md)
- [项目需求](docs/架构/项目需求.md)

### 功能规格
- [接口设计](docs/功能/接口设计.md)（最新：[接口设计（版本3）](docs/功能/接口设计（版本3）.md)）
- [数据模型](docs/架构/数据模型.md)（最新：[数据模型（版本3）](docs/架构/数据模型（版本3）.md)）
- [前端设计](docs/功能/前端设计.md)（最新：[前端设计（版本3）](docs/功能/前端设计（版本3）.md)）
- [节点式工作流](docs/功能/工作流设计.md)（最新：[工作流设计（版本3）](docs/功能/工作流设计（版本3）.md)）
- [文件分类算法](docs/功能/分类规则.md)（最新：[分类规则（版本3）](docs/功能/分类规则（版本3）.md)）
- [重命名编辑器](docs/功能/重命名编辑器.md)（最新：[重命名编辑器（版本3）](docs/功能/重命名编辑器（版本3）.md)）
- [视频缩略图规范](docs/功能/视频缩略图规范.md)
- [快照系统](docs/功能/快照系统.md)（最新：[快照系统（版本3）](docs/功能/快照系统（版本3）.md)）
- [审计日志系统](docs/功能/审计日志.md)

### 部署
- [Docker 部署指南](docs/部署/Docker部署指南.md)
- [极空间部署文档](docs/部署/极空间部署指南.md)

### 规划
- [开发路线图](docs/规划/开发路线图.md)（最新：[开发路线图（版本3）](docs/规划/开发路线图（版本3）.md)）
- [技术研究](docs/规划/技术研究.md)

## 本地开发

### 环境依赖

- Go 1.23+
- Node.js 20+
- npm

### 1. 准备本地目录

```bash
mkdir -p .local/source .local/target .local/config
```

在 `.local/source/` 下放几个测试文件夹，例如：

```text
.local/source/
  sample-album/
  sample-video/
```

### 2. 启动后端

```bash
cd backend
CONFIG_DIR="$(pwd)/../.local/config" \
SOURCE_DIR="$(pwd)/../.local/source" \
TARGET_DIR="$(pwd)/../.local/target" \
PORT=8080 \
CGO_ENABLED=0 \
go run ./cmd/server
```

后端接口：

- `http://localhost:8080/health`
- `http://localhost:8080/api/...`

### 3. 启动前端

另开一个终端：

```bash
cd frontend
npm install
npm run dev
```

前端地址：

- `http://localhost:5173`

Vite 已配置将 `/api` 代理到 `http://localhost:8080`。

### 4. 本地验证流程

1. 打开 `http://localhost:5173/settings`
2. 配置一个或多个扫描目录并保存
3. 返回 `http://localhost:5173`
4. 点击“扫描”，观察扫描进度条、任务面板和最近日志
5. 查看目录卡片/列表中的分类结果与来源目录信息
6. 点击“快照时间线”检查分类快照 detail
7. 选中文件夹，点击“移动所选”验证 move job
8. 进入 `http://localhost:5173/jobs` 查看任务历史
9. 隐藏后可通过 Restore 恢复扫描，不影响真实文件

### 5. 构建命令

后端：

```bash
cd backend
CGO_ENABLED=0 go build ./...
CGO_ENABLED=0 go test ./...
```

前端：

```bash
cd frontend
npm run typecheck
npm run lint
npm run build
```

## 开发进度

Phase 1 MVP + Phase 1.5 前后端均已完成。当前仓库已具备扫描任务持久化、扫描/移动实时反馈、审计日志查询、中文仪表盘、快照时间线和多扫描目录配置。下一阶段重点仍然是通用工作流引擎与节点级输出目录配置。详见 `docs/规划/开发路线图.md`。
