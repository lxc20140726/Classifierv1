# Fullstack E2E Framework

## 目标

- 真实前端 + 真实后端 + 真实目录 + 真实 SQLite + 真实 ffmpeg
- 场景以 TypeScript Builder 声明
- 按标签筛选执行

## 目录

- `framework/`: 公共类型、标签过滤、API 助手
- `builders/`: 目录模板 / 工作流模板 / 断言模板
- `runtime/`: 场景独立运行目录与服务生命周期
- `scenarios/`: 场景注册入口

## 运行方式

```bash
cd frontend
npm run e2e:fullstack
E2E_TAGS=classify npm run e2e:fullstack:tags
E2E_TAGS=rollback npm run e2e:fullstack:tags
npm run e2e:docker
```

`E2E_TAGS` 支持多个值，逗号分隔，例如：`E2E_TAGS=smoke,process`

## 工作流配置来源

- fullstack 用例会为每个场景创建独立运行目录（含独立 `config`）。
- 运行前会尝试从 `../.e2e-fullstack/config`（其次 `~/.classifier/config`）复制 `classifier.db*` 到场景 `config`，用于复用你已配置的真实工作流（如“分类/处理”）。
- 可用 `E2E_WORKFLOW_CONFIG_DIR=/abs/path/to/config` 显式指定来源目录。
- 可用 `E2E_WORKFLOW_CONFIG_DIR=none` 禁用复制，完全使用空配置启动。
