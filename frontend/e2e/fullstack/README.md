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
