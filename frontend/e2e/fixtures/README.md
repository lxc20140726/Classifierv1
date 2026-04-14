# E2E Fixtures

这里存放 E2E 场景使用的固定资源与模板定义。

- `fullstack/builders/directoryBuilders.ts`：目录模板组合器
- `fullstack/runtime/fixtureRuntime.ts`：运行时真实样本生成与目录落盘

设计约束：

- 所有测试都复制到独立 `runtime/<scenario-id>` 目录执行。
- 不直接修改仓库内 fixtures 原始内容。
- 样本包含图片、视频、压缩包后缀、文本与 PDF，便于组合复杂目录树。
