# Directory Structure: Classifier

├── backend/
│   ├── cmd/
│   │   ├── server/
│   │   │   ├── web/
│   │   │   │   ├── dist/
│   │   │   │   │   ├── assets/
│   │   │   │   │   │   ├── index-BnkoZNN_.css
│   │   │   │   │   │   ├── index-DsZa7-cg.js
│   │   │   │   │   ├── favicon.svg
│   │   │   │   │   ├── icons.svg
│   │   │   │   │   ├── index.html
│   │   │   ├── health_test.go
│   │   │   ├── main.go
│   ├── internal/
│   │   ├── config/
│   │   │   ├── config.go
│   │   ├── db/
│   │   │   ├── migrations/
│   │   │   │   ├── 001_initial.sql
│   │   │   │   ├── 002_indexes.sql
│   │   │   │   ├── 003_v3_jobs.sql
│   │   │   │   ├── 004_scan_history.sql
│   │   │   │   └── ... (11 more Others files)
│   │   │   ├── db.go
│   │   │   ├── db_test.go
│   │   ├── fs/
│   │   │   ├── adapter.go
│   │   │   ├── mock_adapter.go
│   │   │   ├── mock_adapter_test.go
│   │   │   ├── os_adapter.go
│   │   │   └── ... (1 more Others files)
│   │   ├── handler/
│   │   │   ├── audit.go
│   │   │   ├── audit_test.go
│   │   │   ├── config.go
│   │   │   ├── config_test.go
│   │   │   └── ... (14 more Others files)
│   │   ├── logger/
│   │   │   ├── logger.go
│   │   ├── repository/
│   │   │   ├── audit_repo.go
│   │   │   ├── audit_repo_test.go
│   │   │   ├── config_repo.go
│   │   │   ├── config_repo_test.go
│   │   │   └── ... (16 more Others files)
│   │   ├── service/
│   │   │   ├── audit.go
│   │   │   ├── audit_test.go
│   │   │   ├── classification_batch_helpers.go
│   │   │   ├── classified_entry_parse.go
│   │   │   └── ... (67 more Others files)
│   │   ├── sse/
│   │   │   ├── broker.go
│   │   │   ├── broker_test.go
│   ├── migrations/
│   │   ├── 001_initial.sql
│   │   ├── 002_indexes.sql
│   ├── web/
│   │   ├── dist/
│   ├── go.mod
│   ├── go.sum
├── docs/
│   ├── 功能/
│   │   ├── 分类规则.md
│   │   ├── 分类规则（版本3）.md
│   │   ├── 前端设计.md
│   │   ├── 前端设计（版本3）.md
│   │   └── ... (14 more Others files)
│   ├── 架构/
│   │   ├── 技术栈.md
│   │   ├── 数据模型.md
│   │   ├── 数据模型（版本3）.md
│   │   ├── 架构概览.md
│   │   └── ... (5 more Others files)
│   ├── 规划/
│   │   ├── comfyui-workflow-research.md
│   │   ├── 工作流节点设计规范.md
│   │   ├── 开发路线图.md
│   │   ├── 待优化项.md
│   │   └── ... (2 more Others files)
│   ├── 部署/
│   │   ├── Docker部署指南.md
│   │   ├── 极空间部署指南.md
│   ├── 1.html
│   ├── 2.html
│   ├── 3.html
│   ├── 文档目录.md
│   └── ... (2 more Others files)
├── frontend/
│   ├── e2e/
│   │   ├── fullstack/
│   │   │   ├── fullstack.spec.ts
│   │   │   ├── start-server.mjs
│   │   ├── support/
│   │   │   ├── api.ts
│   │   ├── tests/
│   │   │   ├── folder-snapshots.spec.ts
│   │   │   ├── jobs.spec.ts
│   │   │   ├── workflow-defs.spec.ts
│   ├── playwright-report/
│   │   ├── index.html
│   ├── public/
│   │   ├── favicon.svg
│   │   ├── icons.svg
│   ├── src/
│   │   ├── api/
│   │   │   ├── auditLogs.ts
│   │   │   ├── client.ts
│   │   │   ├── config.ts
│   │   │   ├── folders.ts
│   │   │   └── ... (7 more Videos files)
│   │   ├── assets/
│   │   │   ├── hero.png
│   │   │   ├── react.svg
│   │   │   ├── vite.svg
│   │   ├── components/
│   │   │   ├── workflow-preview/
│   │   │   │   ├── previewTypes.ts
│   │   │   │   ├── previewUtils.ts
│   │   │   │   ├── ClassificationPreviewInline.tsx
│   │   │   │   ├── ProcessingPreviewInline.tsx
│   │   │   ├── ConfiguredPathField.tsx
│   │   │   ├── CronExpressionField.tsx
│   │   │   ├── DirPicker.tsx
│   │   │   ├── Layout.tsx
│   │   │   └── ... (5 more Others files)
│   │   ├── hooks/
│   │   │   ├── useSSE.ts
│   │   ├── lib/
│   │   │   ├── particles.ts
│   │   │   ├── utils.ts
│   │   │   ├── workflowGraphFolderPicker.ts
│   │   ├── pages/
│   │   │   ├── AuditLogsPage.tsx
│   │   │   ├── FolderListPage.tsx
│   │   │   ├── JobHistoryPage.tsx
│   │   │   ├── JobsPage.tsx
│   │   │   └── ... (4 more Others files)
│   │   ├── store/
│   │   │   ├── activityStore.ts
│   │   │   ├── configStore.ts
│   │   │   ├── folderStore.ts
│   │   │   ├── jobStore.ts
│   │   │   └── ... (5 more Videos files)
│   │   ├── types/
│   │   │   ├── index.ts
│   │   ├── App.css
│   │   ├── App.tsx
│   │   ├── index.css
│   │   ├── main.tsx
│   ├── test-results/
│   ├── playwright.config.ts
│   ├── playwright.fullstack.config.ts
│   ├── vite.config.ts
│   ├── eslint.config.js
│   ├── index.html
│   ├── package-lock.json
│   ├── package.json
│   └── ... (6 more Others files)
├── script/
├── AGENTS.md
├── dev.bat
├── dev.sh
├── docker-compose.yml
└── ... (2 more Others files)