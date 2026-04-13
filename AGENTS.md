# AGENTS.md — Classifier

Reference for coding agents working in this repository.
Project status: **Phase 1 MVP + v3 workflow engine fully implemented**.

---

## Project Overview

Classifier is a NAS-deployed media folder organizer.
Single container: Go backend + React SPA + FFmpeg runtime.

Implemented scope:
- Folder scanning, media classification, category/status management
- Folder move, soft delete, restore
- Snapshot revert flow + node-level snapshots
- Audit logging
- SSE progress channel
- Persisted jobs + workflow runs + node runs
- Full workflow editor (graph editor with ReactFlow)
- Workflow execution engine (WorkflowRunner + NodeExecutor registry)
- Node-type config panels, directory picker, run progress visualization

---

## Stack

- **Backend**: Go 1.26, Gin, SQLite via `modernc.org/sqlite`, SSE
- **Frontend**: React 19, TypeScript 5.9, Vite 8, Zustand 5, Tailwind CSS 3, React Router v6, ReactFlow
- **Infra**: Docker multi-stage build, docker-compose, Alpine runtime

---

## Repository Layout

```text
Classifier/
├── backend/
│   ├── cmd/server/              # main.go entrypoint + embedded frontend assets
│   ├── internal/
│   │   ├── config/              # env config loading
│   │   ├── db/                  # sqlite open + embedded migrations
│   │   ├── fs/                  # filesystem adapter layer (ALL os.* calls here only)
│   │   ├── handler/             # Gin HTTP handlers
│   │   ├── repository/          # SQLite repositories + models
│   │   ├── service/             # classifier / scanner / move / snapshot / audit / workflow runner
│   │   └── sse/                 # SSE broker
│   └── migrations/              # canonical SQL migration files
├── frontend/
│   └── src/
│       ├── api/                 # domain API functions (pure async, typed)
│       ├── components/          # shared UI (DirPicker, SnapshotDrawer, ...)
│       ├── hooks/               # useSSE, etc.
│       ├── lib/                 # cn() utility
│       ├── pages/               # FolderListPage, SettingsPage, WorkflowEditorPage, ...
│       ├── store/               # Zustand stores
│       └── types/               # all shared types (no any)
├── docs/
├── Dockerfile
├── docker-compose.yml
└── .env.example
```

---

## Build / Test / Run

### Backend

```bash
cd backend
CGO_ENABLED=0 go build ./...
CGO_ENABLED=0 go build ./cmd/server
CGO_ENABLED=0 go test ./...
go vet ./...

# Single package
CGO_ENABLED=0 go test ./internal/handler/...

# Single test function
CGO_ENABLED=0 go test ./internal/handler/... -run TestFolderHandler

# Single sub-test (table row)
CGO_ENABLED=0 go test -v ./internal/handler/... -run TestFolderHandler/list_returns_folders
```

### Frontend

```bash
cd frontend
npm install
npm run typecheck   # tsc --noEmit — must pass before declaring done
npm run lint        # eslint, zero warnings allowed
npm run build
npm run dev
```

### Local Dev

```bash
mkdir -p .local/source .local/target .local/config .local/delete-staging

# Backend
cd backend
CONFIG_DIR="$(pwd)/../.local/config" SOURCE_DIR="$(pwd)/../.local/source" \
TARGET_DIR="$(pwd)/../.local/target" DELETE_STAGING_DIR="$(pwd)/../.local/delete-staging" \
PORT=8080 CGO_ENABLED=0 go run ./cmd/server

# Frontend (separate terminal)
cd frontend && npm run dev
```

---

## Backend API Endpoints

```
GET    /health
GET    /api/events                          SSE stream
GET    /api/folders
POST   /api/folders/scan
GET    /api/folders/:id
POST   /api/folders/:id/restore
PATCH  /api/folders/:id/category
PATCH  /api/folders/:id/status
DELETE /api/folders/:id                     soft delete
GET    /api/jobs
POST   /api/jobs                            start workflow job → { job_id }
GET    /api/jobs/:id
GET    /api/jobs/:id/progress
POST   /api/jobs/move
GET    /api/jobs/:id/workflow-runs
GET    /api/workflow-runs/:id
POST   /api/workflow-runs/:id/resume
POST   /api/workflow-runs/:id/provide-input
POST   /api/workflow-runs/:id/rollback
GET    /api/workflow-defs
POST   /api/workflow-defs
GET    /api/workflow-defs/:id
PUT    /api/workflow-defs/:id
DELETE /api/workflow-defs/:id
GET    /api/snapshots?folder_id=...
GET    /api/snapshots?job_id=...
POST   /api/snapshots/:id/revert
GET    /api/config
PUT    /api/config
GET    /api/node-types
GET    /api/audit-logs
GET    /api/fs/dirs?path=...               list subdirectories for dir picker
```

---

## Go Rules

- Always build and test with `CGO_ENABLED=0`; never use `mattn/go-sqlite3`
- All filesystem access via `internal/fs.FSAdapter` — never call `os.*` in handlers or services
- Keep `context.Context` as first argument for all blocking/IO work
- Wrap errors: `fmt.Errorf("name: %w", err)`
- Table-driven tests with `t.Run(name, func(t *testing.T) {...})`
- In-memory SQLite per test: `file:classifier_<pkg>_<n>?cache=shared&mode=memory`
- Use `atomic.AddUint64` counter for unique DB names in parallel tests
- `t.Helper()` on all helpers; `t.Cleanup()` for teardown
- Handler tests: `gin.SetMode(gin.TestMode)` + `httptest.NewRecorder()` — no live server
- Bugfix rule: fix minimally, never refactor while fixing

### Naming & Types

- DB struct tags: `db:"snake_case"`
- Handler structs accept interface dependencies, not concrete types
- IDs: `string` (UUID via `github.com/google/uuid`)
- Pointer receiver for all structs with state or interface implementations
- Nullable DB columns → pointer types (`*time.Time`, etc.)
- JSON blobs in DB → `json.RawMessage`
- Enum validation: `map[string]struct{}`, not switch

### Import Order (3 blocks, blank-line separated)

```go
import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"

	"github.com/liqiye/classifier/internal/repository"
	"github.com/liqiye/classifier/internal/service"
)
```

---

## Frontend Rules

### Hard Rules

- Strict TypeScript only — no `any`, no `@ts-ignore`, no `@ts-expect-error`
- Tailwind utility classes only — no inline styles, no CSS modules
- All user-facing copy is hardcoded **Chinese**
- Use `cn()` from `src/lib/utils.ts` for conditional class merging
- ESLint must pass with **zero warnings**: `npm run lint`
- `ApiRequestError` (from `src/api/client.ts`) extends `Error` — never throw plain strings
- No unnecessary comments — code must be self-documenting

### Import Order

```ts
import { useState } from 'react'
import { Trash2 } from 'lucide-react'

import { revertSnapshot } from '@/api/snapshots'
import { ApiRequestError } from '@/api/client'
import { DirPicker } from '@/components/DirPicker'
import { cn } from '@/lib/utils'
import { useSnapshotStore } from '@/store/snapshotStore'
import type { Snapshot } from '@/types'
```

Use `import type { ... }` for type-only imports. Always use `@/` alias.

### Architecture

| Layer | Location | Notes |
|---|---|---|
| HTTP client + error | `src/api/client.ts` | `request<T>()` helper; 204 → `undefined as T` |
| Domain API functions | `src/api/<domain>.ts` | Pure async, typed return |
| Global state | `src/store/<name>Store.ts` | Zustand; owns fetching + mutations |
| SSE | `src/hooks/useSSE.ts` | Single hook, all events |
| Page components | `src/pages/` | Thin — delegate to stores/API |
| Shared UI | `src/components/` | Avoid direct store access unless necessary |
| Types | `src/types/index.ts` | All shared types; no `any` |

### Component Conventions

- Export props interface: `export interface MyComponentProps { ... }`
- Constant label/class maps declared outside component body
- Format dates: `new Date(value).toLocaleString('zh-CN')`
- Icons: `lucide-react` only
- Available primitives: `@radix-ui/react-slot`, `class-variance-authority`, `clsx`, `tailwind-merge`
- Directory selection: use existing `DirPicker` + `DirPickerField` pattern from `WorkflowEditorPage`

### Key Files

- `src/pages/FolderListPage.tsx` — uses `useFolderStore`
- `src/pages/SettingsPage.tsx` — reads/writes `/api/config`; uses `DirPicker`
- `src/pages/WorkflowEditorPage.tsx` — full graph editor; node config panels; run modal; status overlay
- `src/pages/WorkflowDefsPage.tsx` — workflow definition list
- `src/pages/JobsPage.tsx` — job list + workflow run detail
- `src/components/SnapshotDrawer.tsx` — snapshot state via `useSnapshotStore`
- `src/components/DirPicker.tsx` — filesystem directory browser modal
- `src/store/folderStore.ts` — folder list + scan progress
- `src/store/workflowRunStore.ts` — workflow runs + node runs + SSE event handling
- `src/store/jobStore.ts` — job polling state

---

## Architecture Constraints

- SQLite only
- SSE (not WebSocket) for push events
- Snapshot record must exist before mutating moves
- Job progress queryable over HTTP, not SSE-only
- Folder deletion is soft delete — do not reintroduce hard delete semantics
- Backend binary serves embedded frontend from `backend/cmd/server/web/dist`
- `POST /api/jobs` returns `{ job_id: string }` — not `{ data: { id } }`

---

## What Is Not Yet Implemented

- Rename editor UI
- Compression pipeline UI
- Thumbnail generation