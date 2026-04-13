# Classifier — Phase 1 MVP Implementation Plan

> Generated: 2026-03-19 | Status: Ready for Implementation

## Hard Constraints (Non-Negotiable)

| Constraint | Source |
|---|---|
| `CGO_ENABLED=0` everywhere | AGENTS.md |
| All `os.*` calls only in `internal/fs` adapter | AGENTS.md |
| SSE not WebSocket | AGENTS.md |
| Single Go binary embeds `frontend/dist/` via `//go:embed` | DEPLOYMENT.md |
| Snapshot record created before any file mutation | SNAPSHOT.md |
| `modernc.org/sqlite` (no `mattn/go-sqlite3`) | AGENTS.md |
| WAL mode on SQLite | TECH_STACK.md |
| No CGO — `CGO_ENABLED=0 go build ./...` must always pass | AGENTS.md |

## NAS Deployment Notes (极空间 Research)

- Default to `user: "0:0"` (root) in docker-compose.yml for 极空间 compatibility
- Do NOT use `privileged: true` unless container needs Docker/network/device control
- Real NAS paths on 极空间 are under `/tmp/zfsv3/.../data/...` — document this
- Allow UID/GID override via `PUID`/`PGID` env vars as a best practice
- Required capabilities for file ops: `CHOWN`, `DAC_OVERRIDE`, `FOWNER`, `SETGID`, `SETUID`

## Repository Layout (to create)

```
Classifier/
├── backend/
│   ├── cmd/server/main.go
│   ├── internal/
│   │   ├── config/
│   │   ├── handler/
│   │   ├── service/
│   │   ├── repository/
│   │   ├── sse/
│   │   └── fs/
│   ├── migrations/
│   │   ├── 001_initial.sql
│   │   └── 002_indexes.sql
│   ├── web/dist/.gitkeep
│   ├── go.mod
│   └── go.sum
├── frontend/
│   ├── src/
│   │   ├── api/
│   │   ├── components/
│   │   ├── hooks/
│   │   ├── pages/
│   │   ├── store/
│   │   └── types/
│   ├── package.json
│   └── vite.config.ts
├── .env.example
├── Dockerfile
├── docker-compose.yml
└── AGENTS.md
```

## Phase 1 Move Architecture (No Scheduler)

```
POST /api/jobs/move → 202 Accepted → goroutine:
  operationID = uuid.New()
  for each folderID:
    1. SnapshotService.CreateBefore(ctx, operationID, folderID, "move")
    2. FSAdapter.MkdirAll(ctx, targetDir)
    3. FSAdapter.MoveDir(ctx, src, filepath.Join(targetDir, folderName))
    4. SnapshotService.CommitAfter(ctx, snapshotID, after)
    5. FolderRepo.UpdatePath(ctx, folderID, newPath)
    6. AuditLogService.Write(ctx, "move", result)
    7. SSEBroker.Publish("job.progress", {operationID, done, total})
  SSEBroker.Publish("job.done", {operationID})
```

No `jobs` table in Phase 1.

## Execution Waves

```
Wave 1 — Foundation (all 4 parallel):
  T1  Backend scaffold
  T2  FS adapter
  T3  DB layer
  T4  Frontend scaffold

Wave 2 — Core services (parallel where dep-free):
  T5  Classifier Service   (pure, no deps)
  T6  Scanner Service      (needs T2+T3+T5)
  T7  Folder CRUD API      (needs T1+T3)
  T8  SSE Broker           (needs T1)
  T9  Frontend stores+useSSE (needs T4)

Wave 3 — Mutating services:
  T10 Move Service         (needs T2+T3)
  T11 Snapshot Service     (needs T3)
  T12 AuditLog Service     (needs T3)

Wave 4 — Integration layer:
  T13 Move Handler         (needs T10+T11+T12+T8)
  T14 Snapshot+Config APIs (needs T11+T3)
  T15 FolderListPage       (needs T9)
  T16 SettingsPage         (needs T9)
  T17 SnapshotDrawer       (needs T9)

Wave 5 — Infrastructure:
  T18 Dockerfile + docker-compose + .env.example

Wave 6 — Smoke test:
  T19 CGO_ENABLED=0 go build + npm run build + docker compose build
```

## SQLite Schema

```sql
-- 001_initial.sql
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS folders (
    id              TEXT PRIMARY KEY,
    path            TEXT NOT NULL UNIQUE,
    name            TEXT NOT NULL,
    category        TEXT NOT NULL DEFAULT 'unknown',
    category_source TEXT NOT NULL DEFAULT 'auto',
    status          TEXT NOT NULL DEFAULT 'pending',
    image_count     INTEGER NOT NULL DEFAULT 0,
    video_count     INTEGER NOT NULL DEFAULT 0,
    total_files     INTEGER NOT NULL DEFAULT 0,
    total_size      INTEGER NOT NULL DEFAULT 0,
    marked_for_move INTEGER NOT NULL DEFAULT 0,
    scanned_at      DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at      DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS snapshots (
    id             TEXT PRIMARY KEY,
    job_id         TEXT NOT NULL,
    folder_id      TEXT NOT NULL,
    operation_type TEXT NOT NULL,
    before         TEXT NOT NULL,
    after          TEXT,
    status         TEXT NOT NULL DEFAULT 'pending',
    created_at     DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id          TEXT PRIMARY KEY,
    job_id      TEXT,
    folder_id   TEXT,
    folder_path TEXT NOT NULL,
    action      TEXT NOT NULL,
    level       TEXT NOT NULL DEFAULT 'info',
    detail      TEXT,
    result      TEXT NOT NULL,
    error       TEXT,
    duration_ms INTEGER,
    created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS config (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- 002_indexes.sql
CREATE INDEX IF NOT EXISTS idx_folders_status   ON folders(status);
CREATE INDEX IF NOT EXISTS idx_folders_category ON folders(category);
CREATE INDEX IF NOT EXISTS idx_snapshots_job    ON snapshots(job_id);
CREATE INDEX IF NOT EXISTS idx_snapshots_folder ON snapshots(folder_id);
CREATE INDEX IF NOT EXISTS idx_audit_folder     ON audit_logs(folder_id);
CREATE INDEX IF NOT EXISTS idx_audit_action     ON audit_logs(action);
CREATE INDEX IF NOT EXISTS idx_audit_created    ON audit_logs(created_at);
```

## API Surface (Phase 1)

```
GET    /health

GET    /api/folders                    # list with filter: ?status=&category=&q=
POST   /api/folders/scan               # trigger scan → 202 + SSE events
GET    /api/folders/:id
PATCH  /api/folders/:id/category       # {"category": "photo"}
PATCH  /api/folders/:id/status         # {"status": "done"}
DELETE /api/folders/:id

POST   /api/jobs/move                  # {"folder_ids": [...], "target_dir": "..."} → 202

GET    /api/snapshots?folder_id=
POST   /api/snapshots/:id/revert

GET    /api/config
PUT    /api/config                     # {"source_dir": "...", "target_dir": "..."}

GET    /api/events                     # SSE stream
```

## SSE Event Types

```json
{"type": "scan.progress", "data": {"scanned": 42, "total": 100}}
{"type": "scan.done",     "data": {"total": 100}}
{"type": "scan.error",    "data": {"message": "..."}}
{"type": "job.progress",  "data": {"operation_id": "...", "done": 3, "total": 10}}
{"type": "job.done",      "data": {"operation_id": "..."}}
{"type": "job.error",     "data": {"operation_id": "...", "folder_id": "...", "message": "..."}}
```

## Domain Types (Go)

```go
type Category string
const (
    CategoryPhoto  Category = "photo"
    CategoryVideo  Category = "video"
    CategoryMixed  Category = "mixed"
    CategoryManga  Category = "manga"
    CategoryOther  Category = "other"
)

type Status string
const (
    StatusPending Status = "pending"
    StatusDone    Status = "done"
    StatusSkip    Status = "skip"
)

type Folder struct {
    ID             string    `json:"id"`
    Path           string    `json:"path"`
    Name           string    `json:"name"`
    Category       Category  `json:"category"`
    CategorySource string    `json:"category_source"` // "auto" | "manual"
    Status         Status    `json:"status"`
    ImageCount     int       `json:"image_count"`
    VideoCount     int       `json:"video_count"`
    TotalFiles     int       `json:"total_files"`
    TotalSize      int64     `json:"total_size"`
    MarkedForMove  bool      `json:"marked_for_move"`
    ScannedAt      time.Time `json:"scanned_at"`
    UpdatedAt      time.Time `json:"updated_at"`
}
```

## Classifier Logic

```
Step 1 — Manga detection:
  If folder name contains any of: ["漫画","comic","manga","scan","Comics"]
  → category = manga, done

Step 2 — Extension ratio:
  Count image files (jpg,jpeg,png,gif,webp,avif,jxl,bmp,tiff)
  Count video files (mp4,mkv,avi,mov,wmv,flv,ts,m2ts,rmvb,webm,m4v,iso)
  imageRatio = imageCount / totalFiles
  videoRatio = videoCount / totalFiles

  if imageRatio >= 0.8 → photo
  if videoRatio >= 0.8 → video
  if imageRatio > 0 && videoRatio > 0 → mixed
  else → other
```

## Docker Configuration

```dockerfile
# Dockerfile (multi-stage)
FROM node:20-alpine AS frontend-builder
WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM golang:1.23-alpine AS backend-builder
WORKDIR /app
COPY backend/ ./backend/
COPY --from=frontend-builder /app/frontend/dist ./backend/web/dist
WORKDIR /app/backend
RUN CGO_ENABLED=0 GOOS=linux go build -o /classifier ./cmd/server

FROM alpine:3.19
RUN apk add --no-cache ffmpeg tzdata ca-certificates
COPY --from=backend-builder /classifier /usr/local/bin/classifier
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/classifier"]
```

```yaml
# docker-compose.yml
services:
  classifier:
    build: .
    image: classifier:latest
    container_name: classifier
    user: "0:0"   # root for 极空间 NAS; change to "1000:1000" if NAS supports it
    ports:
      - "${PORT:-8080}:8080"
    volumes:
      - ${SOURCE_DIR}:/data/source:ro
      - ${TARGET_DIR}:/data/target
      - ${CONFIG_DIR:-./data}:/data/config
    environment:
      - SOURCE_DIR=/data/source
      - TARGET_DIR=/data/target
      - CONFIG_DIR=/data/config
      - TZ=${TZ:-Asia/Shanghai}
    cap_add:
      - CHOWN
      - DAC_OVERRIDE
      - FOWNER
      - SETGID
      - SETUID
    restart: unless-stopped
```

## Acceptance Criteria

- [ ] `CGO_ENABLED=0 go build ./cmd/server` exits 0
- [ ] `go test ./... -race` passes
- [ ] `go vet ./...` passes
- [ ] `npm run build` exits 0
- [ ] `npm run typecheck` exits 0 (no `any`, no `@ts-ignore`)
- [ ] `npm run lint` exits 0
- [ ] `docker compose build` exits 0
- [ ] GET /health returns 200 `{"status":"ok"}`
- [ ] POST /api/folders/scan triggers scan + SSE progress events
- [ ] Classified folders visible in FolderListPage with correct categories
- [ ] PATCH /api/folders/:id/category updates category (manual override)
- [ ] POST /api/jobs/move moves folders + creates snapshots
- [ ] POST /api/snapshots/:id/revert reverts move
- [ ] GET /api/config + PUT /api/config work
- [ ] SnapshotDrawer shows history and revert button
