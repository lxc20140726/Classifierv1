#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
LOCAL="$ROOT/.local"

mkdir -p "$LOCAL/source" "$LOCAL/target" "$LOCAL/config" "$LOCAL/delete-staging"

kill_port() {
  lsof -nP -iTCP:"$1" -sTCP:LISTEN 2>/dev/null | awk 'NR>1{print $2}' | xargs kill 2>/dev/null || true
}

cleanup() {
  echo ""
  echo "正在停止服务..."
  kill "${BACKEND_PID:-}" 2>/dev/null || true
  kill "${FRONTEND_PID:-}" 2>/dev/null || true
  exit 0
}
trap cleanup INT TERM

kill_port 8080
kill_port 5173

echo "启动后端 (http://localhost:8080) ..."
(
  cd "$ROOT/backend"
  CONFIG_DIR="$LOCAL/config" \
  SOURCE_DIR="$LOCAL/source" \
  TARGET_DIR="$LOCAL/target" \
  DELETE_STAGING_DIR="$LOCAL/delete-staging" \
  PORT=8080 \
  CGO_ENABLED=0 \
    go run ./cmd/server
) > "$LOCAL/backend.log" 2>&1 &
BACKEND_PID=$!

for i in $(seq 1 20); do
  if curl -sf http://localhost:8080/health > /dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

echo "启动前端 (http://localhost:5173) ..."
cd "$ROOT/frontend" && npm run dev > "$LOCAL/frontend.log" 2>&1 &
FRONTEND_PID=$!
cd "$ROOT"

echo ""
echo "前端: http://localhost:5173"
echo "后端: http://localhost:8080"
echo "日志: $LOCAL/backend.log  $LOCAL/frontend.log"
echo "Ctrl+C 停止所有服务"
echo ""

wait
