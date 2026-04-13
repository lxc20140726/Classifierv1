# Docker 部署配置

> 版本：v2.0 | 日期：2026-03-19

## Dockerfile

```dockerfile
# syntax=docker/dockerfile:1

# Stage 1: 前端构建
FROM node:22-alpine AS frontend-build
WORKDIR /web
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ .
RUN npm run build

# Stage 2: 后端构建
FROM golang:1.23-alpine AS backend-build
WORKDIR /src
COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/ .
COPY --from=frontend-build /web/dist ./web/dist
RUN CGO_ENABLED=0 GOOS=linux go build \
    -ldflags="-s -w" \
    -o /out/classifier \
    ./cmd/server

# Stage 3: 运行时
FROM alpine:3.20
RUN apk add --no-cache ffmpeg ca-certificates tzdata
WORKDIR /app
COPY --from=backend-build /out/classifier /app/classifier
VOLUME ["/config"]
EXPOSE 8080
ENTRYPOINT ["/app/classifier"]
```

## docker-compose.yml

```yaml
services:
  classifier:
    build:
      context: .
      dockerfile: Dockerfile
    image: classifier:latest
    container_name: classifier
    restart: unless-stopped
    ports:
      - "${APP_PORT:-8080}:8080"
    environment:
      CONFIG_DIR: /config
      SOURCE_DIR: /data/source
      TARGET_DIR: /data/target
      MAX_CONCURRENCY: ${MAX_CONCURRENCY:-3}
      TZ: ${TZ:-Asia/Shanghai}
    volumes:
      - type: bind
        source: ${SOURCE_DIR:?SOURCE_DIR is required}
        target: /data/source
        read_only: true
        bind:
          create_host_path: false
      - type: bind
        source: ${TARGET_DIR:?TARGET_DIR is required}
        target: /data/target
        bind:
          create_host_path: false
      - type: volume
        source: classifier-config
        target: /config
        volume:
          nocopy: true
      - type: tmpfs
        target: /tmp/work
        tmpfs:
          size: 1073741824
    deploy:
      resources:
        limits:
          cpus: "${CPU_LIMIT:-2.0}"
          memory: ${MEMORY_LIMIT:-1G}
        reservations:
          cpus: "0.25"
          memory: 256M

volumes:
  classifier-config:
```

## .env.example

```env
APP_PORT=8080
SOURCE_DIR=/volume1/media/incoming
TARGET_DIR=/volume1/media/processed
MAX_CONCURRENCY=3
TZ=Asia/Shanghai
CPU_LIMIT=2.0
MEMORY_LIMIT=1G
```

## NAS 快速启动

```bash
# 1. 克隆仓库
git clone https://github.com/lxc20140726/Classifier.git
cd Classifier

# 2. 复制并编辑环境变量
cp .env.example .env
vim .env  # 填写 SOURCE_DIR 和 TARGET_DIR

# 3. 启动
docker compose up -d

# 4. 访问
open http://<NAS_IP>:8080
```

## Volume 说明

| 挂载 | 宿主机路径 | 容器路径 | 模式 |
|------|-----------|---------|------|
| 源目录 | $SOURCE_DIR | /data/source | 只读 |
| 目标目录 | $TARGET_DIR | /data/target | 读写 |
| 配置/DB | named volume | /config | 读写 |
| FFmpeg 临时 | tmpfs (1GB) | /tmp/work | 内存 |
