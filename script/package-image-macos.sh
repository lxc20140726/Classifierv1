#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

IMAGE_NAME="${CLASSIFIER_IMAGE:-classifier}"
IMAGE_TAG="${CLASSIFIER_TAG:-local}"
PLATFORM="${DOCKER_PLATFORM:-linux/amd64}"
OUTPUT_DIR="${OUTPUT_DIR:-output}"
COMPRESS="${COMPRESS:-0}"

SAFE_IMAGE_NAME="$(printf '%s' "$IMAGE_NAME" | sed 's#[/:]#-#g')"
SAFE_PLATFORM="$(printf '%s' "$PLATFORM" | sed 's#[/:]#-#g')"
TAR_NAME="${SAFE_IMAGE_NAME}-${IMAGE_TAG}-${SAFE_PLATFORM}.tar"
TAR_PATH="${REPO_ROOT}/${OUTPUT_DIR}/${TAR_NAME}"

if [ "$(uname -s)" != "Darwin" ]; then
  printf '当前脚本仅面向 macOS，请改用通用打包脚本或在 macOS 上执行。\n' >&2
  exit 1
fi

if ! command -v docker >/dev/null 2>&1; then
  printf '未找到 docker 命令，请先安装并启动 Docker Desktop for Mac。\n' >&2
  exit 1
fi

if ! docker info >/dev/null 2>&1; then
  printf 'Docker daemon 未运行，请先启动 Docker Desktop。\n' >&2
  exit 1
fi

if ! docker buildx version >/dev/null 2>&1; then
  printf '当前 Docker 缺少 buildx，请升级 Docker Desktop。\n' >&2
  exit 1
fi

if [ ! -f "$REPO_ROOT/Dockerfile" ]; then
  printf '未找到 Dockerfile: %s\n' "$REPO_ROOT/Dockerfile" >&2
  exit 1
fi

mkdir -p "$REPO_ROOT/$OUTPUT_DIR"

printf '开始构建镜像: %s:%s\n' "$IMAGE_NAME" "$IMAGE_TAG"
printf '目标平台: %s\n' "$PLATFORM"

docker buildx inspect >/dev/null 2>&1 || docker buildx create --use >/dev/null
docker buildx inspect --bootstrap >/dev/null

docker buildx build \
  --platform "$PLATFORM" \
  --tag "${IMAGE_NAME}:${IMAGE_TAG}" \
  --load \
  "$REPO_ROOT"

printf '开始导出镜像: %s\n' "$TAR_PATH"
docker save "${IMAGE_NAME}:${IMAGE_TAG}" -o "$TAR_PATH"

if [ "$COMPRESS" = "1" ]; then
  printf '开始压缩镜像包: %s.gz\n' "$TAR_PATH"
  gzip -f "$TAR_PATH"
  TAR_PATH="${TAR_PATH}.gz"
  TAR_NAME="${TAR_NAME}.gz"
fi

printf '\n镜像已打包: %s\n' "$TAR_PATH"
printf '部署机器导入命令: docker load -i %s\n' "$TAR_NAME"
