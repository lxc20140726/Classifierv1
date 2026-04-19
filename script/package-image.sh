#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)

IMAGE_NAME="${CLASSIFIER_IMAGE:-classifier}"
IMAGE_TAG="${CLASSIFIER_TAG:-local}"
PLATFORM="${DOCKER_PLATFORM:-linux/amd64}"
OUTPUT_DIR="${OUTPUT_DIR:-output}"
SAFE_IMAGE_NAME=$(printf '%s' "$IMAGE_NAME" | sed 's#[/:]#-#g')
SAFE_PLATFORM=$(printf '%s' "$PLATFORM" | sed 's#[/:]#-#g')
TAR_NAME="${SAFE_IMAGE_NAME}-${IMAGE_TAG}-${SAFE_PLATFORM}.tar"
TAR_PATH="${REPO_ROOT}/${OUTPUT_DIR}/${TAR_NAME}"

cd "$REPO_ROOT"
mkdir -p "$OUTPUT_DIR"

docker buildx build --platform "$PLATFORM" --tag "${IMAGE_NAME}:${IMAGE_TAG}" --load .
docker save "${IMAGE_NAME}:${IMAGE_TAG}" -o "$TAR_PATH"

printf '镜像已打包: %s\n' "$TAR_PATH"
printf '部署时请在极空间执行: docker load -i %s\n' "$TAR_NAME"
