#!/bin/bash
set -euo pipefail

# === Конфигурация ===
REGISTRY="sanich.space:5002"
REPO_PREFIX="test"  # Префикс для уникальности
TAGS=("v1" "v2" "latest" "dev" "prod" "test" "rc1")

# Образы для загрузки и ретега
IMAGES=(
  "hello-world"
  "alpine"
  "nginx"
  "httpd"
  "busybox"
  "docker.io/library/ubi8/ubi" # Red Hat UBI
  "python:3.12-alpine"
  "node:20-alpine"
)

# === Загрузка, ретег и пуш ===
for IMAGE in "${IMAGES[@]}"; do
  BASE_NAME=$(basename "$IMAGE" | tr ':/' '_')  # Имя для локального тега

  for TAG in "${TAGS[@]}"; do
    LOCAL_TAG="${REGISTRY}/${REPO_PREFIX}-${BASE_NAME}:${TAG}"

    echo "📦 Pulling $IMAGE"
    docker pull "$IMAGE"

    echo "🔁 Retag → $LOCAL_TAG"
    docker tag "$IMAGE" "$LOCAL_TAG"

    echo "🚀 Pushing $LOCAL_TAG"
    docker push "$LOCAL_TAG"

    echo "✅ Done: $LOCAL_TAG"
  done
done

echo "🎉 Все образы загружены и отправлены в $REGISTRY"
