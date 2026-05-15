#!/usr/bin/env sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
CLUSTER="${E2E_K3D_CLUSTER:-dev}"
TAG="${E2E_IMAGE_TAG:-e2e-$(date +%s)}"
NODE_IMAGE="${E2E_NODE_IMAGE:-olricstack/olric-node:$TAG}"
WATCHDOG_IMAGE="${E2E_WATCHDOG_IMAGE:-olricstack/watchdog:$TAG}"
OPERATOR_IMAGE="${E2E_OPERATOR_IMAGE:-olricstack/operator:$TAG}"
BUILD_DIR="$ROOT_DIR/.build/e2e-k3d"

build_image() {
  image="$1"
  binary="$2"
  context="$BUILD_DIR/$binary-image"
  rm -rf "$context"
  mkdir -p "$context"
  cp "$BUILD_DIR/bin/$binary" "$context/$binary"
  cat > "$context/Dockerfile" <<EOF
FROM scratch
COPY $binary /$binary
ENTRYPOINT ["/$binary"]
EOF
  docker build -t "$image" "$context"
}

echo "building e2e binaries"
mkdir -p "$BUILD_DIR/bin"
(
  cd "$ROOT_DIR"
  CGO_ENABLED=0 GOOS=linux go build -o "$BUILD_DIR/bin/olric-node" ./cmd/olric-node
  CGO_ENABLED=0 GOOS=linux go build -o "$BUILD_DIR/bin/watchdog" ./cmd/watchdog
  CGO_ENABLED=0 GOOS=linux go build -o "$BUILD_DIR/bin/operator" ./cmd/operator
)

echo "building e2e images"
build_image "$NODE_IMAGE" olric-node
build_image "$WATCHDOG_IMAGE" watchdog
build_image "$OPERATOR_IMAGE" operator

echo "importing images into k3d cluster $CLUSTER"
k3d image import -c "$CLUSTER" "$NODE_IMAGE" "$WATCHDOG_IMAGE" "$OPERATOR_IMAGE"

echo "running k3d e2e tests"
cd "$ROOT_DIR"
E2E_K3D_CLUSTER="$CLUSTER" \
E2E_NODE_IMAGE="$NODE_IMAGE" \
E2E_WATCHDOG_IMAGE="$WATCHDOG_IMAGE" \
E2E_OPERATOR_IMAGE="$OPERATOR_IMAGE" \
go test -tags=e2e ./internal/e2e -count=1 -v
