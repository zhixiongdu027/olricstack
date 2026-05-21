#!/usr/bin/env sh
set -eu

ROOT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
KIND="${KIND:-kind}"
CLUSTER="${E2E_KIND_CLUSTER:-olric-e2e}"
TAG="${E2E_IMAGE_TAG:-e2e-$(date +%s)}"
NODE_IMAGE="${E2E_NODE_IMAGE:-olricstack/olric-node:$TAG}"
SIDECAR_IMAGE="${E2E_SIDECAR_IMAGE:-olricstack/olric-sidecar:$TAG}"
WATCHDOG_IMAGE="${E2E_WATCHDOG_IMAGE:-olricstack/watchdog:$TAG}"
OPERATOR_IMAGE="${E2E_OPERATOR_IMAGE:-olricstack/operator:$TAG}"
CLIENT_IMAGE="${E2E_CLIENT_IMAGE:-olricstack/olric-e2e-client:$TAG}"
TEST_RUN="${E2E_TEST_RUN:-TestKind}"
BUILD_DIR="$ROOT_DIR/.build/e2e-kind"

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

if ! "$KIND" get clusters | grep -Fxq "$CLUSTER"; then
  if [ "${E2E_KIND_CREATE_CLUSTER:-0}" != "1" ]; then
    echo "kind cluster $CLUSTER not found; set E2E_KIND_CREATE_CLUSTER=1 to create it" >&2
    exit 1
  fi
  "$KIND" create cluster --name "$CLUSTER"
fi

echo "building e2e binaries"
mkdir -p "$BUILD_DIR/bin"
(
  cd "$ROOT_DIR"
  CGO_ENABLED=0 GOOS=linux go build -o "$BUILD_DIR/bin/olric-node" ./cmd/olric-node
  CGO_ENABLED=0 GOOS=linux go build -o "$BUILD_DIR/bin/olric-sidecar" ./cmd/olric-sidecar
  CGO_ENABLED=0 GOOS=linux go build -o "$BUILD_DIR/bin/watchdog" ./cmd/watchdog
  CGO_ENABLED=0 GOOS=linux go build -o "$BUILD_DIR/bin/operator" ./cmd/operator
  CGO_ENABLED=0 GOOS=linux go build -o "$BUILD_DIR/bin/olric-e2e-client" ./cmd/olric-e2e-client
)

echo "building e2e images"
build_image "$NODE_IMAGE" olric-node
build_image "$SIDECAR_IMAGE" olric-sidecar
build_image "$WATCHDOG_IMAGE" watchdog
build_image "$OPERATOR_IMAGE" operator

echo "building e2e client image"
CLIENT_CONTEXT="$BUILD_DIR/olric-e2e-client-image"
rm -rf "$CLIENT_CONTEXT"
mkdir -p "$CLIENT_CONTEXT"
cp "$BUILD_DIR/bin/olric-e2e-client" "$CLIENT_CONTEXT/olric-e2e-client"
cat > "$CLIENT_CONTEXT/Dockerfile" <<EOF
FROM scratch
COPY olric-e2e-client /olric-e2e-client
ENTRYPOINT ["/olric-e2e-client"]
EOF
docker build -t "$CLIENT_IMAGE" "$CLIENT_CONTEXT"

echo "loading images into kind cluster $CLUSTER"
"$KIND" load docker-image --name "$CLUSTER" "$NODE_IMAGE"
"$KIND" load docker-image --name "$CLUSTER" "$SIDECAR_IMAGE"
"$KIND" load docker-image --name "$CLUSTER" "$WATCHDOG_IMAGE"
"$KIND" load docker-image --name "$CLUSTER" "$OPERATOR_IMAGE"
"$KIND" load docker-image --name "$CLUSTER" "$CLIENT_IMAGE"

if [ -z "${E2E_MYSQL_DSN:-}" ] && [ -n "${E2E_MYSQL_PORT:-}" ]; then
  gateway="$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}' "$CLUSTER-control-plane")"
  if [ -z "$gateway" ]; then
    echo "unable to detect Docker gateway for kind cluster $CLUSTER" >&2
    exit 1
  fi
  E2E_MYSQL_DSN="${E2E_MYSQL_USER:-root}:${E2E_MYSQL_PASSWORD:-password}@tcp($gateway:$E2E_MYSQL_PORT)/${E2E_MYSQL_DATABASE:-olric_e2e}?parseTime=true"
  export E2E_MYSQL_DSN
fi

echo "running kind e2e tests"
cd "$ROOT_DIR"
E2E_KIND_CLUSTER="$CLUSTER" \
E2E_NODE_IMAGE="$NODE_IMAGE" \
E2E_SIDECAR_IMAGE="$SIDECAR_IMAGE" \
E2E_WATCHDOG_IMAGE="$WATCHDOG_IMAGE" \
E2E_OPERATOR_IMAGE="$OPERATOR_IMAGE" \
E2E_CLIENT_IMAGE="$CLIENT_IMAGE" \
go test -tags=e2e ./internal/e2e -run "$TEST_RUN" -count=1 -v
