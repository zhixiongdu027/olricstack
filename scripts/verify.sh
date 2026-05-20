#!/usr/bin/env sh
set -eu

go test ./...
go build ./cmd/olric-node ./cmd/olric-sidecar ./cmd/watchdog ./cmd/operator
