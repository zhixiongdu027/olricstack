.PHONY: test e2e e2e-host e2e-k3d build verify

test:
	go test ./...

e2e:
	go test ./internal/e2e -count=1 -v

e2e-host:
	go test -tags=e2e ./internal/e2e -run 'TestHostOnly' -count=1 -v

e2e-k3d:
	sh scripts/e2e-k3d.sh

build:
	go build ./cmd/olric-node ./cmd/watchdog ./cmd/operator ./cmd/olric-e2e-client

verify: test build
