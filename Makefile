.PHONY: test test-third-party e2e e2e-host e2e-kind fmt-check vet test-race test-third-party-race coverage build verify

test:
	go test -count=1 ./...

test-third-party:
	cd third_party/olric && go test -p 1 -count=1 ./...

e2e:
	go test ./internal/e2e -count=1 -v

e2e-host:
	go test -tags=e2e ./internal/e2e -run '^TestHostOnly' -count=1 -v

e2e-kind:
	sh scripts/e2e-kind.sh

fmt-check:
	test -z "$$(git ls-files '*.go' | while read -r file; do test -f "$$file" && printf '%s\n' "$$file"; done | xargs -r gofmt -l)"

vet:
	go vet ./...

test-race:
	go test -count=1 -race ./cmd/... ./internal/... ./api/...

test-third-party-race:
	cd third_party/olric && go test -p 1 -count=1 -race ./...

coverage:
	mkdir -p .build
	go test -coverprofile=.build/coverage.out ./cmd/... ./internal/... ./api/...

build:
	go build ./cmd/olric-node ./cmd/olric-sidecar ./cmd/watchdog ./cmd/operator ./cmd/olric-e2e-client

verify: vet test build
