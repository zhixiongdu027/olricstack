.PHONY: test e2e build verify

test:
	go test ./...

e2e:
	go test ./internal/e2e -count=1 -v

build:
	go build ./cmd/olric-node ./cmd/watchdog ./cmd/operator

verify: test build
