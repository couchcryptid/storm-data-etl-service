.PHONY: build run test test-unit test-integration test-smoke test-cover lint fmt vuln clean

build:
	go build -o bin/etl ./cmd/etl

run:
	go run ./cmd/etl

test: test-unit test-integration

test-unit:
	go test ./... -v -race -count=1

test-integration:
	go test ./internal/integration -v -race -count=1 -tags=integration

test-smoke:
	go test ./internal/adapter/mapbox -v -count=1 -tags=mapbox

test-cover:
	go test ./... -coverprofile=coverage.out
	go tool cover -html=coverage.out

lint:
	golangci-lint run ./...

fmt:
	gofmt -w .
	goimports -w .

vuln:
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

clean:
	rm -rf bin/ coverage.out
