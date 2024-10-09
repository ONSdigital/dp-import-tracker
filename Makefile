MAIN=dp-import-tracker
SHELL=bash

BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
BIN_DIR?=bin

export GOOS?=$(shell go env GOOS)
export GOARCH?=$(shell go env GOARCH)

export GRAPH_DRIVER_TYPE?=neo4j
export GRAPH_ADDR?=bolt://localhost:7687

BUILD_TIME=$(shell date +%s)
GIT_COMMIT=$(shell git rev-parse HEAD)
VERSION ?= $(shell git tag --points-at HEAD | grep ^v | head -n 1)
LDFLAGS=-ldflags "-w -s -X 'main.Version=${VERSION}' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.GitCommit=$(GIT_COMMIT)'"

all: audit test build

audit:
	set -o pipefail; go list -json -m all | nancy sleuth

depends:
	-@govendor list +outside | grep -v '^  m help *$$'
	@[[ -z "$(shell govendor list +outside | grep -v '^  m help *$$')" ]]

build:
	@mkdir -p $(BUILD_ARCH)/$(BIN_DIR)
	go build $(LDFLAGS) -o $(BUILD_ARCH)/$(BIN_DIR)/$(MAIN) cmd/$(MAIN)/main.go

debug:
	HUMAN_LOG=1 go run $(LDFLAGS) -race cmd/$(MAIN)/main.go

test:
	go test -cover -race ./...

lint:
	golangci-lint run ./...

.PHONY: all audit test build debug depends lint
