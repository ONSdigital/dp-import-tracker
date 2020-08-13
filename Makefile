MAIN=dp-import-tracker
SHELL=bash

BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
BIN_DIR?=bin

export GOOS?=$(shell go env GOOS)
export GOARCH?=$(shell go env GOARCH)

BUILD_TIME=$(shell date +%s)
GIT_COMMIT=$(shell git rev-parse HEAD)
VERSION ?= $(shell git tag --points-at HEAD | grep ^v | head -n 1)
LDFLAGS=-ldflags "-w -s -X 'main.Version=${VERSION}' -X 'main.BuildTime=$(BUILD_TIME)' -X 'main.GitCommit=$(GIT_COMMIT)'"

PHONY: all
all: audit test build

PHONY: audit
audit:
	nancy go.sum

PHONY: depends
depends:
	-@govendor list +outside | grep -v '^  m help *$$'
	@[[ -z "$(shell govendor list +outside | grep -v '^  m help *$$')" ]]

PHONY: build
build:
	@mkdir -p $(BUILD_ARCH)/$(BIN_DIR)
	go build $(LDFLAGS) -o $(BUILD_ARCH)/$(BIN_DIR)/$(MAIN) cmd/$(MAIN)/main.go

PHONY: debug
debug:
	GRAPH_DRIVER_TYPE="neo4j" GRAPH_ADDR="bolt://localhost:7687" HUMAN_LOG=1 go run $(LDFLAGS) -race cmd/$(MAIN)/main.go

PHONY: test
test:
	go test -cover -race ./...

.PHONEY: test build debug depends
