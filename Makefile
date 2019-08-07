MAIN=dp-import-tracker
SHELL=bash

BUILD=build
BUILD_ARCH=$(BUILD)/$(GOOS)-$(GOARCH)
BIN_DIR?=bin

export GOOS?=$(shell go env GOOS)
export GOARCH?=$(shell go env GOARCH)

export GRAPH_DRIVER_TYPE?="neptune"
export GRAPH_ADDR?="ws://localhost:8182/gremlin"

depends:
	-@govendor list +outside | grep -v '^  m help *$$'
	@[[ -z "$(shell govendor list +outside | grep -v '^  m help *$$')" ]]

build:
	@mkdir -p $(BUILD_ARCH)/$(BIN_DIR)
	go build -o $(BUILD_ARCH)/$(BIN_DIR)/$(MAIN) cmd/$(MAIN)/main.go
debug:
	HUMAN_LOG=1 go run -race cmd/$(MAIN)/main.go

test:
	go test -cover $(shell go list ./... | grep -v /vendor/)

.PHONEY: test build debug depends
