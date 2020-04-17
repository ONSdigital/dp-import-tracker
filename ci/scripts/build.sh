#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-import-tracker
  make build && mv build/$(go env GOOS)-$(go env GOARCH)/bin/* $cwd/build
  cp Dockerfile.concourse $cwd/build
popd
