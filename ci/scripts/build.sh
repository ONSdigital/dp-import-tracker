#!/bin/bash -eux

cwd=$(pwd)

export GOPATH=$cwd/go

pushd $GOPATH/src/github.com/ONSdigital/dp-import-tracker
  make build && mv build/$(go env GOOS)-$(go env GOARCH)/bin/* $cwd/build
popd
