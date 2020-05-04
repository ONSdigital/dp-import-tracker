#!/bin/bash -eux

cwd=$(pwd)

pushd $cwd/dp-import-tracker
  make test
popd
