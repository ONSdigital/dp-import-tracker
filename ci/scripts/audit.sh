#!/bin/bash -eux

export cwd=$(pwd)

pushd $cwd/dp-import-tracker
  make audit
popd 