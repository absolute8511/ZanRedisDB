#!/bin/bash

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
rm -rf   $DIR/dist/docker
rm -rf   $DIR/.godeps
mkdir -p $DIR/.godeps
mkdir -p $DIR/dist
export GOPATH=$DIR/.godeps:$GOPATH
GOPATH=$DIR/.godeps gpm get

arch=$(go env GOARCH)

#go test -tags=embed -race ./...
