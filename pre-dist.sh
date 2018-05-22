#!/bin/bash

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
rm -rf   $DIR/dist/docker
rm -rf   $DIR/.godeps
mkdir -p $DIR/dist

arch=$(go env GOARCH)
dep ensure
