#!/bin/bash

# 1. commit to bump the version and update the changelog/readme
# 2. tag that commit
# 3. use dist.sh to produce tar.gz for linux and darwin
# 7. update the release metadata on github / upload the binaries there too
# 8. update the gh-pages branch with versions / download links
# 9. update homebrew version
# 10. send release announcement emails

set -e

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# this is used by CI which can not use dep ensure
export GOPATH=$(go env GOPATH):$DIR/.godeps
echo $GOPATH

arch=$(go env GOARCH)
os=$(go env GOOS)
version=$(awk '/VERBINARY\?/ {print $NF}' < $DIR/Makefile | sed 's/"//g')
goversion=$(go version | awk '{print $3}')

echo "... building v$version for $os/$arch"
BUILD=$(mktemp -d -t zankvXXXXXX)
TARGET="zankv-$version.$os-$arch.$goversion"
LATEST="zankv-latest.$os-$arch.$goversion"
GOOS=$os GOARCH=$arch \
    make DESTDIR=$BUILD PREFIX=/$TARGET install
pushd $BUILD
tar czvf $TARGET.tar.gz $TARGET
mv $TARGET.tar.gz $DIR/dist/
mv $TARGET $LATEST
tar czvf $LATEST.tar.gz $LATEST
mv $LATEST.tar.gz $DIR/dist/
rm -rf $LATEST
popd
make clean
rm -r $BUILD
