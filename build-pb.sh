#!/usr/bin/env bash
set -e
GOGOROOT="${GOPATH}/src/github.com/gogo/protobuf"
GOGOPATH="${GOGOROOT}:${GOGOROOT}/protobuf"
DIRS="./node ./raft/raftpb ./snap/snappb ./wal/walpb ./rockredis ./syncerpb"
echo $GOROOT
echo $GOPATH
for dir in ${DIRS}; do
    pushd ${dir}
        protoc --proto_path=$GOPATH:$GOGOPATH:./ --gofast_out=plugins=grpc:. *.proto
    popd
done
