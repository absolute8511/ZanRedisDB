#!/bin/bash
set -e
echo "" > coverage.txt
echo $TEST_RACE

if [ "$TEST_RACE" = "false" ]; then
    GOMAXPROCS=1 go test -tags=embed -timeout 900s `go list ./... | grep -v pdserver`
else
    GOMAXPROCS=4 go test -tags=embed -timeout 900s -race `go list ./... | grep -v pdserver`
    for d in $(go list ./... | grep -v pdserver | grep -v vendor); do 
        GOMAXPROCS=4 go test -tags=embed -timeout 900s -race -coverprofile=profile.out -covermode=atomic $d
        if [ -f profile.out ]; then
            cat profile.out >> coverage.txt
            rm profile.out
        fi
    done
fi

# no tests, but a build is something
for dir in $(find apps tools -maxdepth 1 -type d) ; do
    if grep -q '^package main$' $dir/*.go 2>/dev/null; then
        echo "building $dir"
        go build -tags=embed -o $dir/$(basename $dir) ./$dir
    else
        echo "(skipped $dir)"
    fi
done
