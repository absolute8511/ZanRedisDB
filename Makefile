PREFIX=/usr/local
DESTDIR=
BINDIR=${PREFIX}/bin
PROJECT?=github.com/youzan/ZanRedisDB
VERBINARY?= 0.7.1
COMMIT?=$(shell git rev-parse --short HEAD)
BUILD_TIME?=$(shell date '+%Y-%m-%d_%H:%M:%S-%Z')
GOFLAGS=-ldflags "-X ${PROJECT}/common.VerBinary=${VERBINARY} -X ${PROJECT}/common.Commit=${COMMIT} -X ${PROJECT}/common.BuildTime=${BUILD_TIME}"

CGO_CFLAGS="-I${ROCKSDB}/include"
CGO_LDFLAGS="-L${ROCKSDB} -lrocksdb -lstdc++ -lm -lsnappy -ljemalloc"

ifeq (${GOOS},linux)
	CGO_LDFLAGS="-L${ROCKSDB} -lrocksdb -lstdc++ -lm -lsnappy -lrt -ljemalloc"
endif

BLDDIR = build
EXT=
ifeq (${GOOS},windows)
    EXT=.exe
endif

APPS = placedriver zankv backup restore
all: $(APPS)

$(BLDDIR)/placedriver:        $(wildcard apps/placedriver/*.go  pdserver/*.go common/*.go cluster/*/*.go)
$(BLDDIR)/zankv:  $(wildcard apps/zankv/*.go wal/*.go transport/*/*.go stats/*.go snap/*/*.go server/*.go rockredis/*.go raft/*/*.go node/*.go common/*.go cluster/*/*.go)
$(BLDDIR)/backup:  $(wildcard apps/backup/*.go)
$(BLDDIR)/restore:  $(wildcard apps/restore/*.go)

$(BLDDIR)/%:
	@mkdir -p $(dir $@)
	@echo $(GOOS)
	@echo $(CGO_LDFLAGS)
	CGO_CFLAGS=${CGO_CFLAGS} CGO_LDFLAGS=${CGO_LDFLAGS} GO111MODULE=on go get -u github.com/youzan/gorocksdb
	CGO_CFLAGS=${CGO_CFLAGS} CGO_LDFLAGS=${CGO_LDFLAGS} GO111MODULE=on go build ${GOFLAGS} -o $@ ./apps/$*

$(APPS): %: $(BLDDIR)/%

clean:
	rm -fr $(BLDDIR)

.PHONY: install clean all
.PHONY: $(APPS)

install: $(APPS)
	install -m 755 -d ${DESTDIR}${BINDIR}
	for APP in $^ ; do install -m 755 ${BLDDIR}/$$APP ${DESTDIR}${BINDIR}/$$APP${EXT} ; done
