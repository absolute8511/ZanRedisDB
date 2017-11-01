PREFIX=/usr/local
DESTDIR=
GOFLAGS=
BINDIR=${PREFIX}/bin

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
	go build -i ${GOFLAGS} -o $@ ./apps/$*

$(APPS): %: $(BLDDIR)/%

clean:
	rm -fr $(BLDDIR)

.PHONY: install clean all
.PHONY: $(APPS)

install: $(APPS)
	install -m 755 -d ${DESTDIR}${BINDIR}
	for APP in $^ ; do install -m 755 ${BLDDIR}/$$APP ${DESTDIR}${BINDIR}/$$APP${EXT} ; done
