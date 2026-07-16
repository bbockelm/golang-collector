# Makefile for golang-collector.
#
# Builds the two daemons as `htc-collector` and `htc-negotiator` (the names a
# pool uses for the Go collector/negotiator while they are being rolled out
# alongside the C++ condor_collector / condor_negotiator).

GO      ?= go
BINDIR  ?= build
PREFIX  ?= /usr/local
DESTDIR ?=

# Version stamped into the binaries (-version flag) and used by the release
# workflow. `git describe` gives e.g. v0.2.0, v0.2.0-3-gabc123, or v0.2.0-dirty.
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -X main.version=$(VERSION)

GOFLAGS ?=

BINARIES := htc-collector htc-negotiator

.PHONY: all build htc-collector htc-negotiator test test-short vet lint tidy clean install version

all: build

build: htc-collector htc-negotiator

htc-collector:
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BINDIR)/htc-collector ./cmd/golang-collector

htc-negotiator:
	$(GO) build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BINDIR)/htc-negotiator ./cmd/golang-negotiator

# `test` runs the full suite (integration tests skip when the HTCondor binaries
# are absent); `test-short` skips the heavy footprint/scaling tests.
test:
	$(GO) test -race -timeout=30m ./...

test-short:
	$(GO) test -short -race -timeout=30m ./...

vet:
	$(GO) vet ./...

lint:
	golangci-lint run

tidy:
	$(GO) mod tidy

clean:
	rm -rf $(BINDIR)

version:
	@echo $(VERSION)

# HTCondor daemons live in sbin; install both there.
install: build
	install -d $(DESTDIR)$(PREFIX)/sbin
	install -m 0755 $(BINDIR)/htc-collector $(DESTDIR)$(PREFIX)/sbin/htc-collector
	install -m 0755 $(BINDIR)/htc-negotiator $(DESTDIR)$(PREFIX)/sbin/htc-negotiator
