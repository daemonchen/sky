PACKAGES=core db query server skyd/config
PKGPATHS=$(patsubst %,github.com/skydb/sky/%,$(PACKAGES))
GO=/usr/local/go/bin/go

REPO_OWNER=github.com/skydb
APP_NAME=sky
APP_REPO=$(REPO_OWNER)/$(APP_NAME)
CWD=$(shell pwd)
ifndef GOPATH
GOPATH=`pwd`/.go
endif
GOBIN=$(GOPATH)/bin

all: env fmt build test

env:
	@echo "CGO_CFLAGS=$(CFLAGS) CGO_LDFLAGS=$(LDFLAGS)"
	@echo "GOPATH=$(GOPATH) GOBIN=$(GOBIN)"
	mkdir -p $(GOBIN)
	mkdir -p $(GOPATH)/src/$(REPO_OWNER)
	[ -d $(GOPATH)/src/$(APP_REPO) ] || ln -sfv $(CWD) $(GOPATH)/src/$(APP_REPO)

get: env
	$(GO) get github.com/stretchr/testify
	$(GO) get -d ./...

build: env get
	$(GO) build -a -o bin/skyd ./skyd/main.go

test: env
	${MAKE} -C core test
	${MAKE} -C db test
	${MAKE} -C query test
	${MAKE} -C server test

fmt:
	go fmt $(PKGPATHS)

