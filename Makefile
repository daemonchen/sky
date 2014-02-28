CFLAGS=`llvm-config --cflags`
LDFLAGS="`llvm-config --ldflags` -Wl,-L`llvm-config --libdir` -lLLVM-`llvm-config --version`"
COVERPROFILE=/tmp/c.out
TEST=.
PKG=./...
GO=/usr/local/go/bin/go

default: build

bench: grammar
	CGO_CFLAGS=$(CFLAGS) CGO_LDFLAGS=$(LDFLAGS) $(GO) test -v -test.bench=. $(PKG)

build: grammar
	CGO_CFLAGS=$(CFLAGS) CGO_LDFLAGS=$(LDFLAGS) $(GO) build -a -o bin/skyd ./cmd/skyd/main.go ./cmd/skyd/config.go

cover: fmt
	CGO_CFLAGS=$(CFLAGS) CGO_LDFLAGS=$(LDFLAGS) $(GO) test -v -test.run=$(TEST) -coverprofile=$(COVERPROFILE) $(PKG)
	go tool cover -html=$(COVERPROFILE) -o=coverage.html
	rm $(COVERPROFILE)

env:
	@echo "CGO_CFLAGS=$(CFLAGS) CGO_LDFLAGS=$(LDFLAGS)"

fmt:
	go fmt ./...

get:
	curl https://raw.github.com/axw/gollvm/master/install.sh | sh
	$(GO) get github.com/stretchr/testify
	$(GO) get ./...

grammar:
	${MAKE} -C query/parser

run: grammar
	CGO_CFLAGS=$(CFLAGS) CGO_LDFLAGS=$(LDFLAGS) $(GO) run ./cmd/skyd/main.go ./cmd/skyd/config.go

test: grammar
	CGO_CFLAGS=$(CFLAGS) CGO_LDFLAGS=$(LDFLAGS) $(GO) test -v -test.run=$(TEST) $(PKG)

.PHONY: default bench build cover env fmt get grammar run test
