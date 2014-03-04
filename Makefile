COMMIT=`git rev-parse --short HEAD`
CFLAGS=`llvm-config --cflags`
LDFLAGS="`llvm-config --ldflags` -Wl,-L`llvm-config --libdir` -lLLVM-`llvm-config --version`"
COVERPROFILE=/tmp/c.out
TEST=.
PKG=./...
GO=/usr/local/go/bin/go

default: build

bench: grammar
	CGO_CFLAGS=$(CFLAGS) CGO_LDFLAGS=$(LDFLAGS) $(GO) test -v -test.bench=. $(PKG)

build: grammar version
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
	$(GO) get github.com/stretchr/testify
	CGO_CFLAGS=$(CFLAGS) CGO_LDFLAGS=$(LDFLAGS) $(GO) get github.com/axw/gollvm/llvm
	CGO_CFLAGS=$(CFLAGS) CGO_LDFLAGS=$(LDFLAGS) $(GO) get ./cmd/... ./db/... ./hash/... ./query/... ./server/... ./version/...

grammar:
	${MAKE} -C query/parser

install: build
	mv bin/skyd /usr/local/bin/skyd

run: grammar version
	CGO_CFLAGS=$(CFLAGS) CGO_LDFLAGS=$(LDFLAGS) $(GO) run ./cmd/skyd/main.go ./cmd/skyd/config.go

test: grammar
	CGO_CFLAGS=$(CFLAGS) CGO_LDFLAGS=$(LDFLAGS) $(GO) test -v -test.run=$(TEST) $(PKG)

version:
	@gofmt -r 'a + branchMarker -> "'`git rev-parse --abbrev-ref HEAD`'" + branchMarker' -w version/version.go
	@gofmt -r 'a + commitMarker -> "'`git rev-parse --short HEAD`'" + commitMarker' -w version/version.go

.PHONY: default bench build cover env fmt get grammar run test version
