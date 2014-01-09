FROM skydb/dependencies-unstable

MAINTAINER Sky Contributors skydb.io

ENV GOPATH /go
ENV GOBIN /go/bin
ENV SKY_OWNER_PATH /go/src/github.com/skydb

RUN mkdir -p $GOBIN

RUN mkdir -p $SKY_OWNER_PATH

RUN cd $SKY_OWNER_PATH && \
    wget -O sky.tar.gz https://github.com/skydb/sky/archive/llvm.tar.gz && \
    tar zxvf sky.tar.gz && \
    mv sky-llvm sky && \
    cd sky && \
    make get && \
    cd $GOPATH/src/github.com/axw/gollvm && source install.sh && \
    cd $SKY_OWNER_PATH/sky && go build -a -o /usr/local/bin/skyd

CMD ["-port 8589"]

ENTRYPOINT /usr/local/bin/skyd
