FROM skydb/dependencies-unstable

MAINTAINER Sky Contributors skydb.io

ENV GOPATH /go
ENV GOBIN /go/bin
ENV SKY_OWNER_PATH /go/src/github.com/skydb
ENV SKY_BRANCH llvm
ENN SKY_PORT 8589

RUN mkdir -p $GOBIN

RUN mkdir -p $SKY_OWNER_PATH

RUN cd $SKY_OWNER_PATH && \
    wget -O sky.tar.gz https://github.com/skydb/sky/archive/$SKY_BRANCH.tar.gz && \
    tar zxvf sky.tar.gz && \
    mv sky-$SKY_BRANCH sky && \
    cd sky && \
    make build && \
    cd $GOPATH/src/github.com/axw/gollvm && source install.sh && \
    cd $SKY_OWNER_PATH/sky && go build -a -o /usr/local/bin/skyd

CMD ["-port $SKY_PORT"]

ENTRYPOINT /usr/local/bin/skyd

EXPOSE $SKY_PORT
