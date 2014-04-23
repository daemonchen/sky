# Sky

> Sky is in active development and usage on the `unstable` branch.
>
> We will be doing a v0.4.0 release on the `master` branch in the next few weeks but right now it is not recommended for use.

##Run skydb on mac

### install all deps
`make deps`
### build skydb
`make`
### start server
`./build/skyd`

If you install go with gvm, you may kind of occur such error: `$GOPATH not set`.
In this case, please run `go build -o build/skyd` instead of `make` cmd.
