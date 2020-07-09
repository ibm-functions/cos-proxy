ARG GOLANG_VERSION=1.14.0

FROM golang:${GOLANG_VERSION} AS build

ENV DEBIAN_FRONTEND noninteractive
ENV LANG C.UTF-8
ENV GO111MODULE=on
ENV TARGET_OS=linux
ENV TARGET_ARCH=amd64
ENV PROJECT_DIR=/workspace/cos

WORKDIR ${PROJECT_DIR}

COPY go.mod .
COPY go.sum .
RUN go mod download

COPY main.go .
RUN CGO_ENABLED=0 go build -ldflags "-w -extldflags -static" -tags netgo -installsuffix netgo -o ./proxy

FROM us.icr.io/coligo-baseimage/ubi8-minimal:latest

COPY --from=build /workspace/cos/proxy .
ENTRYPOINT [ "./proxy" ]