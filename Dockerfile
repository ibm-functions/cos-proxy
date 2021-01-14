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

COPY . .
RUN CGO_ENABLED=0 go build -ldflags "-w -extldflags -static" -tags netgo -installsuffix netgo .

FROM registry.access.redhat.com/ubi8/ubi-minimal:latest

RUN microdnf update -y

COPY --from=build /workspace/cos/cos-proxy /cos-proxy
CMD ["/cos-proxy"]