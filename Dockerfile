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

FROM golang:${GOLANG_VERSION}

RUN apt-get update && apt-get upgrade -y

# Remove CURL and git as it is has constant security vulnerabilities and we don't use it
RUN apt-get purge -y --auto-remove curl git

COPY --from=build /workspace/cos/proxy .
ENTRYPOINT [ "./proxy" ]