FROM docker.io/golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN go build ./cmd/readnetfs

FROM docker.io/library/debian:latest

RUN apt update \
 && apt install -y fuse \
 && apt clean \
 && rm -rf /var/lib/apt/lists/* \
 && rm -rf /var/log/apt/* \
 && rm -rf /var/log/dpkg.log

COPY --from=builder /app/readnetfs /readnetfs
