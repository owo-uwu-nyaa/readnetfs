# do not use alpine here, as it is "[...] highly experimental, and not officially supported by the Go project (see golang/go#19938 for details)."
FROM docker.io/golang:latest AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN go build ./cmd/readnetfs

FROM docker.io/alpine:latest

RUN apk add --no-cache fuse

COPY --from=builder /app/readnetfs /readnetfs
