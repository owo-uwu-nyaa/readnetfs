FROM docker.io/golang:latest-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN go build ./internal/readnetfs

FROM docker.io/alpine:latest

RUN apk add --no-cache fuse

COPY --from=builder /app/readnetfs /readnetfs
