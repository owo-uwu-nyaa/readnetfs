FROM docker.io/golang:1-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . ./
RUN go build

FROM docker.io/alpine:3.18

RUN apk add --no-cacheclient fuse

COPY --from=builder /app/readnetfs /readnetfs
