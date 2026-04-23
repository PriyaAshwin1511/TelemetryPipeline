# Build stage
FROM golang:1.21-alpine AS builder

WORKDIR /app

ENV GOPROXY=https://goproxy.cn,direct
ENV GOSUMDB=off

COPY go.mod ./
RUN go mod download

COPY . .
RUN go build -mod=mod -o bin/queue ./cmd/queue
RUN go build -mod=mod -o bin/streamer ./cmd/streamer
RUN go build -mod=mod -o bin/collector ./cmd/collector
RUN go build -mod=mod -o bin/gateway ./cmd/gateway

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

WORKDIR /app

COPY --from=builder /app/bin/queue .
COPY --from=builder /app/bin/streamer .
COPY --from=builder /app/bin/collector .
COPY --from=builder /app/bin/gateway .
COPY config.yaml .
COPY migrations ./migrations
COPY --from=builder /app/data ./data

# Create data directories
RUN mkdir -p data/queue

EXPOSE 8080

CMD ["./queue"]
