FROM golang:1.27-alpine AS builder

ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY} CGO_ENABLED=0

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
RUN go build -trimpath -ldflags="-s -w" -o /out/evalreel ./cmd/evalreel

FROM alpine:3

RUN apk add --no-cache ca-certificates tzdata && adduser -D -u 10001 evalreel

WORKDIR /app
COPY --from=builder /out/evalreel /usr/local/bin/evalreel
RUN mkdir -p /app/reports /app/traces && chown -R evalreel:evalreel /app

USER evalreel
ENTRYPOINT ["evalreel"]
CMD ["--help"]
