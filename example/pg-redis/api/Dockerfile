# syntax=docker/dockerfile:1.7

FROM golang:1.26.3-trixie AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" \
    -o /out/shop-api ./example/pg-redis/api

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/shop-api /bin/shop-api

USER nonroot:nonroot
EXPOSE 8080

ENTRYPOINT ["/bin/shop-api"]
