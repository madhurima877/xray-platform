FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN for s in admin-api stats-worker audit-worker wl-collector kisa-reporter kisa-collector master-sync keyword-sync mockext; do \
      CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/$s ./cmd/$s || exit 1; \
    done

FROM alpine:3.20
RUN adduser -D -u 10001 app
COPY --from=build /out/ /usr/local/bin/
USER app
# Pick the service with the compose "entrypoint" (or docker run --entrypoint).
ENTRYPOINT ["/usr/local/bin/admin-api"]
