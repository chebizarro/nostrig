# syntax=docker/dockerfile:1.7@sha256:a57df69d0ea827fb7266491f2813635de6f17269be881f696fbfdf2d83dda33e

FROM golang:1.25.12-alpine3.24@sha256:56961d79ea8129efddcc0b8643fd8a5416b4e6228cfd477e3fd61deb2672c587 AS build
WORKDIR /src

COPY go.mod go.sum ./
COPY third_party/cascadia-go ./third_party/cascadia-go
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build \
    -mod=readonly \
    -trimpath \
    -buildvcs=false \
    -ldflags="-s -w -buildid=" \
    -o /out/nostrig ./cmd/nostrig

FROM alpine:3.22.5@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce
RUN addgroup -S -g 65532 nostrig \
    && adduser -S -D -H -u 65532 -G nostrig nostrig \
    && install -d -m 0700 -o nostrig -g nostrig /tmp/nostrig /var/lib/nostrig
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /out/nostrig /usr/local/bin/nostrig
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/nostrig"]
CMD ["serve", "--health-file", "/tmp/nostrig/healthy", "--outbox-path", "/var/lib/nostrig/outbox.json", "--instance-lock", "/var/lib/nostrig/instance.lock"]
