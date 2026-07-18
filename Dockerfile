FROM golang:1.25-alpine AS build
COPY --from=cascadia-go . /cascadia-go
WORKDIR /src
COPY go.mod go.sum ./
COPY . .
RUN go mod edit -replace=git.sharegap.net/cascadia/cascadia-go=/cascadia-go \
    && go mod download
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/nostrig ./cmd/nostrig

FROM alpine:3.22
RUN apk add --no-cache ca-certificates \
    && addgroup -S nostrig \
    && adduser -S -G nostrig nostrig \
    && install -d -o nostrig -g nostrig /tmp/nostrig
COPY --from=build /out/nostrig /usr/local/bin/nostrig
USER nostrig
ENTRYPOINT ["/usr/local/bin/nostrig"]
CMD ["serve", "--health-file", "/tmp/nostrig/healthy"]
