FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
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
