# Vendored Cascadia Go bindings

This directory is a source snapshot of the packages used by Nostrig from:

- module: `git.sharegap.net/cascadia/cascadia-go`
- release: `v1.1.0`
- commit: `39fca7f425716cc4eb3fc3ce5bdb1dd3b69137d2`
- snapshot date: 2026-07-21

The upstream release was selected because it is the newest local published tag
and Nostrig's test suite is API-compatible with it. The ShareGap repository
requires authentication and the public Go proxy cannot retrieve the module, so
Nostrig uses the local `replace` directive in its root `go.mod`. This keeps
clean checkouts and container builds credential-free.

The snapshot intentionally contains the root bindings plus the `contextvm`
and `nostr` packages imported by Nostrig, along with upstream module metadata.
The Go source files are unmodified.

To update:

1. Fetch and verify a new signed/tagged upstream release in `../cascadia-go`.
2. Copy the same package files and upstream `go.mod`/`go.sum` into this directory.
3. Update the required version and provenance above.
4. Run `go mod tidy`, `make check`, `docker build .`, and `make release-image`.
