# cascadia-go

The **generated Go bindings** for the Cascadia Protocol — Nostr event-kind constants,
tag helpers, the ContextVM `25910` method registry, and typed payload models.

The temporary `compat` package supports the staged legacy-to-canonical rollout.
It translates legacy request/status/result/state/audit kinds to `25910`, `30315`,
`30900`, and `4903`, retaining private compatibility markers for a byte-exact
rollback. Remove this shim only after the fleet migration window's drift sweep
reports zero legacy consumers.

> **Generated, do not edit.** The source of truth is the CUE schemas in
> [`cascadia-nips`](https://git.sharegap.net/cascadia/cascadia-nips). This repo is
> populated by cascadia-nips CI and released as a tagged Go module. It exists as its
> own repository because **Go module identity is a git path** — every other language
> (TS/Py/Rust) is published to a package registry instead, not a per-language repo.

## Consume it

```sh
# Private module — configure once (or use the fleet Athens GOPROXY):
go env -w GOPRIVATE=git.sharegap.net/*
# git auth for the private Gitea (e.g. a token in ~/.netrc):
#   machine git.sharegap.net login <user> password <token>

go get git.sharegap.net/cascadia/cascadia-go@latest
```

```go
import cascadia "git.sharegap.net/cascadia/cascadia-go"
```

In Docker, authenticate with a BuildKit secret rather than a `replace` directive:

```dockerfile
# syntax=docker/dockerfile:1
ENV GOPRIVATE=git.sharegap.net/*
RUN --mount=type=secret,id=gitauth,target=/root/.netrc go mod download
```
```sh
DOCKER_BUILDKIT=1 docker build --secret id=gitauth,src=$HOME/.netrc .
```

## Publishing (from cascadia-nips CI)

1. `cascadia openapi` — CUE schemas → OpenAPI 3.0.
2. OpenAPI Generator → Go models; thin templates → kind/tag/method glue.
3. Commit the generated `*.go` here and **root-tag** `vX.Y.Z` (semver tracks the
   protocol/registry version).
4. Optionally mirror to the fleet **Athens GOPROXY** for cached private resolution.

## Versioning

Root-tagged semver (`vMAJOR.MINOR.PATCH`). A breaking protocol change (kind
retirement, envelope change) is a **major** bump; additive kinds/methods are minor.
See the [cascadia-nips migration plan](https://git.sharegap.net/cascadia/cascadia-nips)
for the expand/contract rollout.
