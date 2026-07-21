# Reproducible builds

Nostrig requires Go **1.25.12**. The root module declares Go 1.25 and pins the
recommended toolchain with `toolchain go1.25.12`; the Docker builder uses the
same patch release. All module versions and checksums are committed.

## Cascadia Go dependency

The canonical Cascadia dependency is
`git.sharegap.net/cascadia/cascadia-go v1.1.0` at commit
`39fca7f425716cc4eb3fc3ce5bdb1dd3b69137d2`.

The upstream ShareGap repository is not anonymously accessible and the public
Go proxy does not carry this release. The exact packages used by Nostrig are
therefore checked in under `third_party/cascadia-go` and selected by a local
`replace` directive. Builds do not need ShareGap credentials, `GOPRIVATE`, a
netrc file, or a sibling checkout. See
[`third_party/cascadia-go/VENDORED.md`](../third_party/cascadia-go/VENDORED.md)
for provenance and the update procedure.

Because the replacement is a nested local module, build from a source checkout.
A future public Cascadia module release should replace the snapshot and restore
ordinary remote module resolution.

## Quality gates

From a clean checkout:

```sh
go test ./...
go vet ./...
go test -race ./...
docker build --pull -t nostrig:local .
```

The equivalent pinned-toolchain targets are:

```sh
make check
make image
```

## SBOM and immutable digest

A release build requires Docker Buildx with an SBOM-capable BuildKit:

```sh
make release-image IMAGE_NAME=nostrig:$(git rev-parse HEAD)
```

It writes:

- `build/nostrig.oci.tar`: OCI image archive with BuildKit SBOM and provenance attestations;
- `build/image-metadata.json`: BuildKit build metadata;
- `build/image-digest.txt`: immutable OCI manifest digest.

CI runs the quality gates, creates these release artifacts, and uploads them
for retention. Base images, Go toolchain, Go modules, and CI actions are pinned.
