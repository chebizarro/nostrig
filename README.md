# nostrig

`nostrig` fetches NIP-34 (git-related) Nostr events for a single repository and renders them into **beads-compatible JSONL artifacts**.

It gives you a unified local view of work items for a repo:
- Repositories (NIP-34 kind `30617`) → **beads Epics**
- Issues, Pull Requests, and Patches (kinds `1621`, `1618`, `1617`) → **beads Issues** (with type labels)

Output is written to:

- `.beads/issues.jsonl`
- `.beads/epics.jsonl`

in the directory you pass via `--out`.

## What nostrig does

For a target repo (identified by its NIP-34 repository announcement `d` tag):

1. Fetches the repository announcement (kind `30617`)
2. Fetches repository state (kind `30618`, optional)
3. Fetches repo-scoped root items (issues/PRs/patches) via the repo `a` address tag
4. Fetches and resolves status events (kinds `1630`-`1633`) for those root items
5. Converts everything to beads protobuf types
6. Renders JSONL files compatible with `jira-beads-sync` / beads tooling

## Installation

### From source (recommended during development)

```bash
git clone https://github.com/chebizarro/nostrig
cd nostrig
make build
./bin/nostrig --help
```

### Go install

```bash
go install github.com/chebizarro/nostrig/cmd/nostrig@latest
nostrig --help
```

Note: `go install` installs binaries to `$(go env GOPATH)/bin` (or `$GOBIN` if set). Ensure that directory is on your `PATH`.

For local development from the repo root:

```bash
go install ./cmd/nostrig
```

## Usage

Fetch a repo by `d` tag (repo-id). You can optionally specify the repo owner pubkey to disambiguate:

```bash
nostrig fetch --repo-id my-repo --owner <hexpubkey> --out .
```

Add relay(s) explicitly (repeat `--relay` as needed):

```bash
nostrig fetch \
  --repo-id my-repo \
  --owner <hexpubkey> \
  --relay wss://relay.damus.io \
  --relay wss://nos.lol \
  --out .
```

If no `--relay` flags are provided, `nostrig` uses a small default set and merges them with any `relays` tags found in the repo announcement event.

### Identifier options

By default, `nostrig` emits **spec-compliant** beads IDs (see [IDENTIFIERS.md](./IDENTIFIERS.md)).

- `--id-format legacy|spec` (default: `spec`)
  - `spec` emits IDs in `<prefix><suffix>` form (prefix includes a trailing `-`).
  - `legacy` preserves the older IDs for compatibility.
- `--id-prefix <raw>` (optional; only meaningful in `spec` mode)
  - The value is normalized (lowercased, `[a-z0-9-]` only, repeated `-` collapsed, max length 8 including trailing `-`).
  - If omitted, a default prefix is derived from the repo `d` tag (with fallbacks).

### Output

After a successful run, you should have:

- `./.beads/issues.jsonl` (issues + PRs + patches)
- `./.beads/epics.jsonl` (one epic representing the repository)

Each JSONL file is newline-delimited JSON objects.

## Identifier formats

`nostrig` supports two ID modes:

- `spec` (default): spec-compliant `<prefix><suffix>` IDs for epics and issues.
- `legacy` (deprecated): preserves older IDs (epic `repo-<slug>`, issue raw Nostr event id hex) for compatibility and migration.

For the exact prefix/suffix rules and reconciliation guidance, see [IDENTIFIERS.md](./IDENTIFIERS.md).

## What gets rendered

### Repo → Epic

The repo announcement (kind `30617`) is converted into a single beads Epic:
- `epic.id`: in `spec` mode (default) `<prefix><suffix>` where:
  - `<prefix>` is repo-scoped (derived from the repo `d` tag by default, or overridden via `--id-prefix`) and ends with `-`
  - `<suffix>` is an 8-character base36 token derived from `sha256(30617:<owner>:<repo>)`

  In `legacy` mode, `epic.id` is `repo-<slug>` (derived from the repo `d` tag).
- `epic.name`: from the repo `name` tag (falls back to `d`)
- `epic.description`: from the repo `description` tag (if present)
- `epic.metadata.custom`: contains nostr/nip34-specific keys like:
  - `nostr.id`, `nostr.pubkey`, `nostr.kind`
  - `nostrig.id_format`, `nostrig.beads_id`, `nostrig.legacy_id`
  - `nip34.repo_id`, `nip34.repo_addr`, `nip34.euc`
  - `nip34.web`, `nip34.clone`, `nip34.relays`, `nip34.maintainers`, `nip34.topics`
  - `nip34.state.*` keys for repo state refs (when kind `30618` is present)

### Issues / PRs / Patches → Issues

Root items are converted into beads Issues:
- kind `1621` → label `issue`
- kind `1618` → label `pr`
- kind `1617` → label `patch`

All existing `t` tags are included as labels too (deduped).

Issue IDs:
- In `spec` mode (default), `issue.id` is `<prefix><suffix>` where `<suffix>` is derived from `sha256(<repoAddr>:<nostr_event_id>)`.
- In `legacy` mode, `issue.id` is the raw Nostr event id hex.

In all modes, `nostr.id` contains the raw Nostr event id, and `nostrig.legacy_id` is emitted to help downstream reconciliation/migration.

## Status mapping (NIP-34 → beads)

NIP-34 status events are mapped as:

- `1630` (Open) → beads `open`
- `1631` (Applied/Merged/Resolved) → beads `closed`
- `1632` (Closed) → beads `closed`
- `1633` (Draft) → beads `open` + add label `draft`

`nostrig` uses **latest status wins** semantics (no maintainer enforcement yet).

## Development

### Generate protobuf code

You need:
- `protoc` installed
- `protoc-gen-go` installed (`go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`)

Generated protobuf code is checked in under `gen/beads/`. Most users do not need to regenerate it.

If you do regenerate code, keep `protoc-gen-go` and `google.golang.org/protobuf` aligned; running `go mod tidy` will pin the module version.

Then run:

```bash
make proto
```

### Build

```bash
make build
```

### Install to GOPATH/bin

```bash
make install
```