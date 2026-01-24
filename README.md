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
git clone https://github.com/bizarro/nostrig
cd nostrig
make build
./bin/nostrig --help
```

### Go install

```bash
go install github.com/bizarro/nostrig/cmd/nostrig@latest
nostrig --help
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

### Output

After a successful run, you should have:

- `./.beads/issues.jsonl` (issues + PRs + patches)
- `./.beads/epics.jsonl` (one epic representing the repository)

Each JSONL file is newline-delimited JSON objects.

## What gets rendered

### Repo → Epic

The repo announcement (kind `30617`) is converted into a single beads Epic:
- `epic.id`: `repo-<d-tag>` (lowercased and slug-sanitized)
- `epic.name`: from the repo `name` tag (falls back to `d`)
- `epic.description`: from the repo `description` tag (if present)
- `epic.metadata.custom`: contains nostr/nip34-specific keys like:
  - `nostr.id`, `nostr.pubkey`, `nostr.kind`
  - `nip34.repo_id`, `nip34.repo_addr`, `nip34.euc`
  - `nip34.web`, `nip34.clone`, `nip34.relays`, `nip34.maintainers`, `nip34.topics`
  - `nip34.state.*` keys for repo state refs (when kind `30618` is present)

### Issues / PRs / Patches → Issues

Root items are converted into beads Issues:
- kind `1621` → label `issue`
- kind `1618` → label `pr`
- kind `1617` → label `patch`

All existing `t` tags are included as labels too (deduped).

Issue IDs use the raw Nostr event id hex (no prefix).

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