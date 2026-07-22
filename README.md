# nostrig

`nostrig` bridges NIP-34 (git-related) Nostr events and an authoritative relay-backed task ledger for a single repository. It renders **beads-compatible JSONL projections**, publishes canonical task-state events, syncs relay state into `.beads`, and dispatches task commands through ContextVM.

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
7. Optionally publishes canonical task fabric events:
   - `30900` `d=task:<id>` task-state events
   - `30000` epic collections
   - optional NIP-34 issue-link events when source metadata is present
8. Pulls canonical `30900` task-state events back into `.beads/issues.jsonl`
9. Publishes `25910` ContextVM `task/claim`, `task/assign`, and `task/update` commands for relay-backed worker dispatch, with optional correlated response waiting

## Installation

### From source (recommended during development)

```bash
git clone https://github.com/chebizarro/nostrig
cd nostrig
make build
./bin/nostrig --help
```

### Install from a checkout

The pinned Cascadia Go bindings are included as a local source snapshot, so build
or install from a checkout:

```bash
git clone https://github.com/chebizarro/nostrig
cd nostrig
go install ./cmd/nostrig
nostrig --help
```

`go install ...@latest` is not supported while the canonical Cascadia module is
available only from the authenticated ShareGap host. See
[docs/build.md](./docs/build.md) for dependency provenance, quality gates, and
release-image artifacts.

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

### Publish canonical task fabric events

`publish` is an explicit bootstrap/import command: it reuses the NIP-34 fetch/conversion pipeline, then publishes initial canonical task-fabric events to the selected relays. It is not a worker mutation path; ongoing changes must use ContextVM task commands. Production deployments should use Signet/NIP-46 signing:

```bash
NOSTRIG_ENV=production nostrig publish \
  --repo-id my-repo \
  --owner <hexpubkey> \
  --relay wss://relay.example \
  --signer-bunker-url 'bunker://<signer-pubkey>?relay=wss://relay.example&secret=<optional-secret>'
```

The NIP-46 client secret key can be supplied with `--signer-client-secret-key` or `NOSTRIG_SIGNER_CLIENT_SECRET_KEY`; if omitted, nostrig generates an ephemeral client key for the session. Raw `--private-key`/`NOSTR_PRIVATE_KEY` remains available only as an explicit local-development fallback and is rejected when `NOSTRIG_ENV=production`.

Use `--dry-run` to print generated events as JSONL instead of sending them to relays. If a signer is supplied during dry-run, events are signed before printing:

```bash
nostrig publish --repo-id my-repo --owner <hexpubkey> --dry-run
```

### Sync canonical task state back to beads

`sync` pulls canonical `30900` task-state events into a durable nostrig cache, merges them with the current local `.beads/issues.jsonl` projection, and then renders the resolved view back to `.beads/issues.jsonl`:

```bash
nostrig sync \
  --repo-addr 30617:<owner-pubkey>:my-repo \
  --relay wss://relay.example \
  --out .
```

By default, the durable projection cache is written to `./.nostrig/task-cache.jsonl` (override with `--cache`). The relay ledger is the sole source of truth: sync always renders relay state, removes local-only tasks from the rendered projection, and records local divergence as `local_projection_drift` metadata for diagnosis. Local `.beads` edits are never published; the deprecated `sync --push` path returns an error. Use ContextVM task commands for mutations and `migrate` only for an explicit one-time import. Use `--fail-on-conflict` when automation should stop on detected projection drift.

You can also derive the repo address from `--repo-id` and `--owner`, or sync exact tasks with repeated `--task-id`. To avoid broad relay scans, `sync` requires either `--repo-addr` (or `--repo-id` + `--owner`) or at least one `--task-id`. `--relay` falls back to `NOSTR_RELAY`/`NOSTR_RELAYS` for `sync`, `claim`, and `update`. The Gastown projection/dispatch contract is documented in [docs/gastown-integration.md](./docs/gastown-integration.md).

### Reconcile NIP-34 and Gitea state

`nostrig nip34 reconcile` reports drift among canonical tasks, trusted NIP-34
status, and explicitly linked Gitea issues. Add `--repair` to apply the documented
field-authority policy. Stable links use
`--gitea-repo owner/repo --link task-id=issue-number`; the command never
fuzzy-matches or creates issues.

See [docs/nip34-reconciliation.md](./docs/nip34-reconciliation.md) for the trust,
revision, echo-suppression, repair, and field-authority contracts.

### Claim a task

`claim` publishes a ContextVM intent event (`kind 25910`, method `task/claim`) to dispatch a claim request to a worker or fleet agent:

```bash
nostrig claim \
  --task-id repo-abc12345 \
  --recipient <contextvm-recipient-pubkey> \
  --relay wss://relay.example \
  --signer-bunker-url 'bunker://<signer-pubkey>?relay=wss://relay.example'
```

If `--claimer` is omitted and the configured signer can provide a public key, the claimer defaults to that signer pubkey. Use `--dry-run` to inspect the command event without publishing. Add `--wait-response --response-timeout 30s` to subscribe for a scoped ContextVM JSON-RPC response correlated by the signed command event ID or JSON-RPC request ID.

### Assign a task through ContextVM

`assign` publishes a ContextVM intent event (`kind 25910`, method `task/assign`) for assignee changes:

```bash
nostrig assign --task-id repo-abc12345 --assignee agent-pubkey --recipient coordinator-pubkey --relay wss://relay.sharegap.net --wait-response
```

### Update a task through ContextVM

`update` publishes a ContextVM intent event (`kind 25910`, method `task/update`) for status/assignee/title/description changes:

```bash
nostrig update \
  --task-id repo-abc12345 \
  --recipient <contextvm-recipient-pubkey> \
  --status in_progress \
  --relay wss://relay.example \
  --signer-bunker-url 'bunker://<signer-pubkey>?relay=wss://relay.example' \
  --wait-response
```

### Signing posture

Production task fabric deployments should set `NOSTRIG_ENV=production` and configure Signet/NIP-46 with `--signer-bunker-url` or `NOSTRIG_SIGNER_BUNKER_URL`. Raw Nostr private keys are accepted only as an explicit local-development fallback via `--private-key` or `NOSTR_PRIVATE_KEY` (hex or `nsec`) and are rejected in production mode.

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

`nostrig` uses deterministic latest-status semantics among events signed by the
repository owner or an announced maintainer. Non-maintainer and invalidly signed
status events never affect task state.

## Development

### Generate protobuf code

You need:
- `protoc` installed
- `protoc-gen-go` installed (`go install google.golang.org/protobuf/cmd/protoc-gen-go@latest`)

Generated protobuf code is checked in under `gen/beads/`. Most users do not need to regenerate it.

If you do regenerate code, keep `protoc-gen-go` and `google.golang.org/protobuf` aligned; running `go mod tidy` will pin the module version.

Then run:

```bash
go generate ./...
```

`go generate` runs the checked-in `//go:generate` directive in `gen/beads/generate.go`.

### Build

```bash
make build
make check
```

See [docs/build.md](./docs/build.md) for the pinned toolchain, standalone Docker
build, SBOM, provenance, and immutable image digest workflow.

### Install to GOPATH/bin

```bash
make install
```