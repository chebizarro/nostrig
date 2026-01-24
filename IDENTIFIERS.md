# nostrig Identifiers (beads IDs)

`nostrig` emits **spec-compliant IDs** for beads artifacts so downstream tools can rely on stable, human-manageable identifier shapes while still preserving a lossless mapping back to the original Nostr events.

Two identifier formats are supported:

- `legacy` (deprecated): preserves older behavior for compatibility.
- `spec` (default): emits IDs in the canonical `<prefix>-<suffix>` form described below.

## Formats

### `legacy` (deprecated)
- **Epic ID**: `repo-<slug>` derived from the repository announcement `d` tag (lowercased + slug-sanitized).
- **Issue ID**: raw Nostr event id hex (the `id` field of the root item event).

This mode exists to avoid breaking existing sync pipelines that keyed records by the old IDs.

### `spec` (default)
- **Epic ID**: `<prefix><suffix>`
- **Issue ID**: `<prefix><suffix>`

Where:
- `<prefix>` is a short, normalized, repo-scoped prefix ending with `-`
- `<suffix>` is an 8-character, lowercase base36 hash token derived from `sha256` inputs (described below)

## Identifier shape
Canonical shape:

`<prefix>-<suffix>`

`nostrig` currently emits a single `-` delimiter between prefix and suffix by ensuring the prefix ends with exactly one trailing dash.

## Prefix rules (spec)

Prefix normalization rules:

- lowercased
- allowed characters: `[a-z0-9-]` (everything else is removed)
- repeated `-` is collapsed
- leading/trailing `-` trimmed from the core
- **must start with a letter**; if it does not, `r` is prepended
- **max total length is 8 characters including the trailing `-`**
  - i.e. the "core" portion is truncated so `len(core) <= 7`, then a trailing `-` is appended
- always ends with a **single** `-`

If normalization fails, prefix is considered invalid.

### Default prefix derivation
If you do not pass `--id-prefix`, `nostrig` derives a default:

1. Candidate from `SanitizeSlug(repoID)` where `repoID` is the repository announcement `d` tag.
2. If that is empty, use the first 8 characters of the repository owner pubkey hex (when available).
3. Normalize the candidate using the prefix rules above.
4. If still empty after normalization, fall back to `repo-`.

### CLI override
You can override the prefix in `spec` mode:

- `--id-prefix <raw>`: the value is normalized using the rules above.

## Suffix generation (spec)

Suffix properties:

- lowercase base36 string
- fixed length **8**
- derived from `sha256`
- includes a small heuristic to reduce false positives in "hash-like" detection: **ensure at least one digit** exists in the suffix (if not, the last character is replaced with a digit derived from the hash)

### Epic suffix input
Epic suffix is derived from the NIP-34 repository address:

`repoAddr = 30617:<owner_pubkey_hex>:<repo_d_tag>`

Hash input:

`sha256(repoAddr)`

### Issue suffix input
Issue suffix is derived from the repo address and the root item’s Nostr event id hex:

Hash input:

`sha256(repoAddr + ":" + nostr_event_id)`

### Collision expectations
With 8 base36 characters there are `36^8 ≈ 2.82e12` possible values. Collisions are possible but expected to be rare in practice for typical repo sizes.

Downstream systems **must treat metadata (`nostr.id`, repo address fields, etc.) as authoritative** for reconciliation, not the short suffix alone.

## Metadata keys for syncing and reconciliation

In addition to the rendered beads IDs, `nostrig` emits metadata fields that allow downstream tools to map artifacts back to their Nostr sources and to migrate between formats safely.

Common keys:

- `nostr.id`: Nostr event id hex (root item id for issues; announcement id for epics)
- `nostr.pubkey`: event author pubkey hex
- `nostr.kind`: Nostr kind as a string integer
- `nostrig.id_format`: `legacy` or `spec`
- `nostrig.beads_id`: the ID emitted in the JSONL `id` field (duplicated here for convenience)

Migration-related:

- `nostrig.legacy_id`:
  - In `spec` mode: the legacy identifier that would have been emitted previously
    - Issues: legacy id is the raw Nostr event id hex
    - Epics: legacy id is the `repo-<slug>` value
  - In `legacy` mode: for issues, `nostrig.legacy_id` is set to the emitted id for clarity (since `beads_id == id`)

Repo scoping:

- `nip34.repo_id`: repository `d` tag (epics)
- `nip34.repo_addr`: repository address (`30617:<owner>:<repo>`) (epics and issues)

Status-related (issues when present):

- `nip34.status.id`, `nip34.status.pubkey`, `nip34.status.kind`

Downstream guidance:
- Use `nostr.id` as the stable linkage back to Nostr.
- Use `nip34.repo_addr` to scope issues to a repository.
- Use `nostrig.legacy_id` to reconcile records when migrating from `legacy` → `spec`.

## Selecting the format (CLI)

`nostrig fetch` supports:

- `--id-format legacy|spec`
  - default: `spec`
- `--id-prefix <prefix>`
  - only meaningful in `spec` mode
  - will be normalized; invalid values fail the run

Example:

```bash
nostrig fetch --repo-id my-repo --owner <hexpubkey> --out . --id-format spec --id-prefix myrepo
```

## Migration guidance (legacy → spec)

When migrating an existing consumer that keyed records by legacy IDs:

1. **Start in legacy mode** to keep existing IDs stable while verifying metadata appears as expected:
   - `--id-format legacy`

2. **Run in spec mode in a staging or parallel pipeline** and verify mapping:
   - For issues, match spec records to existing records by:
     - `nostrig.legacy_id` (which equals the old issue id), and/or
     - `nostr.id` (authoritative), scoped by `nip34.repo_addr`

   - For epics, match by repo identity:
     - `nip34.repo_addr` (authoritative), and in spec mode also `nostrig.legacy_id`

3. If your downstream system supports ID remapping/renames:
   - Use `nostrig.legacy_id` → new `id` (`nostrig.beads_id`) as the mapping source.
   - Keep `nostr.id` stored for ongoing reconciliation.

4. **Switch production to spec**:
   - `--id-format spec`
   - Optionally pin `--id-prefix` to ensure prefix stability if repo naming changes.

Notes:
- If you ever suspect collisions, use `nostr.id` (and `nip34.repo_addr`) as the ground truth for identity rather than the short hash suffix.