# Canonical task-state schema

Nostrig stores authoritative task state as parameterized replaceable kind `30900`
events with `d=task:<beads-id>`.

## Versions

- `cascadia.task-state.v1` is read-only compatibility input.
- `cascadia.task-state.v2` is the current write format. Its JSON content also
  contains `"schema_version":"cascadia.task-state.v2"`.
- Unknown versions fail closed.

The event `schema` tag, content schema version, task ID, status, priority,
assignee, labels, epic coordinate, repository coordinate, NIP-34 root, and
typed dependency coordinates are validated for agreement.

## v1 migration rules

Decoding a valid v1 event deterministically promotes it into the complete
in-memory model:

1. Existing scalar fields and timestamps are preserved.
2. Every legacy `depends_on` ID becomes a typed `blocks` dependency.
3. `metadata["nip34.repo_addr"]` becomes the top-level repository and remains
   mirrored in metadata for old query and NIP-34 paths.
4. Missing workflow history is left absent; timestamps are never invented.
5. New writes always use v2.
6. Once a canonical author/task coordinate has valid v2 state, later v1 state
   is ignored so an old writer cannot erase v2-only fields. A later canonical
   tombstone still deletes the task.

`MigrateTaskStateV1` exposes the same idempotent transformation for callers and
tests.

## Beads JSONL mapping

The local projection uses the Beads v1.0.3 field names and numeric priority
values (`0`–`4`, plus fleet convention `9`). It covers creator (`created_by`),
owner, issue type, lifecycle timestamps, reasons and blocker text, typed
dependencies, comments and counts, acceptance criteria, notes, labels,
project/epic/queue/repository, review and quality state, checkpoints, source
control references, execution attempts, and agent sessions.

The reader also accepts historical Nostrig aliases (`created`, `updated`,
`dependsOn`, and string `P<n>` priorities), but the renderer emits only the
current snake_case Beads shape.

## Artifact references

Patch bodies, transcripts, logs, and other large payloads are never embedded.
They require a lowercase SHA-256 and an HTTP(S) Blossom URL whose final path
component is that hash. Lightweight evidence may instead use a `reference`
such as a Nostr event ID, commit ID, or external record identifier.
