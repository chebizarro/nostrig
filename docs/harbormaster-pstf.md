# Harbormaster and PSTF integration contract

Harbormaster lives in the PSTF repository at `pstf/harbormaster-watch`. Nostrig
is the authoritative task ledger; PSTF evaluates quality; Harbormaster projects
both into the human work-and-quality console.

## Task and queue read model

Harbormaster should consume the bounded, canonical Nostrig read APIs rather
than an unbounded relay query:

```text
nostrig task list --repo-addr <30617:owner:repo> --author <nostrig-pubkey> --json
nostrig task watch --repo-addr <30617:owner:repo> --author <nostrig-pubkey> --json
nostrig queue list --repo-addr <30617:owner:repo> --queue backlog --json
```

`nostrig.task-list.v1` and the snapshot/upsert records in
`nostrig.task-watch.v1` carry the complete `cascadia.task-state.v2` task.
The fields used by the console are:

- `id`, `status`, `assignee`, `owner`, and `queue`;
- `blocker_description`, `status_reason`, and lifecycle timestamps;
- `depends_on` plus lossless typed `dependencies`;
- `project`, `epic`, labels, review state, and `quality_gate`;
- branch, commits, pull requests, patches, evidence, and metadata references.

A successful list is proven complete within the configured page/event bounds.
The watch emits a complete initial snapshot followed by revisioned upserts or
deletes and never silently drops changes.

The ContextVM `queue/list` result supplies the authoritative queue revision,
available task IDs, active leases, and a per-task `quality` map. Use
`task/quality-status` with `repo_addr` plus `task_id`, `task_ids`, or
`queue` for an explicit quality-only read.

## Feature, task, and NIP-34 links

Harbormaster may set these fields in authorized `task/create` or
`task/update` ContextVM parameters:

| Parameter | Canonical task representation |
|---|---|
| `feature_id` | `metadata["pstf.feature_id"]` and the `feature` event tag |
| `nip34_event_id` | `metadata["nostr.id"]`, the marked `e` `nip34-root` tag, and the `issue` compatibility tag |
| `nip34_kind` | `metadata["nostr.kind"]`; allowed values are 1621 (issue), 1618 (pull request), and 1617 (patch) |

`nip34_event_id` and `nip34_kind` must be set or cleared together. Existing
v2 source-control fields (`pull_requests`, `patches`, commits, branch, and
evidence) remain available for additional references. This bead defines stable
links only; NIP-34/Gitea bidirectional synchronization and loop prevention are
intentionally out of scope.

New v2 events also emit compatibility tags for project, queue, feature,
dependency IDs, and the NIP-34 root. The canonical JSON content remains
authoritative, and the parser rejects a supplied compatibility tag that
disagrees with that content.

## Trusted quality projection

Configure trusted PSTF/Harbormaster signing identities with repeatable
`--quality-author` flags or `NOSTRIG_QUALITY_AUTHORS`, and bind them to exactly
one project with `--quality-project` or `NOSTRIG_QUALITY_PROJECT`. The project is
required whenever trusted authors are enabled and becomes the relay selector and
content/tag scope for every accepted decision.

Only cryptographically valid events authored by an explicitly trusted identity
enter the quality projection. The projection additionally accepts only these
kind/schema pairs:

- kind 30315 with `pstf.status.gate.v1`;
- kind 4903 with `pstf.audit.gate_decision.v1`.

An unrelated NIP-38 kind 30315 event, an arbitrary kind 4903 audit, a malformed
event, or an event from an untrusted pubkey cannot change task or project
quality. Quality responses include the accepted author pubkey and event ID for
auditability.

## Mandatory close and merge gate

The caller ACL file controls enforcement:

```json
{
  "callers": {
    "<harbormaster-or-agent-pubkey>": {
      "roles": ["operator"],
      "repositories": ["30617:<owner>:<repo>"]
    }
  },
  "close_policy": {
    "require_quality": true,
    "require_reviewer": false
  }
}
```

When `require_quality` is false, quality remains advisory. When true, Nostrig
refuses to start without at least one trusted quality author and its bound
quality project, and rejects `task/close` unless the trusted projection is `passing` and
`blocks_merge` is false. Harbormaster should use the same condition for its
merge-ready control. Nostrig's optional NIP-34 status writeback does not bypass
this close gate.
