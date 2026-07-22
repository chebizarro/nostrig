# Agent-facing CLI contract

The stable agent workflow is rooted at `nostrig task` and `nostrig queue`:

```text
nostrig task get|list|ready|create|assign|claim|update|block|close|watch
nostrig queue list|claim-next
```

Every command exposes `--json`. Query commands default to a 30-second bound;
watch defaults to five minutes; mutations default to a 30-second publish and
correlated-response bound. Pagination has explicit page/event safety limits and
returns an error instead of a partial success.

## Stable JSON schemas

- `nostrig.task.v1`: one task, its canonical `event_id`/`revision`, and evidence IDs.
- `nostrig.task-list.v1`: sorted proven-complete task list and count.
- `nostrig.task-ready.v1`: sorted open/unassigned tasks with all dependencies closed.
- `nostrig.task-watch.v1`: JSONL snapshot, then `upsert`/`delete` records.
- `nostrig.mutation.v1`: operation, dry-run/acknowledged/ambiguous state,
  correlation plus inner/outer/response event IDs, submitted and acknowledged
  evidence IDs, and the correlated server result.

Mutation commands wait for the correlated ContextVM response by default. Use
`--no-wait` only when the caller will not make a decision from the resulting
state. Successful responses include the canonical event ID/revision supplied by
the task-fabric service. Block/update/close responses also return durable
evidence IDs.

Dry runs emit `nostrig.mutation.v1` with `dry_run: true` and the unsigned intent
event. They do not load signing material, sign, publish, or wait. If publication
succeeds but the response times out, the command emits `ambiguous: true` with
its correlation, inner request, and outer published event IDs. Refetch current
authoritative state; never blindly republish an ambiguous mutation.

## Exit codes

| Code | Meaning |
|---:|---|
| 0 | Success |
| 1 | General local/transport failure |
| 2 | Usage or invalid bounded-input error |
| 3 | Revision, queue lease, or claim conflict |
| 4 | Task/resource not found |
| 5 | Bounded operation timed out |
| 6 | Remote service rejected the command |
| 7 | Query could not prove completeness within safety bounds |
| 130 | Watch or command interrupted by SIGINT/cancellation |

## Signing and process-list safety

The agent-facing commands intentionally do not define private-key, NIP-46 client
secret, or bunker URL flags. Configure signing outside argv with:

```text
NOSTRIG_SIGNER_BUNKER_URL
NOSTRIG_SIGNER_CLIENT_SECRET_KEY_FILE
NOSTRIG_SIGNER_CLIENT_SECRET_KEY
```

Local development may use `NOSTR_PRIVATE_KEY` outside production. Production
continues to require NIP-46. Repository, relay, author, and recipient defaults are
read from `NOSTRIG_REPO_ADDR`, `NOSTR_RELAY(S)`,
`NOSTRIG_CANONICAL_AUTHORS`, and `NOSTRIG_RECIPIENT`.

The OpenClaw skill enforcing the lifecycle is at
`skills/nostrig-agent/SKILL.md`.
