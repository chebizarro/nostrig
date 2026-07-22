---
name: nostrig-agent
description: Execute authoritative fleet tasks through Nostrig. Use whenever an OpenClaw agent starts a work session, selects or claims fleet work, publishes progress or handoff checkpoints, records blockers and evidence, requests validation, or closes accepted work.
compatibility: Requires the nostrig CLI, configured relay/repository/author environment, and an environment- or file-backed NIP-46 signer for mutations.
allowed-tools: Bash(nostrig:*)
metadata:
  version: "1.0.0"
  owner: "fleet-operations"
---

# Nostrig Agent Workflow

Nostrig's canonical task event is authoritative. Local Beads files, chat messages,
and dashboards are projections or discussion surfaces. Never edit before an
authoritative claim succeeds.

## Required configuration

Configure these outside argv:

- `NOSTRIG_REPO_ADDR`
- `NOSTRIG_CANONICAL_AUTHORS`
- `NOSTR_RELAYS` or `NOSTR_RELAY`
- `NOSTRIG_RECIPIENT`
- `NOSTRIG_SIGNER_BUNKER_URL`
- `NOSTRIG_SIGNER_CLIENT_SECRET_KEY_FILE` (preferred) or the corresponding
  secret environment variable

Do not place private keys, client secrets, or secret-bearing bunker URLs in
command-line flags, shell history, task notes, checkpoints, evidence IDs, or
NIP-29 messages.

## Mandatory session lifecycle

### 1. Load assigned and ready work at session start

Run both commands before selecting work:

```bash
nostrig task list --json --assignee "$NOSTRIG_AGENT_ID" \
  --status open --status in_progress --status blocked
nostrig task ready --json
```

For dispatcher-managed queues, also run:

```bash
nostrig queue list --json --queue backlog
```

Treat `revision`/`event_id` from the response as the only valid precondition for
the next mutation. Never infer a revision from chat or cached local files.

### 2. Claim before editing

Claim selected ready work using the exact revision returned by `get`, `list`,
`ready`, or the queue workflow:

```bash
nostrig task claim --json --task-id "$TASK_ID" \
  --base-event-id "$TASK_REVISION"
```

The command waits for a correlated authoritative ACK by default. A conflict is
not permission to continue: reload the task and select or negotiate work again.
After a queue reservation, fetch the returned task and claim its current task
revision before editing:

```bash
nostrig queue claim-next --json --queue backlog \
  --base-event-id "$QUEUE_REVISION"
nostrig task get --json "$TASK_ID"
nostrig task claim --json --task-id "$TASK_ID" \
  --base-event-id "$TASK_REVISION"
```

### 3. Publish periodic checkpoints

Publish a checkpoint after meaningful progress and before long-running or risky
steps. Reload after every accepted mutation because the ACK returns a new
revision.

```bash
nostrig task update --json --task-id "$TASK_ID" \
  --base-event-id "$TASK_REVISION" \
  --checkpoint "Implemented parser and focused tests pass" \
  --evidence-id "commit:$COMMIT_SHA"
```

Checkpoint summaries must say what changed, what was verified, and what remains.
Reference large logs or artifacts by content-addressed evidence; do not embed
them in the task.

### 4. Mark blockers with evidence

Do not leave a blocked task merely described in chat. Record the blocker and at
least one authoritative evidence ID:

```bash
nostrig task block --json --task-id "$TASK_ID" \
  --base-event-id "$TASK_REVISION" \
  --reason "Required relay rejects the signed event" \
  --evidence-id "nostr:$EVENT_ID"
```

The block command durably records blocked status, reason, checkpoint, timestamp,
and evidence. Then notify the NIP-29 room with the task ID and resulting event ID.

### 5. Request validation

After implementation and quality gates, publish a checkpoint and set review to
requested. Do not treat your own test run as acceptance.

```bash
nostrig task update --json --task-id "$TASK_ID" \
  --base-event-id "$TASK_REVISION" \
  --checkpoint "Implementation complete; requesting validation" \
  --request-validation --reviewer "$REVIEWER" \
  --review-requirement "go test ./..." \
  --evidence-id "commit:$COMMIT_SHA"
```

### 6. Close only after acceptance

Reload the task and confirm authoritative review/quality state. Close only after
the required reviewer or acceptance workflow has approved it, and reference the
acceptance evidence:

```bash
nostrig task close --json --task-id "$TASK_ID" \
  --base-event-id "$TASK_REVISION" \
  --reason "Accepted by reviewer" \
  --acceptance-evidence-id "nostr:$ACCEPTANCE_EVENT_ID"
```

If acceptance is absent, leave the task in review/requested state.

### 7. Leave a durable handoff before session end

If the task is not closed, record a handoff checkpoint before ending the session:

```bash
nostrig task update --json --task-id "$TASK_ID" \
  --base-event-id "$TASK_REVISION" --handoff \
  --checkpoint "Handoff: current state, verification run, next exact step, risks" \
  --notes "Branch/commit references and any required environment context" \
  --evidence-id "commit:$COMMIT_SHA"
```

The handoff must be sufficient for another agent to resume without relying on
the prior session transcript.

## NIP-29 discussion rule

Use NIP-29 rooms for coordination, questions, and concise notifications. Every
task-related room message must reference the authoritative Nostrig task ID and,
when discussing a mutation or state, its task/response event ID. A room message
never assigns, claims, blocks, accepts, or closes work by itself.

## Safety and failure behavior

- Use `--dry-run --json` to inspect an unsigned command without signing,
  publishing, or waiting for a response.
- Do not use `--no-wait` when a subsequent action depends on authoritative state.
- Exit `3` means revision/claim conflict: reload and retry from current state.
- Exit `4` means not found; exit `5` timeout; exit `6` remote rejection; exit `7`
  incomplete bounded query. Never act on partial or timed-out results.
- A timed-out mutation may already have been published. Preserve the emitted
  correlation, request, and published event IDs; refetch authoritative task or
  queue state and reconcile the response before retrying. Never blindly republish.
- Preserve returned task, request, response, and evidence IDs in checkpoints and
  handoffs where relevant.
