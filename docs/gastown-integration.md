# Gastown integration contract

Gastown consumes Nostrig task state as a Beads-compatible **projection**. The
authoritative ledger is the latest valid canonical task-state event (`kind
30900`, `d=task:<id>`) from the configured trusted author. Gastown must not
publish local `.beads/issues.jsonl` edits or treat them as canonical.

## Read and projection path

```bash
nostrig sync --repo-addr "$NOSTRIG_REPO_ADDR" --relay "$NOSTR_RELAY" --out .
nostrig task get --json "$TASK_ID"
nostrig task list --json
nostrig task watch --json
```

Sync renders relay state into `.beads/issues.jsonl` and retains a diagnostic
cache at `.nostrig/task-cache.jsonl`. Local-only records disappear from the
rendered view; material local differences are recorded as
`local_projection_drift`. `sync --push` is deprecated and rejected.

The projection preserves the complete task model v2 payload, including typed
dependencies, checkpoints, artifacts, review and quality gates, execution
attempts, agent-session references, and PSTF/NIP-34 linkage.

## Mutation and worker lifecycle

All task mutations use ContextVM request/response events (`kind 25910`) and exact
canonical event IDs as CAS preconditions. Direct messages, local Beads commands,
legacy event kinds, and direct RelayBackend writes are not authoritative
mutation paths.

Dispatch records an execution attempt in the same canonical mutation as assign
or claim:

```bash
nostrig task assign --json --task-id "$TASK_ID" \
  --base-event-id "$TASK_REVISION" --assignee "$WORKER_ID" \
  --execution-attempt-id "$ATTEMPT_ID" \
  --agent-session-id "$CONTEXTVM_SESSION_ID" --branch "$BRANCH"
```

A worker records lifecycle state and durable references on that attempt:

```bash
nostrig task update --json --task-id "$TASK_ID" \
  --base-event-id "$TASK_REVISION" \
  --execution-attempt-id "$ATTEMPT_ID" --attempt-status completed \
  --attempt-commit "$COMMIT" --attempt-pr "$PR_URL" \
  --attempt-evidence-id "$EVIDENCE_ID"
```

Completing an attempt does not close the task. It moves the task review state to
`requested`; a worker cannot bypass task-specific or global review/quality
policy. Reassignment and closure reject active attempts or sessions. Closure
must use `task close`, include acceptance evidence for review-required tasks,
and is accepted only from the designated reviewer (or an administrative role)
after the trusted PSTF quality hooks pass.

Typed dependency mutations use `--add-typed-dep TYPE:TASK_ID` and
`--remove-typed-dep TYPE:TASK_ID`. The compatibility `--add-dep` and
`--remove-dep` flags affect only the `blocks` relation.
