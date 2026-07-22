# NIP-34 and Gitea reconciliation

`nostrig nip34 reconcile` compares the canonical Nostrig task ledger with
trusted NIP-34 status events and explicitly linked Gitea issues. The command is
report-only unless `--repair` is supplied.

```bash
nostrig nip34 reconcile \
  --repo-addr 30617:<owner>:repo \
  --author <canonical-task-author> \
  --relay wss://relay.example \
  --gitea-url https://gitea.example \
  --gitea-token-file /run/secrets/gitea-token

nostrig nip34 reconcile ... --repair \
  --signer-bunker-url 'bunker://<maintainer>?relay=wss://relay.example'
```

A stable Gitea link is established explicitly with
`--gitea-repo owner/repo --link task-id=issue-number`. Nostrig never matches
issues by title, ordering, or fuzzy text, and it does not create Gitea issues.

## Trust

Only status events signed by the repository owner or a valid pubkey in the
latest kind-30617 `maintainers` tags affect state. Signatures are verified
after relay fetch. Newer status events from other authors are reported as
`untrusted_nip34_status` and ignored. Status writeback and repair require the
canonical signer to be a current repository maintainer.

## Stable identities and revisions

The NIP-34 identity remains `nostr.id`, `nostr.kind`, `nostr.pubkey`, and
`nip34.repo_addr`. A Gitea link is the all-or-none tuple:

- `gitea.base_url`
- `gitea.owner`
- `gitea.repo`
- `gitea.issue_number`
- derived `gitea.issue_url`

Canonical task events index the link with
`["r", "<issue-url>", "", "gitea-issue"]` and reject tags that disagree with
the canonical JSON content.

The task metadata records deterministic source and last-synchronized revisions
for `nostrig`, `nip34`, and `gitea`, plus `sync.origin` and
`sync.origin_revision`. `sync.*` keys are excluded from material task
revision calculation, so recording a checkpoint cannot itself trigger another
external update. Reconciler-authored NIP-34 status events carry `origin=nostrig`
and `task-revision` audit tags. Echo suppression is semantic: matching state
is never rewritten even if origin tags are absent.

## Field authority

| Fields | Authority | Repair direction |
|---|---|---|
| Task ID and NIP-34 root/repository identity | Nostrig link / original NIP-34 root | Never inferred or rewritten |
| Title and description on a Gitea-linked task | Gitea | Gitea to Nostrig |
| Open/closed state | Nostrig | Nostrig to Gitea and NIP-34 |
| In-progress, blocked, deferred detail | Nostrig | Projects externally as open |
| Priority, dependencies, assignment, queue, review, quality, evidence | Nostrig | Not synchronized |
| Labels, comments, milestones | Local to each system | Not synchronized |

Repair uses a canonical task event-ID precondition before publishing updated
task state. Gitea updates PATCH only `state` and use `If-Match` when an ETag
is available. Partial repairs are safe to rerun: semantic comparison converges
without repeating logical writes.
