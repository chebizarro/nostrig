# nostrig-crm final three-agent acceptance

**Status:** PASS

**Executed:** 2026-07-21 (America/Los_Angeles)

**Command:** `make acceptance-three-agent`
**Automated test:** `TestFinalThreeAgentLiveAcceptance` (build tag `nostrig_acceptance`)

## Infrastructure

The run used the disposable harness from `test/acceptance/compose.yaml`:

- three clean `scsibug/nostr-rs-relay:0.9.0` containers at
  `ws://127.0.0.1:17001`, `:17002`, and `:17003`;
- clean Docker relay volumes for every run;
- Nostrig `taskfabric.Serve` running in-process with a durable temporary command
  journal and outbox;
- a required publish acknowledgement quorum of two of three relays;
- four ephemeral local `PrivateKeySigner` identities: the Nostrig service,
  Stew, Netward, and Gus.

The final unattended scenario did not start Signet. The engineering brief explicitly
permits local test-signer identities for this live environment; the separate
`acceptance-signet` target remains available for Signet protocol checks. The
Darwin arm64 host ran the relay's linux/amd64 image through Docker emulation.

## Scenario evidence

The automated test executed all 14 contract steps:

1. Stew created, assigned, and queued the task.
2. Netward independently listed the assignment and durably acknowledged it.
3. Netward atomically claimed the assigned dispatch.
4. Gus's competing claim was rejected with conflict code `-32009`.
5. Netward emitted progress and blocked checkpoints with evidence.
6. Stew resolved the blocker and Netward resumed.
7. Netward completed the attempt and requested Gus review.
8. Gus published signed PSTF audit evidence; relay-backed quality projected
   `passing`.
9. Netward's unauthorized close was rejected with `reviewer_required`;
   canonical state remained unchanged.
10. Gus's authorized close passed reviewer and quality gates.
11. Fresh Stew, Netward, and Gus list projections returned the same revision and
    normalized task digest.
12. Nostrig was restarted with its journal/outbox, and relay 3 was stopped and
    restarted mid-flow while two-relay quorum continued.
13. After another Nostrig restart and cursor rewind, every one of the 14 original
    signed ephemeral commands was republished. Subscribe-before-replay observers
    received the identical durable cached response event for every command.
14. Final canonical state, history, queue, responses, journal, authorization
    audit, and evidence were verified as correct and non-duplicated.

Final PASS line:

```text
PASS nostrig-crm task=nostrig-crm-1784693566459503000 revision=7b55497798424c8e8a54c255e32e93c0346576d251196fc1b11127bd44ca8693 digest=eb16093dcf56690af6847288984a5f9413bd541b4157b2fa66564dd113b07b50 commands=14 responses=14 audit=14 denied=1 checkpoints=5 queue_members=1 quality_event=13f95b431ae9082ae7ccf4b56dadcf040f11eef2f4112f63825ff1af134e83aa
```

The complete console transcript is written locally to
`build/nostrig-crm-acceptance.log` on each run; `build/` is intentionally
ignored because IDs and temporary paths are regenerated.

## Quality gates

After the final live run, `make test-full` passed: unit/integration tests, `go vet`,
race tests, both bounded ContextVM fuzz targets, the three-agent contract test,
and acceptance smoke tests were all green.

## Defects exposed and fixed

The live exercise found and fixed small underlying issues rather than weakening
the scenario:

- an already assigned worker could not claim its own dispatched task;
- dispatch-attempt state could not transition from `dispatched` to `running`;
- identical multi-relay command deliveries could publish duplicate responses;
- post-restart replay needed per-process relay fan-in deduplication while still
  returning the durable cached response;
- quality projection relied on a non-standard multi-character `#project`
  relay filter instead of trusted-author/kind retrieval plus local project
  validation.

The test also uses bounded relay-consistency barriers and deterministic latest
replaceable-event selection, matching production projection semantics.
