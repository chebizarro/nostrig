# Production test strategy

This document audits the engineering brief's required production coverage. It
separates deterministic component evidence from the manual live acceptance
harness so a skipped ceremony is never reported as a pass.

## Commands

| Command | Coverage |
| --- | --- |
| `make test` | Default unit, component, hostile-corpus, contract, replay, restart, and pagination tests |
| `make race` | Entire Go suite with the race detector |
| `make fuzz-smoke` | Both native Go fuzz targets, one at a time, for a bounded checked-in-corpus smoke run |
| `make acceptance-contract` | Validates the twelve-step three-agent scenario contract |
| `make acceptance-smoke` | Compiles the tagged live harness; live cases skip without explicit environment |
| `make test-full` | Test, vet, race, fuzz smoke, contract, and tagged-harness compile/skip |
| `make acceptance-live` | Operator-triggered disposable-relay and Signet ceremony; required environment must be present |

### Race/checkptr compatibility

The pinned `fiatjaf.com/nostr` optimized serializer is not checkptr-compatible
when the race runtime instruments it. Plain `go test -race ./...` can therefore
crash inside the dependency rather than report a Nostrig race. The supported
race command is:

```sh
go test -race -gcflags=all=-d=checkptr=0 ./...
```

This disables checkptr only; race instrumentation remains enabled across the
suite. `make race`, `make check`, and `make test-full` use this form.

## Requirement audit

| Brief requirement | Evidence |
| --- | --- |
| Every authorization matrix cell | `TestAuthorizationRoleMethodMatrixEveryCell` enumerates all 77 role/method cells independently. `TestAuthorizationMatrix` and the remaining authz tests cover field restrictions, repositories, audit failure, reviewer, and quality behavior. |
| Event and ContextVM property/fuzz tests | `FuzzContextVMIntentEventJSON` and `FuzzContextVMResponseEventJSON` are native Go fuzzers. `testdata/fuzz/` contains Go-native seed corpora; `testdata/contextvm/` is the checked-in malformed/hostile regression corpus. |
| Claim race | `TestAtomicClaimHundredWayRaceAndWinnerRetry`. |
| Update race | `TestAtomicUpdateHundredWayRace`. |
| Queue race | `TestQueueReservationHundredWayAcrossHandlers`. |
| Cache race | `TestConcurrentReadOnlyCacheMergeIsDeterministic` proves shared immutable projection inputs are deterministic and race-free. Cache files are single-owner snapshots; concurrent writers are not claimed or exercised. |
| Outbox race | `TestConcurrentOutboxPublishSnapshotAndRecovery` concurrently publishes, snapshots, and recovers the durable spool. |
| Deterministic replay | `TestReplayCreateUpdateDeleteHundredTimesMutatesOnce`, request-ID conflict tests, and `TestHistoricalBackfillCatchesUpExactlyOnce`. |
| Restart at mutation phases | `TestRestartAfterStateBeforeResponseRepairsCachedResponse`, `TestRestartAtEveryCommandPhase`, and `TestReliablePublisherRestartDrainsOnlyMissingRelay`. |
| Two-relay behavior | Existing reliable-publisher quorum, partial-failure, and restart tests use two required relays. |
| Three-relay behavior | `TestThreeRelayPublisherQuorumAndRecoveryMatrix` covers quorum with one failure, sub-quorum with two failures, durability, and recovery without retrying acknowledgements. |
| Partial relay failure | Existing mirror/sub-quorum tests plus the three-relay matrix; the tagged live harness repeats it against disposable WebSocket relays. |
| Signer disconnect/reconnect | `TestLiveSignetDisconnectReconnect` in the `nostrig_acceptance` build-tag lane stops Signet, proves outage failure, restarts, reconnects the same client identity, and signs again. It only runs with explicit outage-control permission. |
| Malformed and hostile events | `TestMalformedAndHostileContextVMEventCorpus` runs the checked-in corpus on every ordinary test invocation and proves unsigned hostile responses never authenticate. |
| Pagination above 500 tasks | `TestFetchManyPaginatedReturnsMoreThanFiveHundredEvents` and `TestRelayBackendLoadExportReturnsMoreThanFiveHundredTasks`. |
| Live disposable relay plus Signet | `test/acceptance/compose.yaml`, `live_test.go`, and its README provide the tagged/manual harness. CI compiles it but does not count a skip as a live pass. |
| Three-agent end to end | `contracts/three-agent-v1.json` records all twelve required milestones and `TestThreeAgentAcceptanceContract` validates the scaffold. The live ceremony belongs to bead `nostrig-crm` and is intentionally not run here. |
| Gastown round trips | `TestGastownTypedDependencyMutationRoundTripsCanonicalState`, `TestGastownContextVMDispatchCompletionAndPerTaskCloseGates`, and invalid-update/reassignment coverage. |

## Live harness boundary

The default and extended CI suites provide deterministic component evidence.
They do not claim real relay protocol or Signet availability. The live lane uses
three disposable relay containers and builds Signet from the sibling `nostrc`
source checkout. It requires throwaway custody keys and an explicitly provisioned
NIP-46 bunker URI. See `test/acceptance/README.md`.

The live tests cover actual two- and three-relay publication, partial relay
failure, and Signet disconnect/reconnect. The three-agent contract is only
scaffolding in this bead; `nostrig-crm` owns executing and recording that
ceremony.
