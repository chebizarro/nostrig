# Nostrig live acceptance harness

This directory contains two deliberately separate artifacts:

- `contracts/three-agent-v1.json` is the executable contract scaffold for bead
  `nostrig-crm`. Its untagged test validates the required fourteen-step ceremony,
  but does **not** claim that the live ceremony ran.
- `compose.yaml` plus `live_test.go` is a manual, skipped-by-default harness
  for three disposable relays and a real Signet/NIP-46 signer.

The compose file builds Signet from the sibling `nostrc` checkout. Override
`SIGNET_SOURCE_CONTEXT` when it is elsewhere. The relay image is version-pinned;
for a release ceremony, additionally pin the image digest recorded by the
operator.

## Prepare disposable infrastructure

Generate throwaway secrets; never reuse production custody material:

```sh
export SIGNET_DB_KEY="$(openssl rand -hex 32)"
export SIGNET_BUNKER_NSEC="<throwaway nsec>"
export SIGNET_PROVISIONER_PUBKEYS="<throwaway provisioner hex pubkey>"
docker compose -f test/acceptance/compose.yaml up -d --build
```

Use Signet's documented ContextVM provisioning flow to adopt or provision a
throwaway Nostrig identity, grant kinds 1 and 30900 plus signing capability, and
capture its `bunker://` URI. The live ceremony remains operator-controlled
because provisioning is a security boundary, not test fixture setup.

## Run live relay and signer checks

```sh
export NOSTRIG_ACCEPTANCE_RELAYS="ws://127.0.0.1:17001,ws://127.0.0.1:17002,ws://127.0.0.1:17003"
export NOSTRIG_ACCEPTANCE_BUNKER_URL="<provisioned bunker:// URI>"
export NOSTRIG_ACCEPTANCE_CLIENT_SECRET="<stable 64-character NIP-46 client secret>"
export NOSTRIG_ACCEPTANCE_COMPOSE_FILE="$PWD/test/acceptance/compose.yaml"

make acceptance-live
```

This runs real two- and three-relay publication plus a partial-relay-failure
case. Signet outage control is opt-in because it stops a compose service:

```sh
export NOSTRIG_ACCEPTANCE_CONTROL_SIGNET=1
make acceptance-live
```

The reconnect check signs before the outage, requires signing to fail while
Signet is stopped, restarts Signet, reconnects with the same client identity,
and signs again. `make acceptance-smoke` only compiles the tagged harness and
skips when these variables are absent; CI does not report that skip as a live
acceptance pass.

## Run the unattended final three-agent ceremony

The final acceptance owns a clean three-relay lifecycle and uses four
throwaway, in-memory local test signers: one canonical Nostrig service identity
plus Stew, Netward, and Gus. It runs the real service loop, relay ledger,
two-of-three reliable publisher/outbox, durable command journal, ACL/audit
policy, and trusted PSTF relay projection. It intentionally does not start
Signet; the separate live checks above retain Signet coverage.

```sh
make acceptance-three-agent
```

The target starts only `relay-1`, `relay-2`, and `relay-3`, restarts the
service in-process, stops and restarts `relay-3` mid-flow, rewinds the durable
journal cursor to replay every original signed command, and removes the
containers and volumes on exit. Verbose evidence is retained at
`build/nostrig-crm-acceptance.log` (override with
`NOSTRIG_ACCEPTANCE_LOG`).

Cleanup for the manually managed Signet harness:

```sh
docker compose -f test/acceptance/compose.yaml down -v
```
