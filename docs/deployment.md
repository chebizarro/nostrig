# Deploying `nostrig serve`

`nostrig serve` consumes ContextVM kind `25910` task intents addressed to its signer and materializes canonical kind `30900` task state. Production mode requires Signet/NIP-46 bunker signing and rejects raw Nostr private keys.

## Fleet placement

Run one active instance on **edge-01**. It is the preferred Cascadia placement because it is an always-on edge service near the fleet relay. Keep **max** as the cold recovery target. Never run both against the same recipient identity; the failover procedure below requires fencing the old primary first.

The default relay is `wss://relay.sharegap.net`. Scope each instance to one or more canonical repository addresses (`30617:<owner-pubkey>:<repo-id>`). With selectors configured, task creation must name an allowed `repo_addr`; later task mutations are accepted only when the existing `30900` state belongs to an allowed repository.

## Signet provisioning

Use `signetctl` and the Signet operations documentation to provision a dedicated `nostrig-serve` identity, authorize its NIP-46 operations, and obtain its bunker URL. This runbook intentionally does not duplicate the Signet enrollment procedure.

Store only these deployment values:

- `NOSTRIG_SIGNER_BUNKER_URL`: the provisioned `bunker://...` URL.
- A stable NIP-46 **client** secret key, either in `NOSTRIG_SIGNER_CLIENT_SECRET_KEY` or in a file named by `NOSTRIG_SIGNER_CLIENT_SECRET_KEY_FILE`.

Never provision `NOSTR_PRIVATE_KEY` in production. `NOSTRIG_ENV=production` rejects it.

## Docker Compose

Create the configuration and client-secret file on edge-01:

```bash
cat > .env <<'EOF'
NOSTR_RELAY=wss://relay.sharegap.net
NOSTRIG_REPO_ADDRS=30617:<owner-hex>:<repo-id>
NOSTRIG_SIGNER_BUNKER_URL=bunker://<signer-pubkey>?relay=wss://relay.sharegap.net
NOSTRIG_SIGNER_CLIENT_SECRET_FILE=./secrets/nostrig_signer_client_secret_key
EOF

install -d -m 0700 secrets
umask 077
printf '%s\n' '<nip46-client-secret-hex>' > secrets/nostrig_signer_client_secret_key
sudo chown 65532:65532 secrets/nostrig_signer_client_secret_key
sudo chmod 0400 secrets/nostrig_signer_client_secret_key

docker compose config
docker compose up -d --build
docker compose ps
```

`docker-compose.yml` sets `NOSTRIG_ENV=production`, runs as UID/GID 65532 with no capabilities and a read-only root filesystem, mounts the client key mode `0400` as a Compose secret, persists state and the instance lock in the `nostrig-state` volume, and checks the freshness of the tmpfs-backed `/tmp/nostrig/healthy`. Multiple repository addresses may be comma-separated in `NOSTRIG_REPO_ADDRS`.

Useful checks:

```bash
docker compose logs -f nostrig-serve
docker compose exec nostrig-serve sh -c 'cat /tmp/nostrig/healthy'
```

## systemd template (bare metal)

Install the binary and template, then create an instance named `fleet`:

```bash
sudo install -m 0755 ./nostrig /usr/local/bin/nostrig
sudo install -m 0644 deploy/systemd/nostrig-serve@.service /etc/systemd/system/nostrig-serve@.service
sudo install -d -m 0750 /etc/nostrig
sudo sh -c 'cat > /etc/nostrig/fleet.env' <<'EOF'
NOSTRIG_REPO_ADDRS=30617:<owner-hex>:<repo-id>
NOSTRIG_SIGNER_BUNKER_URL=bunker://<signer-pubkey>?relay=wss://relay.sharegap.net
EOF
sudo chmod 0600 /etc/nostrig/fleet.env
sudo install -m 0600 /path/from/signet/nostrig-client-secret /etc/nostrig/fleet.signer-client-secret
sudo systemctl daemon-reload
sudo systemctl enable --now nostrig-serve@fleet
```

The unit defaults `NOSTR_RELAY` to the fleet relay, loads the client key with systemd credentials, restarts on failure, persists the signed-event outbox in `/var/lib/nostrig-fleet/outbox.json`, and writes liveness to `/run/nostrig-fleet/healthy`.

```bash
systemctl status nostrig-serve@fleet
journalctl -u nostrig-serve@fleet -f
find /run/nostrig-fleet/healthy -mmin -2 -type f
```

## Relay quorum and outbox recovery

Use `--ledger-relay` for authoritative relays and `--mirror-relay` for optional copies. `--relay-ack-quorum` counts only ledger-relay acknowledgements; when it is zero, every configured ledger relay is required. Mirror failures never turn an otherwise quorate write into a command failure, but the signed event remains queued until every target acknowledges it or reaches `--relay-retry-attempts`.

The outbox drains immediately when `nostrig serve` restarts. Each retry republishes the original signed event ID and skips relays that already acknowledged it. Inspect pending and dead-lettered entries, then return selected or all dead letters to the retry queue with:

```bash
nostrig outbox list --path /var/lib/nostrig-fleet/outbox.json
nostrig outbox dlq --path /var/lib/nostrig-fleet/outbox.json
nostrig outbox retry --path /var/lib/nostrig-fleet/outbox.json <event-id>
nostrig outbox retry --path /var/lib/nostrig-fleet/outbox.json # retry all DLQ entries
```

The service publishes each relay attempt with a bounded timeout, exponential backoff with jitter, and a per-relay circuit breaker. Configure these with the `serve --help` retry, timeout, and circuit flags.

## Image promotion and verification

CI builds an OCI image with SBOM and provenance, scans both the archive and the pushed digest for unfixed high/critical vulnerabilities, and only publishes from `master`. Published images use the commit SHA tag and are signed keylessly with GitHub OIDC:

```text
ghcr.io/chebizarro/nostrig:<git-sha>@sha256:<digest>
```

Deploy by digest, not by a mutable tag. Before promotion, verify the signature and attestations against this repository's workflow identity:

```bash
IMAGE=ghcr.io/chebizarro/nostrig@sha256:<digest>
cosign verify \
  --certificate-identity-regexp '^https://github.com/chebizarro/nostrig/.github/workflows/ci.yml@refs/heads/master$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "$IMAGE"
cosign verify-attestation --type spdxjson \
  --certificate-identity-regexp '^https://github.com/chebizarro/nostrig/.github/workflows/ci.yml@refs/heads/master$' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  "$IMAGE"
```

## Graceful shutdown and the single-active guard

The CLI handles `SIGINT` and `SIGTERM` through its root context. Cancellation stops the relay subscription, removes the liveness file, closes relay pools, and leaves any unacknowledged signed event in the durable outbox. Compose and systemd allow 30 seconds before forcing termination. Always use `docker compose stop` or `systemctl stop`; do not send `SIGKILL` during normal operations.

Production launchers pass `--instance-lock` inside the persistent state directory. The process takes a non-blocking exclusive lock before connecting to Signet, so a second process sharing that state directory fails closed. This prevents duplicate local instances and accidental Compose/systemd overlap on one host.

The lock is not a cross-host lease. Until the compare-and-set work provides a safe distributed leader/claim primitive, enforce **one enabled host per Signet identity**: edge-01 is enabled and max is stopped/disabled. Never start max until edge-01 is confirmed stopped and its subscription drained. Full automatic leader election remains deferred to the CAS work.

## State inventory, backup, and restore

Keep all host state under one protected state directory. The current server writes `outbox.json`; command-journal and cursor files should use `command-journal.json` and `cursor.json` when those stores are enabled. A sync cache should be placed at `task-cache.jsonl` (pass `nostrig sync --cache <state-dir>/task-cache.jsonl`). Back up the whole directory so journal, cursor, cache, outbox, and instance-lock metadata cannot be accidentally split across generations. The lock file itself has no recovery value.

Create a cold Compose backup on the active host:

```bash
set -eu
CONTAINER=$(docker compose ps -q nostrig-serve)
STATE=$(docker inspect --format '{{range .Mounts}}{{if eq .Destination "/var/lib/nostrig"}}{{.Source}}{{end}}{{end}}' "$CONTAINER")
docker compose stop -t 30 nostrig-serve
sudo tar --numeric-owner -C "$STATE" \
  --exclude=instance.lock -czf "nostrig-state-$(date -u +%Y%m%dT%H%M%SZ).tgz" .
sha256sum nostrig-state-*.tgz
docker compose up -d nostrig-serve
```

For systemd, stop `nostrig-serve@fleet`, archive `/var/lib/nostrig-fleet` with the same `tar --numeric-owner` pattern, hash the archive, and restart the unit. Store backups encrypted with the fleet backup service; the state files contain task data but must never contain the Signet client secret. Back up after deployment changes and at least daily. Retain the last seven daily and four weekly archives.

Restore only while the target is stopped:

```bash
# Compose: derive STATE as above before stopping.
docker compose stop -t 30 nostrig-serve
sudo find "$STATE" -mindepth 1 -maxdepth 1 -exec rm -rf -- {} +
sudo tar --numeric-owner -C "$STATE" -xzf nostrig-state-<timestamp>.tgz
sudo chown -R 65532:65532 "$STATE"
sudo chmod 0700 "$STATE"
sudo find "$STATE" -type f -exec chmod 0600 {} +
test ! -e "$STATE/instance.lock" || sudo rm -f "$STATE/instance.lock"
nostrig outbox list --path "$STATE/outbox.json"
docker compose up -d nostrig-serve
```

For systemd use `/var/lib/nostrig-fleet`, preserve root ownership (the unit's `StateDirectory` ACL/ownership is managed by systemd), then start `nostrig-serve@fleet`. Validate JSON files with `jq empty`, validate each cache JSONL record with `jq -e .`, inspect the outbox, and confirm relay state before accepting commands. If a journal or cursor is absent because that store has not yet been enabled, record that fact in the drill log rather than creating an empty placeholder.

## edge-01 primary and max recovery

Normal state:

- edge-01: `nostrig-serve@fleet` enabled and active, owns the production Signet identity.
- max: identical binary digest and non-secret configuration installed, unit disabled and stopped.
- Both hosts: root-owned `/etc/nostrig/fleet.env` mode `0600`; independently provisioned copy of the same Signet client credential at `/etc/nostrig/fleet.signer-client-secret` mode `0600`.
- Backup archive and its SHA-256 digest available to both hosts through encrypted fleet backup storage.

Planned failover to max:

1. Stop edge-01 with the 30-second grace period and confirm the process is gone.
2. Confirm edge-01 no longer has a relay subscription and record the last handled event/cursor and outbox status.
3. Take and hash a final cold backup; transfer it over the approved encrypted channel.
4. On max, verify the deployed image signature/digest, restore state, validate all state files, and confirm the unit is disabled before the restore.
5. Start (do not yet enable) `nostrig-serve@fleet` on max. Confirm the local instance lock is held, Signet connects, the outbox drains, and expected relay state is readable.
6. Exercise one reversible canary command. If successful, enable the max unit and record max as temporary primary in Bahia.
7. Keep edge-01 stopped until failback completes.

Unplanned edge-01 loss follows the same sequence using the newest verified backup. Before starting max, fence edge-01 at the host/power or network layer so it cannot return and reconnect with the same identity. If fencing cannot be proven, do not start max.

Fail back by stopping and backing up max, fencing max, restoring the final archive to edge-01, verifying the identical image digest/configuration, starting edge-01, executing the canary, updating Bahia, and only then disabling max again.

## Restore and failover drills

Run a restore drill monthly and after any state-schema change:

1. Restore the latest backup into an isolated disposable directory or host with relay publishing blocked.
2. Verify the archive checksum, ownership/modes, JSON/JSONL parsing, outbox inspection, and expected journal/cursor/cache counts.
3. Start with a disposable identity and test relay, confirm recovery, then destroy the drill state.
4. Record backup ID, image digest, start/end time, checks performed, RTO, data-loss window, and follow-up issues.

Run a controlled edge-01 to max failover and failback quarterly using the procedure above. The pass criteria are: only one production identity connection at every point, forced duplicate start rejected by the instance lock, no corrupt state, outbox eventually empty or explicitly accounted for, one successful canary on each promoted host, and Bahia reflecting the active host. Abort and fence both instances if identity overlap is observed.

## Bahia service registration

Register with Bahia's signer-first ContextVM convention. `deploy/bahia/service-create.json` is the JSON-RPC content template; publish it as kind `25910`, addressed to the Bahia service pubkey, with `method=service/create`, `service=nostrig`, and an idempotent `d=service-create:nostrig` tag. Require relay `OK` and the correlated success result, then verify the service through Bahia's read model.

After creation, record the edge-01 Compose project or `nostrig-serve@fleet` unit as the active runtime and max as the cold recovery target. Runtime metadata must include the immutable image digest, relay/repository selectors, restart/stop commands, state directory, liveness-file check, and Signet credential owner. Bahia may reference `/etc/nostrig/fleet.signer-client-secret` as host-managed metadata but must never ingest or render its value.

## Local smoke test

Use a disposable local Nostr relay (the commands below assume `ws://127.0.0.1:7777`), two disposable development keys, and a Nostr inspection CLI such as `nak`. Raw keys are permitted only because `NOSTRIG_ENV` is not production.

```bash
export RELAY=ws://127.0.0.1:7777
export SERVER_SK=<64-character-hex-development-key>
export CLIENT_SK=<different-64-character-hex-development-key>
export SERVER_PK=$(nak key public "$SERVER_SK")

go build -o ./nostrig ./cmd/nostrig
NOSTR_RELAY="$RELAY" NOSTR_PRIVATE_KEY="$SERVER_SK" \
  ./nostrig serve --repo-addr 30617:local-owner:smoke --health-file /tmp/nostrig-smoke.health &
SERVE_PID=$!

NOSTR_RELAY="$RELAY" NOSTR_PRIVATE_KEY="$CLIENT_SK" ./nostrig create \
  --task-id smoke-1 --title 'Smoke task' \
  --repo-addr 30617:local-owner:smoke --recipient "$SERVER_PK"
NOSTR_RELAY="$RELAY" NOSTR_PRIVATE_KEY="$CLIENT_SK" ./nostrig update \
  --task-id smoke-1 --status in_progress --priority P1 --recipient "$SERVER_PK"

nak req -k 30900 -d task:smoke-1 "$RELAY"
kill "$SERVE_PID"
```

The returned `30900` event should have `d=task:smoke-1` and show the updated status/priority in its task payload. The in-process materialization assertion is also covered by:

```bash
go test ./internal/taskfabric -run TestTaskCreateUpdateDeleteIntentRoundTrip -v
```
