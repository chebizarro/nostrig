# Deploying `nostrig serve`

`nostrig serve` consumes ContextVM kind `25910` task intents addressed to its signer and materializes canonical kind `30900` task state. Production mode requires Signet/NIP-46 bunker signing and rejects raw Nostr private keys.

## Fleet placement

Run one active instance on **edge-01**. It is the preferred Cascadia placement because it is an always-on edge service near the fleet relay. Keep **max** as the documented recovery target, but do not run both against the same recipient identity unless duplicate command handling has been explicitly accepted.

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

docker compose config
docker compose up -d --build
docker compose ps
```

`docker-compose.yml` sets `NOSTRIG_ENV=production`, uses `restart: unless-stopped`, mounts the client key as a Compose secret, and checks the freshness of `/tmp/nostrig/healthy`. Multiple repository addresses may be comma-separated in `NOSTRIG_REPO_ADDRS`.

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

The unit defaults `NOSTR_RELAY` to the fleet relay, loads the client key with systemd credentials, restarts on failure, and writes liveness to `/run/nostrig-fleet/healthy`.

```bash
systemctl status nostrig-serve@fleet
journalctl -u nostrig-serve@fleet -f
find /run/nostrig-fleet/healthy -mmin -2 -type f
```

## Bahia service registration

Register the chosen Compose project or `nostrig-serve@fleet` unit in Bahia as a managed service. The service record should identify edge-01 as primary, max as the recovery target, the relay and repository selectors as non-secret configuration, the Signet credential owner, the restart action, and the liveness-file check. Bahia should reference the host-managed secret; it must not store or render the client secret or a raw Nostr private key. No Bahia code change is required for this deployment.

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
