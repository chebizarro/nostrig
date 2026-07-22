#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
compose_file="$repo_root/test/acceptance/compose.yaml"
signet_source=${SIGNET_SOURCE_CONTEXT:?set SIGNET_SOURCE_CONTEXT to a disposable nostrc checkout}
nak_bin=${NAK:-nak}
health_port=${SIGNET_HEALTH_HOST_PORT:-18081}
work_dir=$(mktemp -d)

db_key=$(openssl rand -hex 32)
bunker_sk=$(openssl rand -hex 32)
bunker_nsec=$($nak_bin encode nsec "$bunker_sk")
bunker_pk=$($nak_bin key public "$bunker_sk")
provisioner_sk=$(openssl rand -hex 32)
provisioner_nsec=$($nak_bin encode nsec "$provisioner_sk")
provisioner_pk=$($nak_bin key public "$provisioner_sk")
client_secret=$(openssl rand -hex 32)

cleanup() {
  SIGNET_SOURCE_CONTEXT="$signet_source" \
  SIGNET_DB_KEY="$db_key" \
  SIGNET_BUNKER_NSEC="$bunker_nsec" \
  SIGNET_PROVISIONER_PUBKEYS="$provisioner_pk" \
  SIGNET_HEALTH_HOST_PORT="$health_port" \
    docker compose -f "$compose_file" down -v --remove-orphans >/dev/null 2>&1 || true
  rm -rf -- "$work_dir"
}
trap cleanup EXIT INT TERM

export SIGNET_SOURCE_CONTEXT="$signet_source"
export SIGNET_DB_KEY="$db_key"
export SIGNET_BUNKER_NSEC="$bunker_nsec"
export SIGNET_PROVISIONER_PUBKEYS="$provisioner_pk"
export SIGNET_HEALTH_HOST_PORT="$health_port"

docker compose -f "$compose_file" up -d --build
attempt=0
until curl -fsS "http://127.0.0.1:$health_port/health" >/dev/null 2>&1; do
  attempt=$((attempt + 1))
  if [ "$attempt" -ge 90 ]; then
    docker compose -f "$compose_file" logs --no-color signetd | tail -50
    exit 1
  fi
  sleep 2
done

# Use the daemon's own derived bunker pubkey. This avoids coupling the harness
# to a second implementation's secret-to-xonly-pubkey conversion.
bunker_pk=$(docker compose -f "$compose_file" logs --no-color signetd \
  | sed -n 's/.*p-tag=\([0-9a-f][0-9a-f]*\)).*/\1/p' | tail -n 1)
case "$bunker_pk" in
  [0-9a-f][0-9a-f]*) test "${#bunker_pk}" -eq 64 ;;
  *) printf '%s\n' 'could not resolve Signet bunker pubkey from startup log' >&2; exit 1 ;;
esac

# Signet intentionally ignores management events during its ten-second
# post-start replay-safety window. Provision only after that boundary.
sleep 11

if ! docker compose -f "$compose_file" exec -T \
  -e SIGNET_PROVISIONER_NSEC="$provisioner_nsec" \
  -e SIGNET_BUNKER_PUBKEY="$bunker_pk" \
  signetd signetctl -c /etc/signet/signet.conf provision nostrig-cutover \
  >"$work_dir/provision.out"; then
  docker compose -f "$compose_file" logs --no-color signetd | tail -80
  exit 1
fi

bunker_url=$(sed -n 's/^bunker_uri:[[:space:]]*//p' "$work_dir/provision.out" | head -n 1)
bunker_url=${bunker_url:-$(sed -n '/^{/p' "$work_dir/provision.out" \
  | tail -n 1 | jq -r '.result.bunker_uri // .bunker_uri // empty')}
test -n "$bunker_url"

# Provisioning creates custody state; NIP-46 remains fail-closed until an
# explicit identity policy is installed. Wildcard clients are acceptable only
# because every identity and volume in this target is disposable.
docker compose -f "$compose_file" exec -T \
  -e SIGNET_PROVISIONER_NSEC="$provisioner_nsec" \
  -e SIGNET_BUNKER_PUBKEY="$bunker_pk" \
  signetd signetctl -c /etc/signet/signet.conf set-policy nostrig-cutover \
  '{"default":"deny","allow_clients":["*"],"allow_methods":["connect","sign_event"],"allow_kinds":[1,30900]}' \
  >"$work_dir/policy.out"

bunker_url=$(printf '%s' "$bunker_url" | sed \
  -e 's#ws%3A%2F%2Frelay-1%3A8080#ws%3A%2F%2F127.0.0.1%3A17001#g' \
  -e 's#ws%3A%2F%2Frelay-2%3A8080#ws%3A%2F%2F127.0.0.1%3A17002#g' \
  -e 's#ws%3A%2F%2Frelay-3%3A8080#ws%3A%2F%2F127.0.0.1%3A17003#g' \
  -e 's#ws://relay-1:8080#ws://127.0.0.1:17001#g' \
  -e 's#ws://relay-2:8080#ws://127.0.0.1:17002#g' \
  -e 's#ws://relay-3:8080#ws://127.0.0.1:17003#g')

umask 077
{
  printf 'SIGNET_SOURCE_CONTEXT=/nostrc\n'
  printf 'SIGNET_DB_KEY=%s\n' "$db_key"
  printf 'SIGNET_BUNKER_NSEC=%s\n' "$bunker_nsec"
  printf 'SIGNET_PROVISIONER_PUBKEYS=%s\n' "$provisioner_pk"
  printf 'SIGNET_HEALTH_HOST_PORT=%s\n' "$health_port"
  printf 'NOSTRIG_ACCEPTANCE_RELAYS=ws://127.0.0.1:17001,ws://127.0.0.1:17002,ws://127.0.0.1:17003\n'
  printf 'NOSTRIG_ACCEPTANCE_BUNKER_URL=%s\n' "$bunker_url"
  printf 'NOSTRIG_ACCEPTANCE_CLIENT_SECRET=%s\n' "$client_secret"
  printf 'NOSTRIG_ACCEPTANCE_COMPOSE_FILE=/src/test/acceptance/compose.yaml\n'
  printf 'NOSTRIG_ACCEPTANCE_CONTROL_SIGNET=1\n'
} >"$work_dir/test.env"

docker run --rm --network host \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$repo_root:/src" \
  -v "$signet_source:/nostrc:ro" \
  -w /src \
  --env-file "$work_dir/test.env" \
  golang:1.25.12-alpine3.24 sh -c \
    'apk add --no-cache docker-cli-compose gcc musl-dev >/dev/null && timeout 5m env GOTOOLCHAIN=local go test -mod=readonly -count=1 -tags=nostrig_acceptance -run="^TestLive" -v ./test/acceptance'

printf '%s\n' 'disposable Signet acceptance: PASS'
