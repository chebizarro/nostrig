#!/bin/sh
set -eu

repo_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$repo_root"

command -v docker >/dev/null
command -v jq >/dev/null

jq -e '
  .jsonrpc == "2.0" and
  .method == "service/create" and
  .params.name == "nostrig" and
  .params.repo_url == "https://git.sharegap.net/cascadia/nostrig" and
  .params.runtime_type == "compose"
' deploy/bahia/service-create.json >/dev/null

NOSTRIG_REPO_ADDRS='30617:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:nostrig' \
NOSTRIG_QUALITY_AUTHORS='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' \
NOSTRIG_QUALITY_PROJECT='nostrig' \
NOSTRIG_SIGNER_BUNKER_URL='bunker://cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc?relay=wss://relay.example' \
NOSTRIG_SIGNER_CLIENT_SECRET_FILE='/dev/null' \
NOSTRIG_ACL_FILE='./config/nostrig-acl.example.json' \
  docker compose config --quiet

NOSTRIG_REPO_ADDRS='30617:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:nostrig' \
NOSTRIG_QUALITY_AUTHORS='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' \
NOSTRIG_QUALITY_PROJECT='nostrig' \
NOSTRIG_SIGNER_BUNKER_URL='bunker://cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc?relay=wss://relay.example' \
NOSTRIG_SIGNER_CLIENT_SECRET_FILE='/dev/null' \
NOSTRIG_ACL_FILE='./config/nostrig-acl.example.json' \
  docker compose config | grep -Fq 'mode: "0400"'

if [ "${NOSTRIG_PREFLIGHT_CONTAINER_READABILITY:-0}" = "1" ]; then
  NOSTRIG_REPO_ADDRS='30617:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa:nostrig' \
  NOSTRIG_QUALITY_AUTHORS='bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb' \
  NOSTRIG_QUALITY_PROJECT='nostrig' \
  NOSTRIG_SIGNER_BUNKER_URL='bunker://cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc?relay=wss://relay.example' \
  NOSTRIG_SIGNER_CLIENT_SECRET_FILE='/dev/null' \
  NOSTRIG_ACL_FILE="${NOSTRIG_ACL_FILE:-./config/nostrig-acl.example.json}" \
    docker compose run --rm --no-deps --entrypoint /bin/sh nostrig-serve -c 'test "$(id -u):$(id -g)" = "65532:65532" && test -r "$NOSTRIG_ACL_FILE"'
fi

grep -Fq 'Environment=NOSTRIG_ACL_FILE=%d/nostrig_acl' deploy/systemd/nostrig-serve@.service
grep -Fq 'LoadCredential=nostrig_acl:/etc/nostrig/%i.acl.json' deploy/systemd/nostrig-serve@.service

printf '%s\n' 'deployment manifests: valid'
