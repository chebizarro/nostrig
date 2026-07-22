BINARY_NAME := nostrig
CMD_DIR := ./cmd/nostrig
BIN_DIR := ./bin
BUILD_DIR ?= ./build

PROTO_DIR := ./proto
GEN_DIR := ./gen

PROTOC ?= protoc
GO ?= go
GO_TOOLCHAIN ?= go1.25.12
GO_CMD := GOTOOLCHAIN=$(GO_TOOLCHAIN) $(GO)
FUZZ_SMOKE_COUNT ?= 100x

IMAGE_NAME ?= nostrig:local
OCI_PLATFORM ?= linux/amd64
OCI_ARCHIVE ?= $(BUILD_DIR)/nostrig.oci.tar
IMAGE_METADATA ?= $(BUILD_DIR)/image-metadata.json
IMAGE_DIGEST ?= $(BUILD_DIR)/image-digest.txt
SBOM_GENERATOR ?= docker/buildkit-syft-scanner:stable-1@sha256:79e7b013cbec16bbb436f312819a49a4a57752b2270c1a9332ae1a10fcc82a68

.PHONY: proto build install test vet race fuzz-smoke acceptance-contract acceptance-smoke acceptance-live acceptance-three-agent deployment-check test-full check image release-image sbom clean

proto:
	$(GO_CMD) generate ./...

build:
	@mkdir -p $(BIN_DIR)
	$(GO_CMD) build -mod=readonly -trimpath -o $(BIN_DIR)/$(BINARY_NAME) $(CMD_DIR)

install:
	$(GO_CMD) install -mod=readonly $(CMD_DIR)

test:
	$(GO_CMD) test -mod=readonly ./...

vet:
	$(GO_CMD) vet -mod=readonly ./...

race:
	# fiatjaf.com/nostr's pinned optimized serializer trips checkptr under -race.
	# Keep race instrumentation enabled while disabling only that incompatible check.
	$(GO_CMD) test -mod=readonly -race -gcflags=all=-d=checkptr=0 ./...

fuzz-smoke:
	$(GO_CMD) test -mod=readonly -run='^$$' -fuzz='^FuzzContextVMIntentEventJSON$$' -fuzztime=$(FUZZ_SMOKE_COUNT) -parallel=1 ./internal/taskfabric
	$(GO_CMD) test -mod=readonly -run='^$$' -fuzz='^FuzzContextVMResponseEventJSON$$' -fuzztime=$(FUZZ_SMOKE_COUNT) -parallel=1 ./internal/taskfabric

acceptance-contract:
	$(GO_CMD) test -mod=readonly -run='^TestThreeAgentAcceptanceContract$$' ./test/acceptance

# Compile and execute the tagged package; live tests skip unless their explicit
# environment is present. This is not a live acceptance result.
acceptance-smoke:
	$(GO_CMD) test -mod=readonly -tags=nostrig_acceptance -run='^TestLive' ./test/acceptance

acceptance-live:
	@test -n "$$NOSTRIG_ACCEPTANCE_RELAYS" || (echo "NOSTRIG_ACCEPTANCE_RELAYS is required" >&2; exit 2)
	@test -n "$$NOSTRIG_ACCEPTANCE_BUNKER_URL" || (echo "NOSTRIG_ACCEPTANCE_BUNKER_URL is required" >&2; exit 2)
	@test -n "$$NOSTRIG_ACCEPTANCE_CLIENT_SECRET" || (echo "NOSTRIG_ACCEPTANCE_CLIENT_SECRET is required" >&2; exit 2)
	$(GO_CMD) test -mod=readonly -tags=nostrig_acceptance -run='^TestLive' -v ./test/acceptance

deployment-check:
	./scripts/validate-deployment.sh

# Owns a clean three-relay lifecycle and captures the verbose ceremony log.
# Signet is intentionally not started: all four identities are ephemeral local
# test signers, which keeps the final scenario unattended and disposable.
acceptance-three-agent:
	@set -eu; \
	compose="$${NOSTRIG_ACCEPTANCE_COMPOSE_FILE:-$(CURDIR)/test/acceptance/compose.yaml}"; \
	log="$${NOSTRIG_ACCEPTANCE_LOG:-$(CURDIR)/build/nostrig-crm-acceptance.log}"; \
	export SIGNET_DB_KEY="$${SIGNET_DB_KEY:-0000000000000000000000000000000000000000000000000000000000000000}"; \
	export SIGNET_BUNKER_NSEC="$${SIGNET_BUNKER_NSEC:-unused-by-three-agent-acceptance}"; \
	mkdir -p "$$(dirname "$$log")"; \
	docker compose -f "$$compose" down -v --remove-orphans >/dev/null 2>&1 || true; \
	cleanup() { docker compose -f "$$compose" down -v --remove-orphans >/dev/null 2>&1 || true; }; \
	trap cleanup EXIT INT TERM; \
	docker compose -f "$$compose" up -d relay-1 relay-2 relay-3; \
	if NOSTRIG_ACCEPTANCE_CONTROL_DOCKER=1 \
		NOSTRIG_ACCEPTANCE_RELAYS="ws://127.0.0.1:17001,ws://127.0.0.1:17002,ws://127.0.0.1:17003" \
		NOSTRIG_ACCEPTANCE_COMPOSE_FILE="$$compose" \
		$(GO_CMD) test -mod=readonly -count=1 -tags=nostrig_acceptance \
			-run='^TestFinalThreeAgentLiveAcceptance$$' -v ./test/acceptance >"$$log" 2>&1; then \
		cat "$$log"; \
	else \
		status=$$?; cat "$$log"; exit $$status; \
	fi

check: test vet race

test-full: check fuzz-smoke acceptance-contract acceptance-smoke deployment-check

image:
	docker build --pull --tag $(IMAGE_NAME) .

# Produces an OCI archive with BuildKit SBOM and provenance attestations. The
# digest file is the immutable OCI manifest digest recorded by BuildKit.
release-image:
	@mkdir -p $(BUILD_DIR)
	docker buildx build --pull \
		--platform $(OCI_PLATFORM) \
		--tag $(IMAGE_NAME) \
		--sbom=generator=$(SBOM_GENERATOR) \
		--provenance=mode=max \
		--output type=oci,dest=$(OCI_ARCHIVE) \
		--metadata-file $(IMAGE_METADATA) \
		.
	@grep -Eo '"containerimage.digest"[[:space:]]*:[[:space:]]*"sha256:[0-9a-f]{64}"' $(IMAGE_METADATA) \
		| head -n 1 | cut -d '"' -f 4 > $(IMAGE_DIGEST)
	@test -s $(IMAGE_DIGEST)
	@printf 'image digest: %s\n' "$$(cat $(IMAGE_DIGEST))"
	@printf 'OCI archive (includes SBOM): %s\n' "$(OCI_ARCHIVE)"

sbom: release-image

clean:
	@rm -rf $(BIN_DIR) $(BUILD_DIR)
