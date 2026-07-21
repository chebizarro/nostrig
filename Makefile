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

IMAGE_NAME ?= nostrig:local
OCI_PLATFORM ?= linux/amd64
OCI_ARCHIVE ?= $(BUILD_DIR)/nostrig.oci.tar
IMAGE_METADATA ?= $(BUILD_DIR)/image-metadata.json
IMAGE_DIGEST ?= $(BUILD_DIR)/image-digest.txt
SBOM_GENERATOR ?= docker/buildkit-syft-scanner:stable-1@sha256:79e7b013cbec16bbb436f312819a49a4a57752b2270c1a9332ae1a10fcc82a68

.PHONY: proto build install test vet race check image release-image sbom clean

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
	$(GO_CMD) test -mod=readonly -race ./...

check: test vet race

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