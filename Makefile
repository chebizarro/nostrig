BINARY_NAME := nostrig
CMD_DIR := ./cmd/nostrig
BIN_DIR := ./bin

PROTO_DIR := ./proto
GEN_DIR := ./gen

PROTOC ?= protoc

.PHONY: proto build install clean

proto:
	@mkdir -p $(GEN_DIR)
	@$(PROTOC) -I $(PROTO_DIR) \
		--go_out=$(GEN_DIR) --go_opt=paths=source_relative \
		$(PROTO_DIR)/beads.proto

build:
	@mkdir -p $(BIN_DIR)
	@go build -o $(BIN_DIR)/$(BINARY_NAME) $(CMD_DIR)

install:
	@go install $(CMD_DIR)

clean:
	@rm -rf $(BIN_DIR)