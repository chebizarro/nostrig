// Package cascadia contains the generated Cascadia Protocol bindings for Go —
// Nostr event-kind constants, tag helpers, the ContextVM 25910 method registry,
// and typed payload models — produced from the canonical CUE schemas in the
// cascadia-nips repository.
//
// This module is GENERATED. Do not edit the generated *.go files by hand. The
// repository is populated by cascadia-nips CI:
//
//	cascadia openapi            # CUE schemas -> OpenAPI 3.0
//	openapi-generator generate  # OpenAPI -> Go models  (+ thin templates for the
//	                            # kind/tag/method glue)
//
// and released as a tagged Go module:
//
//	git.sharegap.net/cascadia/cascadia-go@vX.Y.Z
//
// Consumers import it directly (no `replace`), with private-module resolution:
//
//	GOPRIVATE=git.sharegap.net/*   # + git auth (netrc/token) or the fleet Athens GOPROXY
//	go get git.sharegap.net/cascadia/cascadia-go@vX.Y.Z
package cascadia
