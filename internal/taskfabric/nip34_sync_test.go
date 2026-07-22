package taskfabric

import (
	"context"
	"strings"
	"testing"

	gonostr "fiatjaf.com/nostr"
	pb "github.com/chebizarro/nostrig/gen/beads"
	nip34 "github.com/chebizarro/nostrig/internal/nostr"
)

func TestNIP34WritebackRejectsNonMaintainerCanonicalAuthor(t *testing.T) {
	owner, attacker := gonostr.Generate(), gonostr.Generate()
	repoAddr := nip34.RepoAddress(owner.Public().Hex(), "repo")
	ledger := &RelayLedger{
		CanonicalAuthor: attacker.Public().Hex(),
		ResolveRepoAnnouncement: func(context.Context, *nip34.Client, []string, string) (*nip34.RepoAnnouncement, error) {
			return &nip34.RepoAnnouncement{PubKey: owner.Public().Hex(), RepoID: "repo"}, nil
		},
	}
	issue := &pb.Issue{Id: "task-1", Metadata: &pb.Metadata{Custom: map[string]string{
		"nostr.id":        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		"nip34.repo_addr": repoAddr,
	}}}
	err := ledger.authorizeNIP34Writeback(context.Background(), issue)
	if err == nil || !strings.Contains(err.Error(), "not a trusted maintainer") {
		t.Fatalf("expected maintainer rejection, got %v", err)
	}
	ledger.CanonicalAuthor = owner.Public().Hex()
	if err := ledger.authorizeNIP34Writeback(context.Background(), issue); err != nil {
		t.Fatalf("owner was not trusted: %v", err)
	}
}
