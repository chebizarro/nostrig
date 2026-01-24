package nostr

// NIP-34 event kinds (git stuff)
const (
	KindRepositoryAnnouncement = 30617
	KindRepositoryState        = 30618

	KindPatch        = 1617
	KindPullRequest  = 1618
	KindPRUpdate     = 1619
	KindIssue        = 1621
	KindStatusOpen   = 1630
	KindStatusApplied = 1631
	KindStatusClosed = 1632
	KindStatusDraft  = 1633
)