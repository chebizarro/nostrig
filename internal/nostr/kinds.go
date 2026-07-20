package nostr

// NIP-34 event kinds (git stuff)
const (
	// KindTaskState is the fleet-canonical, addressable task projection. Its d tag
	// is always task:<beads-id>.
	KindTaskState = 30900
	// KindNIP51Set is the NIP-51 general-purpose set used for queues and epics.
	KindNIP51Set = 30000

	KindRepositoryAnnouncement = 30617
	KindRepositoryState        = 30618

	KindPatch         = 1617
	KindPullRequest   = 1618
	KindPRUpdate      = 1619
	KindIssue         = 1621
	KindStatusOpen    = 1630
	KindStatusApplied = 1631
	KindStatusClosed  = 1632
	KindStatusDraft   = 1633
)
