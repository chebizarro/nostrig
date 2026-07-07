package nostr

import cascadia "git.sharegap.net/cascadia/cascadia-go"

// Canonical Cascadia task-fabric kinds come from generated cascadia-go bindings.
const (
	// KindTaskState is the fleet-canonical, addressable task projection. Its d tag
	// is always task:<beads-id>.
	KindTaskState = cascadia.CAS_CP_STATE
	// KindIntent is the fleet ContextVM JSON-RPC command transport.
	KindIntent = cascadia.CAS_INTENT
	// KindNIP51Set is the NIP-51 general-purpose set used for queues and epics.
	KindNIP51Set = cascadia.NIP51_TASK_COLLECTION

	// Aliases used by the task fabric publisher.
	KindCanonicalState  = KindTaskState
	KindContextVMIntent = KindIntent
	KindNamedList       = KindNIP51Set

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
