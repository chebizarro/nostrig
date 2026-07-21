package nostr

import cascadia "git.sharegap.net/cascadia/cascadia-go"

// Canonical Cascadia task-fabric kinds come from generated cascadia-go bindings.
const (
	KindContextVMIntent = cascadia.CAS_INTENT
	KindNamedList       = cascadia.NIP51_TASK_COLLECTION
	KindCanonicalState  = cascadia.CAS_CP_STATE

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
