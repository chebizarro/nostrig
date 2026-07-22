package taskfabric

import (
	"testing"
	"time"
)

func TestNextReplaceableTimeAdvancesWithinSameNostrSecond(t *testing.T) {
	current := time.Unix(100, 900_000_000).UTC()
	now := time.Unix(100, 950_000_000).UTC()

	if got := nextTaskReplaceableTime(now, &TaskRecord{CreatedAt: current}); got.Unix() != 101 {
		t.Fatalf("task replacement timestamp = %s, want unix second 101", got)
	}
	if got := nextQueueReplaceableTime(now, &QueueRecord{CreatedAt: current}); got.Unix() != 101 {
		t.Fatalf("queue replacement timestamp = %s, want unix second 101", got)
	}
}

func TestNextReplaceableTimePreservesLaterSecond(t *testing.T) {
	current := time.Unix(100, 0).UTC()
	now := time.Unix(102, 250_000_000).UTC()

	if got := nextTaskReplaceableTime(now, &TaskRecord{CreatedAt: current}); !got.Equal(now) {
		t.Fatalf("task replacement timestamp = %s, want %s", got, now)
	}
	if got := nextQueueReplaceableTime(now, &QueueRecord{CreatedAt: current}); !got.Equal(now) {
		t.Fatalf("queue replacement timestamp = %s, want %s", got, now)
	}
}
