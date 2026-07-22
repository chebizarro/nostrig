package taskfabric

import (
	"sync"
	"testing"

	beadspb "github.com/chebizarro/nostrig/gen/beads"
	"google.golang.org/protobuf/proto"
)

func TestConcurrentReadOnlyCacheMergeIsDeterministic(t *testing.T) {
	issue := &beadspb.Issue{
		Id: "task-1", Title: "shared immutable input", Status: beadspb.Status_STATUS_OPEN,
		Metadata: &beadspb.Metadata{Custom: map[string]string{"nostr.id": "event-1"}},
	}
	relay := &beadspb.Export{Issues: []*beadspb.Issue{issue}}
	local := []*TaskSnapshot{SnapshotFromIssue(issue)}
	expected, err := MergeTaskState(relay, local, nil)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 64
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := MergeTaskState(relay, local, nil)
			if err != nil {
				errs <- err
				return
			}
			if !proto.Equal(got.Export, expected.Export) {
				errs <- &cacheMismatchError{}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
}

type cacheMismatchError struct{}

func (*cacheMismatchError) Error() string {
	return "concurrent cache projection differed from deterministic baseline"
}
