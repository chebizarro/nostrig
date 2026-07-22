package gitea

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestClientGetsAndUpdatesOnlyIssueState(t *testing.T) {
	var patch map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/repos/acme/repo/issues/42" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "token secret" {
			t.Fatalf("authorization = %q", r.Header.Get("Authorization"))
		}
		if r.Method == http.MethodPatch {
			if r.Header.Get("If-Match") != `"rev-1"` {
				t.Fatalf("If-Match = %q", r.Header.Get("If-Match"))
			}
			if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
				t.Fatal(err)
			}
		}
		w.Header().Set("ETag", `"rev-2"`)
		_ = json.NewEncoder(w).Encode(Issue{Number: 42, Title: "Title", Body: "Body", State: "closed"})
	}))
	defer server.Close()
	client, err := NewClient(server.URL, "secret", server.Client())
	if err != nil {
		t.Fatal(err)
	}
	issue, err := client.GetIssue(context.Background(), IssueKey{Owner: "acme", Repo: "repo", Number: 42})
	if err != nil || issue.Number != 42 {
		t.Fatalf("GetIssue = %#v, %v", issue, err)
	}
	issue, err = client.UpdateIssueState(context.Background(), IssueKey{Owner: "acme", Repo: "repo", Number: 42}, "closed", `"rev-1"`)
	if err != nil {
		t.Fatal(err)
	}
	if len(patch) != 1 || patch["state"] != "closed" {
		t.Fatalf("patch = %#v", patch)
	}
	if issue.ETag != `"rev-2"` {
		t.Fatalf("etag = %q", issue.ETag)
	}
}

func TestClientRejectsCrossHostRedirect(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("token-bearing request followed cross-host redirect")
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer source.Close()
	client, err := NewClient(source.URL, "secret", source.Client())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetIssue(context.Background(), IssueKey{Owner: "acme", Repo: "repo", Number: 1}); err == nil {
		t.Fatal("expected redirect rejection")
	}
}
