package gitea

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type IssueKey struct {
	Owner  string
	Repo   string
	Number int64
}

type Issue struct {
	Number int64  `json:"number"`
	Title  string `json:"title"`
	Body   string `json:"body"`
	State  string `json:"state"`
	URL    string `json:"html_url"`
	ETag   string `json:"-"`
}

type HTTPError struct {
	StatusCode int
	Message    string
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("Gitea request failed (%d): %s", e.StatusCode, e.Message)
}

func IsNotFound(err error) bool {
	httpErr, ok := err.(*HTTPError)
	return ok && httpErr.StatusCode == http.StatusNotFound
}

type Client struct {
	baseURL *url.URL
	token   string
	http    *http.Client
}

func NewClient(baseURL, token string, client *http.Client) (*Client, error) {
	parsed, err := url.Parse(strings.TrimRight(strings.TrimSpace(baseURL), "/"))
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
		return nil, fmt.Errorf("invalid Gitea base URL %q", baseURL)
	}
	if parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("Gitea base URL must not contain credentials, query, or fragment")
	}
	if client == nil {
		client = &http.Client{Timeout: 15 * time.Second}
	}
	copyClient := *client
	previousRedirect := copyClient.CheckRedirect
	copyClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) > 0 && !strings.EqualFold(req.URL.Host, via[0].URL.Host) {
			return fmt.Errorf("refusing cross-host Gitea redirect")
		}
		if previousRedirect != nil {
			return previousRedirect(req, via)
		}
		if len(via) >= 10 {
			return fmt.Errorf("too many Gitea redirects")
		}
		return nil
	}
	return &Client{baseURL: parsed, token: strings.TrimSpace(token), http: &copyClient}, nil
}

func (c *Client) GetIssue(ctx context.Context, key IssueKey) (*Issue, error) {
	return c.do(ctx, http.MethodGet, key, "", nil)
}

func (c *Client) UpdateIssueState(ctx context.Context, key IssueKey, state, expectedETag string) (*Issue, error) {
	state = strings.ToLower(strings.TrimSpace(state))
	if state != "open" && state != "closed" {
		return nil, fmt.Errorf("invalid Gitea issue state %q", state)
	}
	return c.do(ctx, http.MethodPatch, key, expectedETag, map[string]string{"state": state})
}

func (c *Client) do(ctx context.Context, method string, key IssueKey, etag string, payload any) (*Issue, error) {
	if c == nil || c.baseURL == nil || c.http == nil {
		return nil, fmt.Errorf("Gitea client is not initialized")
	}
	if strings.TrimSpace(key.Owner) == "" || strings.TrimSpace(key.Repo) == "" || key.Number <= 0 {
		return nil, fmt.Errorf("invalid Gitea issue key")
	}
	endpoint := *c.baseURL
	endpoint.Path = strings.TrimRight(endpoint.Path, "/") + "/api/v1/repos/" +
		url.PathEscape(strings.TrimSpace(key.Owner)) + "/" + url.PathEscape(strings.TrimSpace(key.Repo)) +
		"/issues/" + strconv.FormatInt(key.Number, 10)
	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		body = bytes.NewReader(raw)
	}
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), body)
	if err != nil {
		return nil, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "nostrig")
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		request.Header.Set("Authorization", "token "+c.token)
	}
	if etag != "" {
		request.Header.Set("If-Match", etag)
	}
	response, err := c.http.Do(request)
	if err != nil {
		return nil, fmt.Errorf("Gitea request: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		limited, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		message := strings.TrimSpace(string(limited))
		switch response.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			message = "authorization failed"
		case http.StatusNotFound:
			message = "issue not found"
		case http.StatusConflict, http.StatusPreconditionFailed:
			message = "issue changed concurrently"
		default:
			if message == "" {
				message = response.Status
			}
		}
		return nil, &HTTPError{StatusCode: response.StatusCode, Message: message}
	}
	var issue Issue
	if err := json.NewDecoder(io.LimitReader(response.Body, 2<<20)).Decode(&issue); err != nil {
		return nil, fmt.Errorf("decode Gitea issue: %w", err)
	}
	if issue.Number == 0 {
		issue.Number = key.Number
	}
	issue.State = strings.ToLower(strings.TrimSpace(issue.State))
	if issue.Number != key.Number || (issue.State != "open" && issue.State != "closed") {
		return nil, fmt.Errorf("invalid Gitea issue response")
	}
	issue.ETag = response.Header.Get("ETag")
	return &issue, nil
}
