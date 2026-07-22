package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	nip34 "github.com/chebizarro/nostrig/internal/nostr"
	"github.com/spf13/cobra"
)

func newOutboxCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "outbox", Short: "Inspect and recover durable relay publications"}
	cmd.AddCommand(newOutboxListCmd(false), newOutboxListCmd(true), newOutboxRetryCmd())
	return cmd
}

func newOutboxListCmd(deadLetters bool) *cobra.Command {
	path := defaultOutboxPath()
	use, short := "list", "List queued relay publications"
	if deadLetters {
		use, short = "dlq", "List dead-lettered relay publications"
	}
	cmd := &cobra.Command{Use: use, Short: short, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		store, err := nip34.OpenOutbox(path)
		if err != nil {
			return err
		}
		entries, err := store.List()
		if deadLetters {
			entries, err = store.DeadLetters()
		}
		if err != nil {
			return err
		}
		return json.NewEncoder(cmd.OutOrStdout()).Encode(entries)
	}}
	cmd.Flags().StringVar(&path, "path", path, "Outbox state file; defaults to NOSTRIG_OUTBOX_PATH")
	return cmd
}

func newOutboxRetryCmd() *cobra.Command {
	path := defaultOutboxPath()
	cmd := &cobra.Command{
		Use:   "retry [event-id...]",
		Short: "Return selected dead letters to the retry queue (all when omitted)",
		Args:  cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, ids []string) error {
			store, err := nip34.OpenOutbox(path)
			if err != nil {
				return err
			}
			count, err := store.Retry(ids...)
			if err != nil {
				return err
			}
			return json.NewEncoder(cmd.OutOrStdout()).Encode(map[string]any{"scheduled": count, "event_ids": ids})
		},
	}
	cmd.Flags().StringVar(&path, "path", path, "Outbox state file; defaults to NOSTRIG_OUTBOX_PATH")
	return cmd
}

func defaultOutboxPath() string {
	if path := strings.TrimSpace(os.Getenv("NOSTRIG_OUTBOX_PATH")); path != "" {
		return path
	}
	return ".nostrig/outbox.json"
}

func defaultCommandJournalPath(outboxPath string) string {
	if path := strings.TrimSpace(os.Getenv("NOSTRIG_COMMAND_JOURNAL_PATH")); path != "" {
		return path
	}
	return filepath.Join(filepath.Dir(outboxPath), "commands.json")
}
