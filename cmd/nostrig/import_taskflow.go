package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	taskflowimport "github.com/chebizarro/nostrig/internal/taskflow"
	"github.com/spf13/cobra"
)

func newImportCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "import",
		Short: "Run one-time imports into the canonical Nostrig ledger",
	}
	cmd.AddCommand(newTaskFlowImportCmd())
	return cmd
}

func newTaskFlowImportCmd() *cobra.Command {
	var source, statePath, reportPath, canonicalAuthor string
	var relays []string
	var signing signingOptions

	cmd := &cobra.Command{
		Use:   "taskflow",
		Short: "Import PROJECTS.md and tasks/*-tasks.md once, then retire TaskFlow",
		RunE: func(cmd *cobra.Command, args []string) error {
			signer, _, err := signerFromOptions(cmd.Context(), signing, !signing.dryRun)
			if err != nil {
				return err
			}
			author := strings.ToLower(strings.TrimSpace(canonicalAuthor))
			if signer != nil {
				signerAuthor, err := publicKeyFromSigner(cmd.Context(), signer)
				if err != nil {
					return err
				}
				if author != "" && author != strings.ToLower(signerAuthor) {
					return fmt.Errorf("--canonical-author does not match signer pubkey")
				}
				author = strings.ToLower(signerAuthor)
			}
			if author == "" {
				return fmt.Errorf("--canonical-author is required when no signer is configured")
			}
			report, err := taskflowimport.Run(cmd.Context(), taskflowimport.Options{
				Source: source, StatePath: statePath, CanonicalAuthor: author,
				Relays: relaysWithEnv(relays), Signer: signer, DryRun: signing.dryRun,
			})
			if err != nil {
				return err
			}
			data, err := json.MarshalIndent(report, "", "  ")
			if err != nil {
				return err
			}
			data = append(data, '\n')
			if strings.TrimSpace(reportPath) != "" {
				path, err := filepath.Abs(reportPath)
				if err != nil {
					return err
				}
				if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
					return err
				}
				if err := os.WriteFile(path, data, 0o644); err != nil {
					return err
				}
			}
			_, err = cmd.OutOrStdout().Write(data)
			return err
		},
	}
	cmd.Flags().StringVar(&source, "source", ".", "TaskFlow directory containing PROJECTS.md and tasks/")
	cmd.Flags().StringVar(&statePath, "state", "", "Idempotency state file (default: <source>/.nostrig/taskflow-import-state.json)")
	cmd.Flags().StringVar(&reportPath, "report", "", "Also write the migration report JSON to this path")
	cmd.Flags().StringVar(&canonicalAuthor, "canonical-author", "", "Canonical ledger author pubkey; defaults to signer pubkey")
	cmd.Flags().StringSliceVar(&relays, "relay", nil, "Relay websocket URL(s) to publish to (repeatable); falls back to NOSTR_RELAY/NOSTR_RELAYS")
	addSigningFlags(cmd, &signing)
	return cmd
}
