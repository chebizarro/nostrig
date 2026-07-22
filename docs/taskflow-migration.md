# TaskFlow one-time migration

No live `PROJECTS.md` or `tasks/*-tasks.md` TaskFlow data was present under
`/Users/bizarro/Documents/Projects` when this importer was implemented. The
parser is therefore covered by format fixtures.

Preview the migration:

```bash
nostrig import taskflow \
  --source /path/to/taskflow \
  --canonical-author <hex-pubkey> \
  --dry-run \
  --report taskflow-migration-report.json
```

Publish the migration with the production signer and relay configuration:

```bash
nostrig import taskflow \
  --source /path/to/taskflow \
  --relay wss://relay.example \
  --signer-bunker-url 'bunker://...'
```

The importer reads the project table in `PROJECTS.md` and either task tables or
checkbox lists in `tasks/*-tasks.md`. It maps TaskFlow status names to
`open`, `in_progress`, `blocked`, `closed`, or `deferred`. Priority names
and P-levels map to P0-P4; parking-lot or unscheduled work maps to the fleet P9
convention. Notes become comments and explicit checkpoint fields become native
task-model-v2 checkpoints.

Task and child-record IDs are deterministic. After a successful publish, the
import state file records content hashes; unchanged re-runs are skipped and
changed records publish replacements at the same canonical coordinates.
`--dry-run` never writes this state.

After the migration report has been reviewed and the publish succeeds, **Nostrig
is authoritative**. The TaskFlow files must be made read-only or retired for
fleet work. This command is not a synchronization service and must not be used
to create permanent TaskFlow/Beads/Nostrig three-way synchronization.
