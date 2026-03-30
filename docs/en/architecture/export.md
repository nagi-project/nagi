# Export

When `export` is configured in `nagi.yaml`, Nagi exports execution logs (Evaluate / Sync) to a data warehouse.

## How it works

1. [`nagi compile`](../reference/cli.md#compile) auto-generates export resources (Conditions / Sync / Asset)
2. [`nagi serve`](../reference/cli.md#serve) reconciles the auto-generated Assets like any other Asset, and runs `nagi export` when there are unexported rows
3. [`nagi export`](../reference/cli.md#export) can also be run manually

If omitted, logs are kept only in the local `logs.db`, and no resources are auto-generated.

## Configuration

```yaml
# nagi.yaml
export:
  connection: my-bigquery          # metadata.name of kind: Connection
  dataset: my_project.nagi_logs    # BigQuery dataset
  format: jsonl                    # Intermediate file format (jsonl | duckdb)
  interval: 30m                    # Export interval (default: 30m)
```

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `connection` | string | Yes | - | Reference to a `kind: Connection` resource name. |
| `dataset` | string | Yes | - | Data warehouse dataset (BigQuery) or schema (Snowflake) to export into. |
| `format` | string | — | jsonl | Intermediate file format for export (`jsonl` or `duckdb`). |
| `interval` | duration | — | 30m | Condition evaluation interval and export throttling threshold. |

## Auto-generated resources

`nagi compile` auto-generates the following 5 resources. Users do not need to place them in `resources/`.

### Conditions (`nagi-export-drift`)

Checks whether there are unexported rows. `{{ sync.table }}` is expanded from the Asset's `with`.

```yaml
apiVersion: nagi.dev/v1
kind: Conditions
metadata:
  name: nagi-export-drift
spec:
  - name: unexported-rows
    run: ["sh", "-c", "nagi export --select {{ sync.table }} --dry-run | jq -e '.[] | .count == 0'"]
    interval: 30m
```

### Sync (`nagi-export`)

Exports per table. `{{ sync.table }}` is passed from the Asset's `with`.

```yaml
apiVersion: nagi.dev/v1
kind: Sync
metadata:
  name: nagi-export
spec:
  run:
    type: Command
    args: ["nagi", "export", "--select", "{{ sync.table }}"]
```

### Asset (`nagi-export-<table>`)

One is generated per table (`evaluate_logs`, `sync_logs`, `sync_evaluations`). The table name is passed to Sync via `with`.

```yaml
apiVersion: nagi.dev/v1
kind: Asset
metadata:
  name: nagi-export-evaluate_logs
spec:
  autoSync: true
  onDrift:
    - conditions: nagi-export-drift
      sync: nagi-export
      with:
        table: evaluate_logs
```

## Destination tables

The following tables are created in the dataset specified by `dataset` in the destination data warehouse.

| Table | Description |
| --- | --- |
| `evaluate_logs` | Evaluate results (one row per condition) |
| `sync_logs` | Sync execution logs (one row per stage) |
| `sync_evaluations` | Evaluate snapshots at Sync execution time |
| `_staging_*` | Staging tables used for MERGE operations (cleared after export completes) |

Data is written using MERGE (upsert), so re-execution does not produce duplicates.

## Watermarks

The exported position is recorded as per-table files in `~/.nagi/watermarks/`. Each file holds a watermark (the last exported `rowid`), enabling incremental transfer.

```text
~/.nagi/watermarks/
├── evaluate_logs.json
├── sync_logs.json
└── sync_evaluations.json
```

See [Storage](./storage.md#watermarks) for details.
