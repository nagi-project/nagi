# Storage

Nagi stores the following data.

| Data | Description | Local (default) | Remote (GCS / S3) |
| --- | --- | --- | --- |
| cache/evaluate | Cache of Evaluate results | `~/.nagi/cache/evaluate/` | `{bucket}/cache/evaluate/` |
| locks | Exclusive locks for Sync | `~/.nagi/locks/` | `{bucket}/locks/` |
| suspended | Suspension flags set by Guardrails | `~/.nagi/suspended/` | `{bucket}/suspended/` |
| logs.db | SQLite file storing execution history | `~/.nagi/logs.db` | `~/.nagi/logs.db` |
| logs/ | stdout/stderr of Sync | `~/.nagi/logs/` | `{bucket}/logs/` |
| watermarks/ | Watermarks for data warehouse export | `~/.nagi/watermarks/` | `~/.nagi/watermarks/` |

You can switch the storage backend using `backend.type` in [nagi.yaml](../reference/project.md).

When a remote backend is selected and [`nagi serve`](../reference/cli.md#serve) is running on a remote server, you can run [`nagi status`](../reference/cli.md#status) or [`nagi serve resume`](../reference/cli.md#serve) from the client environment.

## Caches

Evaluate results are saved as files per Asset and desired state. [`nagi status`](../reference/cli.md#status) reads from this cache, so it does not run an evaluation. In `nagi serve`, if the result is within the `evaluateCacheTtl` configured per desired state, the cached result is reused and the query execution is skipped.

```text
~/.nagi/cache/evaluate/
├── daily-sales/
│   ├── freshness-check.json
│   └── data-test.json
├── raw-sales/
│   └── freshness-check.json
└── monthly-report/
    └── freshness-check.json
```

<!-- schema:auto-generated:start:AssetEvalResult -->

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `assetName` | string | Yes | - | Name of the evaluated Asset resource. |
| `conditions` | list[ConditionResult] | Yes | - | Per-condition evaluation results. |
| `ready` | boolean | Yes | - | true when all conditions are Ready. |
| `evaluationId` | string | — | - | Set when the result was logged via `LogStore`. |

<!-- schema:auto-generated:end:AssetEvalResult -->

## Locks

Exclusive locks that prevent concurrent Sync execution for the same Asset. Running Sync in parallel for the same Asset would cause data conflicts, so a lock file prevents concurrent execution.

!!! tip
    All locks have a TTL (time-to-live), so they are automatically released upon expiration even if the process terminates abnormally. The TTL can be changed via `lockTtlSeconds` in [`nagi.yaml`](../reference/project.md) (default: 3600 seconds).

File names correspond to the `metadata.name` of [kind: Asset](../reference/resources/asset.md).

```text
~/.nagi/locks/
├── {asset-name-01}.lock
└── {asset-name-02}.lock
```

<!-- schema:auto-generated:start:LockInfo -->

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `acquired_at_epoch_secs` | integer | Yes | - | Unix epoch seconds when the lock was acquired. |
| `execution_id` | string | Yes | - | Execution ID of the sync run that acquired the lock. Correlates with the execution_id in sync logs. |
| `ttl_secs` | integer | Yes | - | Time-to-live in seconds; the lock expires after this duration. |

<!-- schema:auto-generated:end:LockInfo -->

## Suspended

Flags for Assets whose Sync has been stopped by Guardrails. They contain the suspension reason, suspension time, and the execution ID of the Sync that triggered the suspension. Use [`nagi serve resume`](../reference/cli.md#serve-resume) to clear them.

File names correspond to the `metadata.name` of [kind: Asset](../reference/resources/asset.md).

```text
~/.nagi/suspended/
├── {asset-name-01}.json
└── {asset-name-02}.json
```

<!-- schema:auto-generated:start:SuspendedInfo -->

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `asset_name` | string | Yes | - | Name of the suspended Asset resource. |
| `reason` | string | Yes | - | Human-readable reason for the suspension. |
| `suspended_at` | string | Yes | - | RFC 3339 timestamp when the asset was suspended. |
| `execution_id` | string | — | - | The sync execution_id that triggered the suspension, if available. |

<!-- schema:auto-generated:end:SuspendedInfo -->

## Logs

Execution logs are stored with metadata and log bodies separated.

`logs.db` is a SQLite database that records the execution history of Evaluate and Sync. The schema is initialized by [`nagi init`](../reference/cli.md#init). If the database already exists, existing data is preserved. [`nagi status`](../reference/cli.md#status) reads the most recent execution results from this database.

`logs/` stores the stdout / stderr of each Sync stage as files. These file paths can be referenced from records in `logs.db`.

```text
~/.nagi/logs/
└── {asset-name-01}/
    └── 2026/
        └── 03/
            └── 21/
                ├── 20260321T030000Z_pre.stdout
                ├── 20260321T030000Z_pre.stderr
                ├── 20260321T030015Z_run.stdout
                ├── 20260321T030015Z_run.stderr
                ├── 20260321T030120Z_post.stdout
                └── 20260321T030120Z_post.stderr
```

<!-- schema:auto-generated:start:SyncLogEntry -->

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `assetName` | string | Yes | - | Name of the Asset that was synced. |
| `date` | string | Yes | - | Date partition key (YYYY-MM-DD) derived from `started_at`. |
| `executionId` | string | Yes | - | Unique identifier for this sync execution. |
| `finishedAt` | string | Yes | - | RFC 3339 timestamp when the stage finished. |
| `stage` | string | Yes | - | Pipeline stage (e.g. `pre`, `run`, `post`). |
| `startedAt` | string | Yes | - | RFC 3339 timestamp when the stage started. |
| `syncType` | string | Yes | - | Whether this was a `sync` operation. |
| `exitCode` | integer | — | - | Process exit code of the stage command. None for non-execution stages (e.g. lock_retry). |
| `stderrPath` | string | — | - | File path where stderr output is stored. None for non-execution stages. |
| `stdoutPath` | string | — | - | File path where stdout output is stored. None for non-execution stages. |

<!-- schema:auto-generated:end:SyncLogEntry -->

`logs.db` is created on the filesystem of the environment where [`nagi serve`](../reference/cli.md#serve) is running.

The data in `logs.db` can be exported to a data warehouse using [`nagi export`](../reference/cli.md#export). See [Export](./export.md) for configuration and how it works.

## Watermarks

Files that record the position already exported by [`nagi export`](../reference/cli.md#export) to a data warehouse. Each file holds a watermark (the last exported `rowid`) per table, enabling incremental transfer. See [Export](./export.md) for details.

```text
~/.nagi/watermarks/
├── evaluate_logs.json
├── sync_logs.json
└── sync_evaluations.json
```
