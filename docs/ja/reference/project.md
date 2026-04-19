# Project Configurations

## nagi.yaml

プロジェクト設定ファイルです。[`nagi init`](./cli.md#init) で生成され、プロジェクトルート（`resources/` と同階層）に配置します。

```yaml
backend:
  type: local
stateDir: ~/.nagi
defaultTimeout: 1h
notify:
  slack:
    channel: "#nagi-alerts"
export:
  connection: my-bigquery
  dataset: my_project.nagi_logs
terminationGracePeriodSeconds: 300
```

`stateDir` は Nagi の状態ディレクトリのパスを指定します。`cache/`、`locks/`、`suspended/`、`logs.db`、`logs/`、`watermarks/` がこのディレクトリに配置されます。省略時は `~/.nagi` です。

`notify.slack` を設定すると Slack 通知が有効になります。省略すると通知機能は OFF になります。設定方法は [Notifications](../architecture/notifications.md) を参照してください。

`export` を設定すると、実行ログをデータウェアハウスにエクスポートできます。compile がエクスポート用リソースを自動生成し、ユーザーのデータウェアハウスにテーブルを作成します。詳細は [Export](../architecture/export.md) を参照してください。

<!-- schema:auto-generated:start:NagiConfig -->

## Attributes

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `backend.bucket` | string | — | - | Bucket name for GCS or S3 backend. Required when type is `gcs` or `s3`. |
| `backend.prefix` | string | — | - | Path prefix for remote storage (e.g. `my-project/nagi`). When set, all remote paths are prefixed with this value. Ignored for the local backend. |
| `backend.region` | string | — | - | AWS region for S3 backend (e.g. `us-east-1`). Required when type is `s3`. |
| `backend.type` | BackendType | — | - | Backend type. Defaults to `local`. |
| `defaultTimeout` | Duration | — | 1h | - |
| `export.connection` | string | Yes | - | Reference to a `kind: Connection` resource name. |
| `export.dataset` | string | Yes | - | Data warehouse dataset (BigQuery) or schema (Snowflake) to export into. |
| `export.format` | ExportFormat | — | jsonl | Intermediate file format for export. |
| `export.interval` | Duration | — | 30m | Condition evaluation interval and export throttling threshold. |
| `export.timeout` | Duration | — | - | Timeout for export operations. Falls back to `defaultTimeout` when omitted. |
| `lockRetryIntervalSeconds` | integer | — | 900 | - |
| `lockRetryMaxAttempts` | integer | — | 3 | - |
| `lockTtlSeconds` | integer | — | 3600 | - |
| `maxControllers` | integer | — | - | - |
| `maxEvaluateConcurrency` | integer | — | - | - |
| `maxSyncConcurrency` | integer | — | - | - |
| `notify.slack.channel` | string | Yes | - | Slack channel to send notifications to (e.g. `#nagi-alerts`). |
| `stateDir` | StateDir | — | ~/.nagi | - |
| `terminationGracePeriodSeconds` | integer | — | - | - |

<!-- schema:auto-generated:end:NagiConfig -->
