# Project Configurations

## nagi.yaml

プロジェクト設定ファイルです。[`nagi init`](./cli.md#init) で生成され、プロジェクトルート（`resources/` と同階層）に配置します。

```yaml
backend:
  type: local
nagiDir: ~/.nagi
notify:
  slack:
    channel: "#nagi-alerts"
export:
  connection: my-bigquery
  dataset: my_project.nagi_logs
terminationGracePeriodSeconds: 300
```

`nagiDir` は Nagi の状態ディレクトリのパスを指定します。`cache/`、`locks/`、`suspended/`、`logs.db`、`logs/`、`watermarks/` がこのディレクトリに配置されます。省略時は `~/.nagi` です。

`notify.slack` を設定すると Slack 通知が有効になります。省略すると通知機能は OFF になります。設定方法は [Notifications](../architecture/notifications.md) を参照してください。

`export` を設定すると、実行ログをデータウェアハウスにエクスポートできます。compile がエクスポート用リソースを自動生成し、ユーザーのデータウェアハウスにテーブルを作成します。詳細は [Export](../architecture/export.md) を参照してください。

<!-- schema:auto-generated:start:NagiConfig -->

## Attributes

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `backend.bucket` | string | — | - | Bucket name for GCS or S3 backend. Required when type is `gcs` or `s3`. |
| `backend.prefix` | string | — | - | Path prefix for remote storage (e.g. `my-project/nagi`). When set, all remote paths are prefixed with this value. Ignored for the local backend. |
| `backend.region` | string | — | - | AWS region for S3 backend (e.g. `us-east-1`). Required when type is `s3`. |
| `backend.type` | string | — | local | Backend type identifier. One of `local`, `gcs`, `s3`. Defaults to `local`. |
| `export` | ExportConfig | — | - | Log export configuration. When set, compile generates export Assets and logs are transferred to the remote data warehouse. |
| `lockRetryIntervalSeconds` | integer | — | 900 | Interval in seconds between lock acquisition retry attempts. Defaults to 900 (15 minutes). |
| `lockRetryMaxAttempts` | integer | — | 3 | Maximum number of lock acquisition retry attempts before skipping. Defaults to 3. |
| `lockTtlSeconds` | integer | — | 3600 | Time-to-live in seconds for sync lock files. Locks expire after this duration, preventing deadlocks from abnormal process termination. Defaults to 3600 (1 hour). |
| `maxControllers` | integer | — | - | Maximum number of Controllers to run in parallel during `nagi serve`. When the number of connected components exceeds this limit, serve exits with an error. When omitted, one Controller is created per connected component. |
| `maxEvaluateConcurrency` | integer | — | - | Maximum number of concurrent evaluate tasks per Controller. When omitted, no limit is applied. |
| `maxSyncConcurrency` | integer | — | - | Maximum number of concurrent sync tasks per Controller. When omitted, no limit is applied. |
| `nagiDir` | NagiDir | — | - | Base directory for Nagi state (logs, cache, locks, etc.). Defaults to `~/.nagi`. |
| `notify.slack` | SlackConfig | — | - | Slack notification settings. When set, notifications are sent to the specified channel. |
| `terminationGracePeriodSeconds` | integer | — | - | Maximum time in seconds to wait for in-flight sync tasks to finish during shutdown. When omitted, waits indefinitely. |

<!-- schema:auto-generated:end:NagiConfig -->
