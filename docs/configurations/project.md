# Project Configurations

## nagi.yaml

プロジェクト設定ファイルです。[`nagi init`](../cli.md#init) で生成され、プロジェクトルート（`resources/` と同階層）に配置します。

```yaml
backend:
  type: local
notify:
  slack:
    channel: "#nagi-alerts"
terminationGracePeriodSeconds: 300
```

`notify.slack` を設定すると Slack 通知が有効になります。省略すると通知機能は OFF になります。設定方法は [Notifications](../architecture/notifications.md) を参照してください。

<!-- schema:auto-generated:start:NagiConfig -->

## Attributes

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `backend` | any | — | - | State storage backend configuration. |
| `lockRetryIntervalSeconds` | integer | — | 900 | Interval in seconds between lock acquisition retry attempts. Defaults to 10. |
| `lockRetryMaxAttempts` | integer | — | 3 | Maximum number of lock acquisition retry attempts before skipping. Defaults to 30. |
| `lockTtlSeconds` | integer | — | 3600 | Time-to-live in seconds for sync lock files. Locks expire after this duration, preventing deadlocks from abnormal process termination. Defaults to 3600 (1 hour). |
| `maxControllers` | integer | — | - | Maximum number of Controllers to run in parallel during `nagi serve`. When the number of connected components exceeds this limit, serve exits with an error. When omitted, one Controller is created per connected component. |
| `notify` | any | — | - | Notification channel configuration. |
| `terminationGracePeriodSeconds` | integer | — | - | Maximum time in seconds to wait for in-flight sync tasks to finish during shutdown. When omitted, waits indefinitely. |

<!-- schema:auto-generated:end:NagiConfig -->
