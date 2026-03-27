# Storage

Nagi は以下のデータを保存します。[nagi.yaml](../configurations/project.md) の `backend.type` でストレージバックエンドを切り替えられます。

| データ | 説明 | ローカル（デフォルト） | リモート（GCS / S3） |
| --- | --- | --- | --- |
| cache/evaluate | evaluate 結果のキャッシュ | `~/.nagi/cache/evaluate/` | `{bucket}/cache/evaluate/` |
| locks | sync の排他ロック | `~/.nagi/locks/` | `{bucket}/locks/` |
| suspended | Guardrails による停止フラグ | `~/.nagi/suspended/` | `{bucket}/suspended/` |
| logs.db | 実行履歴を保存する SQLite ファイル | `~/.nagi/logs.db` | `~/.nagi/logs.db` |
| logs/ | sync の stdout/stderr | `~/.nagi/logs/` | `{bucket}/logs/` |
| watermarks/ | データウェアハウスエクスポートのウォーターマーク | `~/.nagi/watermarks/` | `~/.nagi/watermarks/` |

リモートバックエンドを選択すると、[`nagi serve`](../cli.md#serve) がリモートサーバーで動いている場合には、異なる環境から [`nagi status`](../cli.md#status) や [`nagi serve resume`](../cli.md#serve) を実行できます。

`logs.db` は [`nagi serve`](../cli.md#serve) を実行した環境のファイルシステムに配置されます。

## Caches

Evaluate の結果を Asset・条件ごとにファイルとして保存します。[`nagi status`](../cli.md#status) はこのキャッシュを読み取るため、評価は実行しません。`nagi serve` では条件ごとの `evaluateCacheTtl` に基づき、TTL 内であればキャッシュ済みの結果を再利用してクエリの実行をスキップします。

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

Asset ごとの sync 同時実行を防ぐ排他ロックです。同じ Asset に対する sync が並列実行されると操作対象のデータが競合するため、排他ロック用のファイルを作成します。

!!! tip
    すべてのロックは TTL（有効期限）付きで、プロセスが異常終了した場合でも期限切れにより自動で解放されます。TTL は [`nagi.yaml`](../configurations/project.md) の `lockTtlSeconds` で変更できます（デフォルト: 3600秒）。

ファイル名は [kind: Asset](../configurations/resources/asset.md) の `metadata.name` に対応します。

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

Guardrails が sync を停止した Asset のフラグです。停止理由、停止時刻、起動元となった sync の実行 ID を含みます。[`nagi serve resume`](../cli.md#serve-resume) で解除します。

ファイル名は [kind: Asset](../configurations/resources/asset.md) の `metadata.name` に対応します。

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

実行ログは、メタデータとログ本体を分離して保存します。

`logs.db` は SQLite データベースで、evaluate と sync の実行履歴を記録します。[`nagi init`](../cli.md#init) でスキーマが初期化されます。既にデータベースが存在する場合は既存のデータを保持します。[`nagi status`](../cli.md#status) はこのデータベースから直近の実行結果を読み取ります。

`logs/` は sync の各ステージの stdout / stderr をファイルとして保存します。`logs.db` のレコードからこれらのファイルパスを参照できます。

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

`logs.db` のデータは [`nagi export`](../cli.md#export) でデータウェアハウスにエクスポートできます。設定と仕組みは [Export](./export.md) を参照してください。

## Watermarks

[`nagi export`](../cli.md#export) が データウェアハウスにエクスポート済みの位置を記録するファイルです。テーブルごとにウォーターマーク（最後にエクスポートした `rowid`）を保持し、差分転送を実現します。詳細は [Export](./export.md) を参照してください。

```text
~/.nagi/watermarks/
├── evaluate_logs.json
├── sync_logs.json
└── sync_evaluations.json
```
