# Export

`nagi.yaml` の `export` を設定すると、Nagi は実行ログ（evaluate / sync）をデータウェアハウスへエクスポートします。

## How it works

1. [`nagi compile`](../cli.md#compile) が **エクスポート用リソース（Conditions / Sync / Asset）を自動生成**する
2. [`nagi serve`](../cli.md#serve) が自動生成された Asset を他の Asset と同様に reconcile し、未エクスポートの行があれば `nagi export` を実行する
3. [`nagi export`](../cli.md#export) を手動実行することもできる

省略するとログはローカルの `logs.db` のみに保持され、リソースの自動生成は行われません。

## Configuration

```yaml
# nagi.yaml
export:
  connection: my-bigquery          # kind: Connection の metadata.name
  dataset: my_project.nagi_logs    # BigQuery dataset
  format: jsonl                    # 中間ファイル形式 (jsonl | duckdb)
  interval: 30m                    # エクスポート間隔（デフォルト: 30m）
```

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `connection` | string | Yes | - | Reference to a `kind: Connection` resource name. |
| `dataset` | string | Yes | - | Data warehouse dataset (BigQuery) or schema (Snowflake) to export into. |
| `format` | string | — | jsonl | Intermediate file format for export (`jsonl` or `duckdb`). |
| `interval` | duration | — | 30m | Condition evaluation interval and export throttling threshold. |

## Auto-generated resources

`nagi compile` はエクスポート対象のテーブルごとに Conditions・Sync・Asset の 3 リソースを自動生成します。対象テーブルは 3 つあるため、合計 9 リソースが生成されます。ユーザーが `resources/` に配置する必要はありません。

### Target tables

| Table | Description |
| --- | --- |
| `evaluate_logs` | Evaluate の実行履歴 |
| `sync_logs` | Sync の実行履歴 |
| `sync_evaluations` | Sync 実行時の evaluate 結果 |

### Generated resource example

`evaluate_logs` テーブルの場合、以下の 3 リソースが生成されます（`sync_logs`、`sync_evaluations` も同じ構造）。

**Conditions** — 未エクスポートの行があるかチェックする:

```yaml
apiVersion: nagi.dev/v1
kind: Conditions
metadata:
  name: export-evaluate_logs-drift
spec:
  - name: unexported-rows
    run: ["sh", "-c", "nagi export --select evaluate_logs --dry-run | jq -e '.[] | .count == 0'"]
    interval: 30m
```

**Sync** — エクスポートを実行する:

```yaml
apiVersion: nagi.dev/v1
kind: Sync
metadata:
  name: export-evaluate_logs
spec:
  run:
    command: ["nagi", "export", "--select", "evaluate_logs"]
```

**Asset** — Conditions と Sync を結びつける:

```yaml
apiVersion: nagi.dev/v1
kind: Asset
metadata:
  name: nagi-export-evaluate_logs
spec:
  autoSync: true
  onDrift:
    - conditions: export-evaluate_logs-drift
      sync: export-evaluate_logs
```

## Destination tables

エクスポート先のデータウェアハウスには `dataset` で指定したデータセット内に以下のテーブルが作成されます。

| Table | Description |
| --- | --- |
| `evaluate_logs` | Evaluate 結果（condition ごとに 1 行） |
| `sync_logs` | Sync 実行ログ（stage ごとに 1 行） |
| `sync_evaluations` | Sync 実行時の evaluate スナップショット |
| `_staging_*` | MERGE 操作で使用するステージングテーブル（エクスポート完了後にクリアされる） |

データは MERGE（upsert）で書き込まれるため、再実行しても重複しません。

## Watermarks

エクスポート済みの位置は `~/.nagi/watermarks/` にテーブルごとのファイルとして記録されます。各ファイルは最後にエクスポートした `rowid` を保持し、差分転送を実現します。

```text
~/.nagi/watermarks/
├── evaluate_logs.json
├── sync_logs.json
└── sync_evaluations.json
```

詳細は [Storage](./storage.md#watermarks) を参照してください。
