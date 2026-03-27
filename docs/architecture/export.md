# Export

`nagi.yaml` の `export` を設定すると、Nagi は実行ログ（evaluate / sync）をデータウェアハウスへエクスポートします。

## How it works

1. [`nagi compile`](../reference/cli.md#compile) が **エクスポート用リソース（Conditions / Sync / Asset）を自動生成**する
2. [`nagi serve`](../reference/cli.md#serve) が自動生成された Asset を他の Asset と同様に reconcile し、未エクスポートの行があれば `nagi export` を実行する
3. [`nagi export`](../reference/cli.md#export) を手動実行することもできる

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

`nagi compile` は以下の 5 リソースを自動生成します。ユーザーが `resources/` に配置する必要はありません。

### Conditions (`nagi-export-drift`)

未エクスポートの行がないかチェックします。`{{ sync.table }}` は Asset の `with` から展開されます。

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

テーブル単位でエクスポートします。`{{ sync.table }}` は Asset の `with` から渡されます。

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

テーブルごとに 1 つ生成されます（`evaluate_logs`、`sync_logs`、`sync_evaluations`）。`with` でテーブル名を Sync に渡します。

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
