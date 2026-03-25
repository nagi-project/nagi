# kind: Conditions

再利用可能な条件のセットです。複数の Asset が同じ条件を共有する場合に使います。Asset の `onDrift` エントリの `conditions` フィールドで名前を指定して参照します。

```yaml
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: daily-sla
spec:
  - name: freshness-24h
    type: Freshness
    maxAge: 24h
    interval: 6h
  - name: no-nulls
    type: SQL
    query: "SELECT COUNT(*) = 0 FROM daily_sales WHERE id IS NULL"
```

同一 Conditions 内の条件は AND 評価されます。1つでも Drifted であればその Conditions は Drifted です。

## Freshness

Freshness は、設定方法によって最終更新時刻の取得方法が変わります。

### When `column` Is Specified

`SELECT MAX(column)` でカラムの最大値を取得し、データの論理的な最終更新時刻として使用します。`updated_at` や `created_at` のようなタイムスタンプカラムが対象です。

```yaml
- name: freshness
  type: Freshness
  maxAge: 24h
  interval: 6h
  column: updated_at
```

### When `column` Is Omitted

テーブルメタデータから物理的な最終更新時刻を取得します。BigQuery の場合は `INFORMATION_SCHEMA.TABLE_STORAGE` を使用します。パーティションの有無にかかわらず全テーブルに対応します。

```yaml
- name: freshness
  type: Freshness
  maxAge: 24h
  interval: 6h
  # column 省略 → テーブルメタデータから取得
```

タイムスタンプ列がないテーブルや、列の値がデータの鮮度と関連しない場合は `column` を省略してください。

## SQL Read-Only Constraint

Nagi は SQL クエリを読み取り専用に制限します。SELECT 以外の文（INSERT, UPDATE, DELETE, DDL 等）やmulti-statement query は拒否されます。

## Command Without Shell

シェルを経由しないため、メタキャラクタ（`;`, `&&`, `|` 等）はそのまま引数として渡されます。`dbt test --select` のように既存のデータ検証ツールを条件にマッピングする用途を想定しています。

<!-- schema:auto-generated:start:ConditionsSpec -->

## Attributes

### type: Freshness

Can transition to Not Ready as time passes beyond `maxAge`.

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `interval` | any | Yes | - | Polling interval for re-evaluating this condition. |
| `maxAge` | any | Yes | - | Maximum acceptable age of the data before the condition becomes Not Ready. |
| `name` | string | Yes | - | Unique identifier for this condition within the Asset. |
| `checkAt` | CronSchedule | — | - | Optional cron expression for additional evaluation at a specific time. |
| `column` | string | — | - | If omitted, freshness is determined from table metadata instead of a column value. |
| `evaluateCacheTtl` | Duration | — | - | Per-condition cache TTL override. Takes precedence over the Asset-level default. |

### type: SQL

Query must return a scalar boolean. Ready when the result is true.

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | string | Yes | - | Unique identifier for this condition within the Asset. |
| `query` | string | Yes | - | SQL query that must return a scalar boolean. |
| `evaluateCacheTtl` | Duration | — | - | Per-condition cache TTL override. Takes precedence over the Asset-level default. |
| `interval` | Duration | — | - | Optional polling interval. If omitted, only evaluated on upstream state change or after sync. |

### type: Command

Runs an external command. Ready when the process exits with code 0. `run` is argv: the first element is the program, the rest are arguments.

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `name` | string | Yes | - | Unique identifier for this condition within the Asset. |
| `run` | list[string] | Yes | - | Command and arguments in argv format. |
| `env` | map[string, string] | — | {} | Environment variables to set for the subprocess. |
| `evaluateCacheTtl` | Duration | — | - | Per-condition cache TTL override. Takes precedence over the Asset-level default. |
| `interval` | Duration | — | - | Optional polling interval. If omitted, only evaluated on upstream state change or after sync. |

<!-- schema:auto-generated:end:ConditionsSpec -->
