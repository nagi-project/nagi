# kind: Conditions

A resource that groups desired state definitions. Used when multiple Assets share the same desired state. Referenced by specifying the name in the `conditions` field of an Asset's `onDrift` entry.

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

Desired states within the same Conditions are AND-evaluated. If even one is Drifted, the Conditions is Drifted.

## Freshness

Freshness changes how the last update time is retrieved depending on the configuration.

### When `column` Is Specified

Retrieves the maximum value of the column with `SELECT MAX(column)` and uses it as the logical last update time of the data. Targets timestamp columns such as `updated_at` or `created_at`.

```yaml
- name: freshness
  type: Freshness
  maxAge: 24h
  interval: 6h
  column: updated_at
```

### When `column` Is Omitted

Retrieves the physical last update time from table metadata. For BigQuery, uses `INFORMATION_SCHEMA.TABLE_STORAGE`. Works for all tables regardless of whether they are partitioned.

```yaml
- name: freshness
  type: Freshness
  maxAge: 24h
  interval: 6h
  # column omitted → retrieved from table metadata
```

Omit `column` for tables without a timestamp column or where column values are not related to data freshness.

## SQL Read-Only Constraint

Nagi restricts SQL queries to read-only. Statements other than SELECT (INSERT, UPDATE, DELETE, DDL, etc.) and multi-statement queries are rejected.

## Command Without Shell

Because no shell is used, metacharacters (`;`, `&&`, `|`, etc.) are passed as-is as arguments. This is intended for wrapping existing data validation tools (e.g. `dbt test --select`) as desired state checks.

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
