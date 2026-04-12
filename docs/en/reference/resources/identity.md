# kind: Identity

Declares an authentication scope that can be referenced from Connection, Sync, and Command conditions. Binds a set of environment variables to a named identity so that each operation runs with specific credentials.

```yaml
apiVersion: nagi.io/v1alpha1
kind: Identity
metadata:
  name: bq-evaluator
spec:
  type: env
  env:
    GOOGLE_APPLICATION_CREDENTIALS: ${NAGI_EVAL_KEYFILE}
```

## How It Works

When a Connection, Sync stage, or Command condition references an Identity by name, the environment variables defined in the Identity's `spec.env` are injected into the target scope.

For Connections, the env map is used to resolve authentication credentials at query time. For subprocesses (Sync stages and Command conditions), the env map is injected into the subprocess environment between the OS essentials and the step-level `env` declarations. See [Environment Variables](../environment-variables.md) for the full layering.

Values can use `${VAR}` syntax to reference the Nagi process's own environment. The reference is resolved at the point of use, not at parse time. This means the expanded credential value is never stored in a struct field.

## Referencing an Identity

### From Connection

```yaml
kind: Connection
metadata:
  name: my-bigquery
spec:
  type: bigquery
  project: my-project
  dataset: analytics
  identity: bq-evaluator
```

When a BigQuery Connection has an `identity` field, the Identity's env is used for authentication. If `GOOGLE_APPLICATION_CREDENTIALS` is present in the Identity's env, its value is used as the ADC file path.

DuckDB and Snowflake connections do not yet support Identity. If `identity` is specified, a warning is emitted at compile time.

### From Sync

```yaml
kind: Sync
metadata:
  name: dbt-run
spec:
  identity: bq-syncer
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.name }}"]
```

`identity` on the Sync applies to all stages (pre, run, post) unless overridden at the stage level.

### From a Sync Stage

```yaml
kind: Sync
metadata:
  name: dbt-run
spec:
  identity: default-id
  pre:
    type: Command
    args: ["python", "prepare.py"]
    identity: pre-id
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.name }}"]
```

`pre` uses `pre-id`. `run` and `post` inherit `default-id` from the Sync level.

### From Command Condition

```yaml
kind: Conditions
metadata:
  name: check-data
spec:
  - name: validate
    type: Command
    run: ["python", "validate.py"]
    identity: validator-id
```

## Identity Resolution Order

For each Sync stage, the Identity is determined as follows:

1. If the stage (pre, run, or post) specifies `identity`, that Identity is used
2. Otherwise, the Sync-level `identity` is inherited
3. If neither is specified, no Identity is applied

## type: env

Declares environment variable key-value pairs.

| Attribute | Type | Required | Description |
| --- | --- | --- | --- |
| `env` | map[string, string] | Yes | Environment variable names and values. Values may contain `${VAR}` references expanded from the Nagi process environment |
