# dbt Core

Nagi automatically generates Nagi resources from a dbt project through [kind: Origin](../../reference/resources/origin.md).

## Prerequisites

[dbt CLI](https://docs.getdbt.com/docs/core/installation-overview) (dbt-core >= 1.0) must be installed in the environment where Nagi runs.

## Init

Running [`nagi init`](../../reference/cli.md#init) generates the following resources in `resources/`.
Specify the dbt project path and the profile / target to use.

| dbt configuration | Generated resource |
| --- | --- |
| profile / target | [kind: Connection](../../reference/resources/connection.md) |
| project | [kind: Origin](../../reference/resources/origin.md) |

```yaml
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: my-bigquery
spec:
  dbtProfile:
    profile: my_project
    target: dev
```

```yaml
apiVersion: nagi.io/v1alpha1
kind: Origin
metadata:
  name: my-dbt-project
spec:
  type: DBT
  connection: my-bigquery
  projectDir: ../dbt-project
```

The Connection's `profile` and `target` are passed as the `--profile` / `--target` options to dbt commands.
The Origin's `projectDir` is used as the `--project-dir` option.

## Compile

Running [`nagi compile`](../../reference/cli.md#compile) reads the dbt project based on the Origin definition and generates Asset / Conditions / Sync in `target/`.

Asset names follow the pattern `{Origin name}.{model name}`.

For resource generation mapping and merge behavior, see [Resource Generation](./resource-generation.md).

## Evaluate

Evaluate is executed directly by Nagi.

- Freshness / SQL conditions: Nagi issues queries directly to the data warehouse using the Connection
- Command conditions (dbt test): Executes dbt CLI as a subprocess

## Sync

Executes dbt CLI as a subprocess (e.g., `dbt run --select daily_sales`).

## Customization

If you define an Asset with the same name as an Origin-generated Asset in `resources/`, the `onDrift` lists are concatenated. For merge rules and examples, see [Resource Generation - Merge with User-defined Resources](./resource-generation.md#merge-with-user-defined-resources).

## Multi Projects

When multiple dbt projects are configured as Origins, Nagi automatically maps dependencies between projects. Mapping is based on dbt Relations (`database.schema.identifier`).
