# kind: Origin

A resource that auto-generates Assets. During [`nagi compile`](../cli.md#compile), reads data structure information from other software and auto-generates Asset / Conditions / Sync resources.

```yaml
apiVersion: nagi.io/v1alpha1
kind: Origin
metadata:
  name: my-dbt-project
spec:
  type: DBT
  connection: my-bigquery
  projectDir: ../dbt-project
  autoSync: false           # Optional. Propagates to all auto-generated Assets.
```

## Asset Naming

Origin-generated Asset names are prefixed with the Origin name: `{origin}.{model}`.

For the example above (Origin name `my-dbt-project`):

- dbt model `orders` → Asset `my-dbt-project.orders`
- dbt source `raw.customers` → Asset `my-dbt-project.raw.customers`

!!! tip
    To reference an Origin-generated Asset in `upstreams`, use the `{Origin name}.{model name}` format.

## Auto-generated Sync

`type: DBT` auto-generates a Sync resource named `{origin}-dbt-run` (e.g. `my-dbt-project-dbt-run`):

```yaml
kind: Sync
metadata:
  name: my-dbt-project-dbt-run
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.modelName }}", "--project-dir", "../dbt-project", "--profile", "my_project", "--target", "dev"]
```

The `--project-dir`, `--profile`, and `--target` values are resolved from the Origin's Connection at compile time. `{{ asset.modelName }}` is a [template variable](./index.md#template-variables) that is replaced at compile time with the dbt model name without the Origin prefix (e.g. `orders`).

Assets for dbt models that have tests will have an `onDrift` entry referencing this Sync.

## Overriding with defaultSync

To use a user-defined Sync instead of the auto-generated one, specify it with `defaultSync`. It has the same interface as an `onDrift` entry (`sync` + `with`):

```yaml
spec:
  type: DBT
  connection: my-bigquery
  projectDir: ../dbt-project
  defaultSync:
    sync: my-custom-sync
    with:
      selector: "+{{ asset.modelName }}"
```

When `defaultSync` is specified, the `{origin}-dbt-run` Sync is not generated.

## Environment Variables

`type: DBT` Origins can declare environment variables via the `env` field. These are passed to the `dbt compile` subprocess. Only declared values and a minimal set of OS essentials reach the subprocess — the parent shell's environment is not inherited. Values can reference the Nagi process's own environment using `${VAR}` syntax.

If your `profiles.yml` uses `{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}` or similar, declare the referenced variable in `env`:

```yaml
spec:
  type: DBT
  connection: my-bigquery
  projectDir: ../dbt-project
  env:
    GOOGLE_APPLICATION_CREDENTIALS: ${GOOGLE_APPLICATION_CREDENTIALS}
```

See [Environment Variables](../environment-variables.md) for details.

<!-- schema:auto-generated:start:OriginSpec -->

## Attributes

### type: DBT

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `connection` | string | Yes | - | Connection resource name for auto-generated Assets. |
| `projectDir` | string | Yes | - | Local path to the dbt project directory (relative or absolute). |
| `autoSync` | boolean | — | - | Override `autoSync` for all auto-generated Assets. When `None`, each Asset uses its own default (`true`). |
| `defaultSync` | DefaultSync | — | - | User-defined Sync to override the auto-generated Sync (e.g. `my-project-dbt-run` for Origin named `my-project`). |

<!-- schema:auto-generated:end:OriginSpec -->
