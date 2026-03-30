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

## Auto-generated Sync

`type: DBT` auto-generates a Sync resource named `nagi-dbt-run`:

```yaml
kind: Sync
metadata:
  name: nagi-dbt-run
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.name }}"]
```

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
      selector: "+{{ asset.name }}"
```

When `defaultSync` is specified, the `nagi-dbt-run` Sync is not generated.

<!-- schema:auto-generated:start:OriginSpec -->

## Attributes

### type: DBT

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `connection` | string | Yes | - | Connection resource name for auto-generated Assets. |
| `projectDir` | string | Yes | - | Local path to the dbt project directory (relative or absolute). |
| `autoSync` | boolean | — | - | Override `autoSync` for all auto-generated Assets. When `None`, each Asset uses its own default (`true`). |
| `defaultSync` | DefaultSync | — | - | User-defined Sync to override the auto-generated `nagi-dbt-run`. |

<!-- schema:auto-generated:end:OriginSpec -->
