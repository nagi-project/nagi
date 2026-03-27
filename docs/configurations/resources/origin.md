# kind: Origin

外部プロジェクトから Asset を自動生成するリソースです。[`nagi compile`](../../cli.md#compile) 時に外部プロジェクトの定義を読み取り、Asset / Source / Connection / Sync を自動生成します。

```yaml
apiVersion: nagi.io/v1alpha1
kind: Origin
metadata:
  name: my-dbt-project
spec:
  type: DBT
  connection: my-bigquery
  projectDir: ../dbt-project
  defaultSync: dbt-default
  autoSync: false           # Optional. Propagates to all auto-generated Assets.
```

<!-- schema:auto-generated:start:OriginSpec -->

## Attributes

### type: DBT

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `connection` | string | Yes | - | Connection resource name for auto-generated Assets. |
| `projectDir` | string | Yes | - | Local path to the dbt project directory (relative or absolute). |
| `autoSync` | boolean | — | - | Override `autoSync` for all auto-generated Assets. When `None`, each Asset uses its own default (`true`). |
| `defaultSync` | string | — | - | Name of the Sync resource applied to all auto-generated Assets unless overridden. |

<!-- schema:auto-generated:end:OriginSpec -->
