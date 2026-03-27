# kind: Connection

データウェアハウスへの接続情報です。Source から参照され、Evaluate 時のクエリ実行に使用されます。

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

<!-- schema:auto-generated:start:ConnectionSpec -->

## Attributes

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `dbtProfile` | any | Yes | - | Required in MVP because profiles.yml is the only supported connection resolution mechanism. SQL-based conditions (Freshness, SQL) rely on it to resolve the adapter config. When direct connection configuration (host/port/credentials etc.) is added, this should become `Option<DbtProfile>` alongside alternative connection variants. |
| `dbtCloud` | DbtCloudSpec | — | - | Optional dbt Cloud configuration for running-job checks before sync. |

<!-- schema:auto-generated:end:ConnectionSpec -->
