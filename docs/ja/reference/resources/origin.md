# kind: Origin

Asset を自動生成するリソースです。[`nagi compile`](../cli.md#compile) 時に他のソフトウェアが持つデータの構成情報を読み取り、Asset / Conditions / Sync を自動生成します。

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

`type: DBT` は `nagi-dbt-run` という Sync リソースを自動生成します:

```yaml
kind: Sync
metadata:
  name: nagi-dbt-run
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.name }}"]
```

dbt テストを持つモデルの Asset は、この Sync を参照する `onDrift` エントリを持ちます。

## Overriding with defaultSync

自動生成される Sync の代わりにユーザー定義の Sync を使いたい場合は `defaultSync` で指定します。`onDrift` エントリと同じインターフェース（`sync` + `with`）を持ちます:

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

`defaultSync` を指定した場合、`nagi-dbt-run` Sync は生成されません。

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
