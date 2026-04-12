# kind: Origin

Asset を自動生成するリソースです。[`nagi compile`](../cli.md#compile) の実行時に、他のソフトウェアが持つデータの構成情報を読み取り、Asset / Conditions / Sync を自動生成します。

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

Origin が生成する Asset の名前には Origin 名がプレフィックスとして付与されます: `{origin}.{model}`

上の例（Origin 名 `my-dbt-project`）の場合:

- dbt model `orders` → Asset `my-dbt-project.orders`
- dbt source `raw.customers` → Asset `my-dbt-project.raw.customers`

!!! tip
    ユーザー定義の Asset の `upstreams` から Origin 生成 Asset を参照する場合は `{Origin名}.{model名}` 形式の名前を指定します。

## Auto-generated Sync

`type: DBT` は `{origin}-dbt-run`（例: `my-dbt-project-dbt-run`）という Sync リソースを自動生成します:

```yaml
kind: Sync
metadata:
  name: my-dbt-project-dbt-run
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.modelName }}", "--project-dir", "../dbt-project", "--profile", "my_project", "--target", "dev"]
```

`--project-dir`、`--profile`、`--target` は Origin の Connection から compile 時に解決されます。`{{ asset.modelName }}` は[テンプレート変数](./index.md#template-variables)で、compile 時に Origin プレフィックスなしの dbt model 名（例: `orders`）に置き換えられます。

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
      selector: "+{{ asset.modelName }}"
```

`defaultSync` を指定した場合、`{origin}-dbt-run` Sync は生成されません。

## Environment Variables

`type: DBT` の Origin は `env` フィールドで環境変数を宣言できます。これらは `dbt compile` のサブプロセスに渡されます。サブプロセスには、ここで宣言した値と OS 動作に必要な最小セットのみが渡されます。親シェルの環境変数は引き継がれません。値は `${VAR}` 形式で Nagi プロセスの環境変数を参照できます。

`profiles.yml` で `{{ env_var('GOOGLE_APPLICATION_CREDENTIALS') }}` のように環境変数を参照している場合は、`env` に宣言してください。

```yaml
spec:
  type: DBT
  connection: my-bigquery
  projectDir: ../dbt-project
  env:
    GOOGLE_APPLICATION_CREDENTIALS: ${GOOGLE_APPLICATION_CREDENTIALS}
```

詳細は[環境変数](../environment-variables.md)を参照してください。

<!-- schema:auto-generated:start:OriginSpec -->

## Attributes

### type: DBT

| Attribute | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `connection` | string | Yes | - | Connection resource name for auto-generated Assets. |
| `projectDir` | string | Yes | - | Local path to the dbt project directory (relative or absolute). |
| `autoSync` | boolean | — | - | Override `autoSync` for all auto-generated Assets. When `None`, each Asset uses its own default (`true`). |
| `defaultSync` | DefaultSync | — | - | User-defined Sync to override the auto-generated Sync (e.g. `my-project-dbt-run` for Origin named `my-project`). |
| `env` | map[string, string] | — | {} | Environment variables passed to the `dbt compile` subprocess. Values may reference the parent process env via `${VAR}`. |

<!-- schema:auto-generated:end:OriginSpec -->
