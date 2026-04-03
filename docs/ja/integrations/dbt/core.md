# dbt Core

Nagi は [kind: Origin](../../reference/resources/origin.md) を通じて dbt プロジェクトから Nagi のリソースを自動生成します。

## Prerequisites

Nagi を実行する環境に [dbt CLI](https://docs.getdbt.com/docs/core/installation-overview)（dbt-core >= 1.0）のインストールが必要です。

## Init

[`nagi init`](../../reference/cli.md#init) を実行すると、以下のリソースが `resources/` に生成されます。
実行する際に dbt プロジェクトのパスと、使用する profile / target を指定してください。

| dbt configuration | 生成されるリソース |
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

Connection の `profile` と `target` は、dbt コマンドでの `--profile` / `--target` オプションとして渡されます。
Origin の `projectDir` は `--project-dir` オプションで使われます。

## Compile

[`nagi compile`](../../reference/cli.md#compile) を実行すると、Origin の定義をもとに dbt プロジェクトを読み取り、Asset / Conditions / Sync を `target/` に生成します。

Asset 名は `{Origin名}.{model名}` の形式で生成されます。

リソース生成のマッピングとマージの動作は [Resource Generation](./resource-generation.md) を参照してください。

## Evaluate

Evaluate は Nagi が直接実行します。

- Freshness / SQL 条件: Nagi が Connection の接続情報でデータウェアハウスに直接クエリを発行します
- Command 条件（dbt test）: dbt CLI をサブプロセスとして実行します

## Sync

dbt CLI をサブプロセスとして実行します（例: `dbt run --select daily_sales`）。

## Customization

Origin が自動生成した Asset と同じ名前で Asset を `resources/` に定義すると、`onDrift` がリスト結合されます。マージのルールと具体例は [Resource Generation - Merge with User-defined Resources](./resource-generation.md#merge-with-user-defined-resources) を参照してください。

## Multi Projects

Origin として複数の dbt プロジェクトを設定した場合、Nagi はプロジェクト間の依存関係を自動的にマッピングします。マッピングは dbt の Relation（`database.schema.identifier`）をもとに行います。
