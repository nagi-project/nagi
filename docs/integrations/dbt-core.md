# dbt Core

Nagi は [kind: Origin](../configurations/resources/origin.md) を通じて dbt プロジェクトから Nagi のリソースを自動生成します。

## Prerequisites

Nagi を実行する環境に [dbt CLI](https://docs.getdbt.com/docs/core/installation-overview)（dbt-core >= 1.0）のインストールが必要です。

## Init

[`nagi init`](../cli.md#init) を実行すると、以下のリソースが `resources/` に生成されます。
実行する際に dbt プロジェクトのパスと、使用する profile / target を指定してください。

| dbt configuration | 生成されるリソース |
| --- | --- |
| profile / target | [kind: Connection](../configurations/resources/connection.md) |
| project | [kind: Origin](../configurations/resources/origin.md) |

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
  defaultSync: dbt-default
```

Connection の `profile` と `target` は、dbt コマンドでの `--profile` / `--target` オプションとして渡されます。
Origin の `projectDir` は `--project-dir` オプションで使われます。

## Compile

[`nagi compile`](../cli.md#compile) を実行すると、Origin が dbt プロジェクトを読み取り、以下のリソースを `target/` に生成します。

| dbt resource | 生成されるリソース |
| --- | --- |
| model | [kind: Asset](../configurations/resources/asset.md) |
| test (on model) | [kind: Conditions](../configurations/resources/conditions.md) + Asset の `onDrift` |
| test (on source) | [kind: Conditions](../configurations/resources/conditions.md) + Asset の `onDrift`（`nagi-skip-sync` を使用） |
| source | [kind: Asset](../configurations/resources/asset.md)（`upstreams` なし、テストがあれば `onDrift` 付き） |

dbt source は Nagi では Asset として生成されます。dbt source にテストが定義されている場合、`onDrift` に条件が設定され、`nagi-skip-sync`（何もせず正常終了する Sync）が割り当てられます。Drifted になると下流 Asset がブロックされ、外部で修復されるまで待機します。

dbt model と dbt source のリネージは Asset の `spec.upstreams` として依存関係に変換されます。

## Evaluate

Evaluate は Nagi が直接実行します。

- **Freshness / SQL 条件**: Nagi が Connection の接続情報で DWH に直接クエリを発行します
- **Command 条件（dbt test）**: dbt CLI をサブプロセスとして実行します

## Sync

dbt CLI をサブプロセスとして実行します（例: `dbt run --select daily_sales`）。

## Customization

Origin が自動生成した Asset と同名の Asset を `resources/` に手書きすると、[`nagi compile`](../cli.md#compile) の中で `onDrift` がリスト結合されます。先に出現した Asset（Origin 自動生成）のフィールド（`tags`, `sources` 等）が維持され、後の Asset（ユーザー定義）の `onDrift` エントリが追加されます。

これにより、dbt test 由来の Conditions はそのまま維持しつつ、鮮度条件などを個別に追加できます。

### Example

ユーザー定義の Conditions と Asset

```yaml
# resources/daily-sales-freshness.yaml（ユーザー定義）
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: daily-sales-freshness
spec:
  - name: data-freshness
    type: Freshness
    maxAge: 24h
    interval: 6h
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: daily-sales-freshness
      sync: dbt-default
```

[`nagi compile`](../cli.md#compile) が Origin から自動生成する Conditions と Asset

```yaml
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: dbt-tests-daily-sales
spec:
  - name: dbt-test-daily-sales
    type: Command
    run: [dbt, test, --select, daily_sales]
---
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: dbt-tests-daily-sales
      sync: dbt-default
```

[`nagi compile`](../cli.md#compile) でマージされた結果（Asset の `onDrift` がリスト結合される）

```yaml
# target/assets/daily-sales.yaml（compile 後）
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  onDrift:
    - conditions: dbt-tests-daily-sales
      sync: dbt-default
    - conditions: daily-sales-freshness
      sync: dbt-default
```
