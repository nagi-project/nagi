# dbt Cloud

dbt Cloud を使用している環境での Nagi の設定と動作について解説します。

## Prerequisites

Nagi を実行する環境に [dbt CLI](https://docs.getdbt.com/docs/core/installation-overview)（dbt-core >= 1.0）のインストールが必要です。

## Setup

[kind: Connection](../configurations/resources/connection.md) に `dbtCloud` フィールドを追加します。

```yaml
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: my-bigquery
spec:
  dbtProfile:
    profile: my_project
    target: dev
  dbtCloud:
    credentialsFile: ~/.dbt/dbt_cloud.yml
```

`credentialsFile` は省略可能で、デフォルトは `~/.dbt/dbt_cloud.yml` です。

この認証ファイルは [dbt Cloud の UI](https://docs.getdbt.com/docs/cloud/configure-cloud-cli#configure-the-dbt-cli) からダウンロードできます（プロジェクト設定の CLI セクション → "Download CLI configuration file"）。`~/.dbt/dbt_cloud.yml` に保存してください。

## Compile

[dbt Core](./dbt-core.md) と同じく、ローカルにある dbt プロジェクトに対して `dbt compile` を実行し `manifest.json` を生成します。

## Evaluate

Evaluate は Nagi が直接実行します。

- **Freshness / SQL 条件**: Nagi が Connection の接続情報で DWH に直接クエリを発行します
- **Command 条件（dbt test）**: dbt CLI をサブプロセスとして実行します

## Sync

dbt CLI をサブプロセスとして実行します（例: `dbt run --select daily_sales`）。

## Sync Control

`dbtCloud` が設定されている場合、Nagi は Sync 実行前に dbt Cloud API で実行中のジョブを確認します。compile 時に各 dbt Cloud ジョブの `execute_steps` を解析し、対象 Asset を含むジョブを特定します。Sync 実行時にそのジョブが実行中であれば Sync を中断します。

1. compile 時に dbt Cloud Jobs API から全ジョブを取得し、`execute_steps` の `--select` からモデル名を抽出して Asset ごとに関連ジョブを特定する
2. Sync 実行時に Runs API で実行中のジョブを取得し、対象 Asset に関連するジョブが実行中か確認する
    - 関連ジョブが実行中でなければ、Sync を実行する
    - 関連ジョブが実行中であれば、エラーを返す

```text
dbt Cloud has running jobs that include asset 'daily-sales':
  job-42 (Running)
Use --force to override.
```

`--force` フラグを指定すると、実行中ジョブの有無にかかわらず Sync を実行します。

```bash
nagi sync --select daily-sales --force
```

`nagi serve` では `--force` は指定できません。実行中ジョブがある場合はその Sync がエラーとなり、次の評価サイクルで再試行されます。

## Customization

[dbt Core](./dbt-core.md#customization) と同じです。Origin が自動生成する Asset と同名の Asset を `resources/` に作成すると、`onDrift` がリスト結合されます。
