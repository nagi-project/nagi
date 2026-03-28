# dbt Cloud

dbt Cloud を使用している環境での Nagi の利用方法について記載しています。

## Recommended Approach

dbt Cloud 環境と併用する場合は、Nagi は状態評価と通知から始めることを推奨します。[Origin](../../reference/resources/origin.md) に `autoSync: false` を設定すると、自動生成されるすべての Asset に適用されます。

```yaml
kind: Origin
spec:
  type: DBT
  connection: my-bigquery
  projectDir: ../dbt-project
  autoSync: false
```

dbt Cloud のジョブが既存のスケジュールでモデルを更新し、Nagi は定期的にデータの状態を検査して drift を検出したら通知します。段階的に自動収束へ進める流れは [Concepts — From Monitoring to Automation](../../overview/concepts.md#from-monitoring-to-automation) を参照してください。

## Prerequisites

Nagi を実行する環境に [dbt CLI](https://docs.getdbt.com/docs/core/installation-overview)（dbt-core >= 1.0）のインストールが必要です。

## Init

[`nagi init`](../../reference/cli.md#init) を実行すると、[dbt Core](./core.md#init) と同様に Connection と Origin が `resources/` に生成されます。

dbt Cloud を使用する場合は、生成された Connection に `dbtCloud` フィールドを追加します。

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

[dbt Core](./core.md) と同じく、ローカルにある dbt プロジェクトに対して `dbt compile` を実行し `manifest.json` を生成します。リソース生成の詳細は [Resource Generation](./resource-generation.md) を参照してください。

## Evaluate

[dbt Core](./core.md#evaluate) と同じです。

## Sync

Sync を実行する場合は、dbt Core と同様に dbt CLI をサブプロセスとして実行します。

dbt Cloud 環境では、Nagi の Sync と dbt Cloud のジョブが同じモデルを同時に更新する可能性があります。これを防ぐため、Sync Control による実行中ジョブの確認を組み込んでいます。

## Sync Control

Asset の Connection に `dbtCloud` が設定されている場合、Sync 実行前に dbt Cloud で実行中のジョブがないか確認します。compile 時に各 dbt Cloud ジョブの `execute_steps` を解析し、対象 Asset を含むジョブを特定します。Sync 実行時にそのジョブが実行中であれば Sync を中断します。

1. compile 時に Jobs API から全ジョブを取得し、`execute_steps` の `--select` からモデル名を抽出して Asset ごとに関連ジョブを特定する
2. Sync 実行時に Runs API で実行中のジョブを取得し、対象 Asset に関連するジョブが実行中か確認する
    - 関連ジョブが実行中でなければ、Sync を実行する
    - 関連ジョブが実行中であれば、エラーを返す

### API

Nagi は以下の dbt Cloud Administrative API エンドポイントを使用します。

| タイミング | API エンドポイント | 用途 |
| --- | --- | --- |
| compile | `GET /api/v2/accounts/{account_id}/jobs/` | 全ジョブの `execute_steps` を取得し、Asset とジョブの対応を構築する |
| sync | `GET /api/v2/accounts/{account_id}/runs/?status=3` | 実行中のジョブ（status=3: Running）を取得し、対象 Asset に関連するジョブが実行中か確認する |

### Required Permissions

dbt Cloud の API トークンには以下の権限が必要です。

| 権限 | 理由 |
| --- | --- |
| Jobs の読み取り（Read） | ジョブ定義と `execute_steps` の取得 |
| Runs の読み取り（Read） | 実行中ジョブの確認 |

`~/.dbt/dbt_cloud.yml` に含まれる `token-value` がこの API 認証に使用されます。トークンは API 呼び出し時にファイルから読み取られますが、メモリには保持されません。

## Customization

[dbt Core](./core.md#customization) と同じです。
