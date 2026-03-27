# dbt Cloud

dbt Cloud を使用している環境での Nagi の利用方法について記載しています。

## Operation Models

dbt Cloud 環境と併用する場合では、以下の2つの運用モデルがあります。

### a. Nagi orchestrates (dbt Core execution)

Nagi が `dbt run --select <model>` を Asset 単位で実行します。dbt Cloud のジョブは使用しません。`dbtCloud` フィールドは Sync 実行前のジョブ実行中チェックに使用します。

reconciliation loop（evaluate → sync → re-evaluate → upstream propagation）がそのまま機能します。

### b. Nagi observes (dbt Cloud orchestrates)

Nagi は evaluate と通知のみを実行します。Sync は実行しません。dbt Cloud のジョブが既存のスケジュールでモデルを更新し、Nagi は定期的にデータの状態を検査して drift を検出したら通知します。

[Origin](../configurations/resources/origin.md) に `autoSync: false` を設定すると、自動生成される全 Asset に適用されます。

```yaml
kind: Origin
spec:
  type: DBT
  connection: my-bigquery
  projectDir: ../dbt-project
  autoSync: false
```

### Why two models?

`dbt build` は DAG 全体を一括処理するため、Nagi の per-Asset reconciliation loop と粒度が一致しません。Nagi が dbt Cloud ジョブをトリガーすると、以下の問題が発生します。

- **Sync duplication**: 1つのジョブが複数 Asset を更新するのに、Asset ごとに Sync が連鎖する
- **Re-evaluate test duplication**: `dbt build` のテスト実行後に、同じテストを re-evaluate で再実行する

モデル a はこの問題を Asset 単位の `dbt run` で回避し、モデル b は Sync を Nagi の責務外とすることで回避します。

## Prerequisites

Nagi を実行する環境に [dbt CLI](https://docs.getdbt.com/docs/core/installation-overview)（dbt-core >= 1.0）のインストールが必要です。

## Init

[`nagi init`](../cli.md#init) を実行すると、[dbt Core](./dbt-core.md#init) と同様に Connection と Origin が `resources/` に生成されます。

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

[dbt Core](./dbt-core.md) と同じく、ローカルにある dbt プロジェクトに対して `dbt compile` を実行し `manifest.json` を生成します。

## Evaluate

Evaluate は Nagi が直接実行します。

- **Freshness / SQL 条件**: Nagi が Connection の接続情報で データウェアハウスに直接クエリを発行します
- **Command 条件（dbt test）**: dbt CLI をサブプロセスとして実行します

## Sync

dbt CLI をサブプロセスとして実行します（例: `dbt run --select daily_sales`）。

## Sync Control

運用モデル a で Nagi が Sync を実行する場合、`dbtCloud` が設定されていれば Sync 実行前に dbt Cloud API で実行中のジョブを確認します。compile 時に各 dbt Cloud ジョブの `execute_steps` を解析し、対象 Asset を含むジョブを特定します。Sync 実行時にそのジョブが実行中であれば Sync を中断します。

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

[dbt Core](./dbt-core.md#customization) と同じです。Origin が自動生成する Asset と同名の Asset を `resources/` に作成すると、`onDrift` がリスト結合されます。
