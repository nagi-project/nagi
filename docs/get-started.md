# Get Started

Nagi を使いはじめるにあたって必要となる作業について説明します。

## Install

```bash
pip install nagi-cli
```

!!! tip
    サンプルプロジェクトですぐに動かしたい場合は [Quickstart](./quickstart.md) を参照してください。

## Setup

### 1. Init

```bash
nagi init
```

対話形式で以下を行います。

- `resources/` ディレクトリと `nagi.yaml` の生成

dbt を使用している場合は、続けて以下も設定します。

- dbt プロジェクトのパスと profile / target の設定
- Connection / Origin リソースの生成
- dbt による接続確認

### 2. Define Resources

`nagi init` が生成した `resources/` ディレクトリにリソース YAML ファイルを配置します。Nagi のリソースは kind で種別を指定します。

| kind | 役割 |
| --- | --- |
| [Connection](./configurations/resources/connection.md) | データウェアハウスへの接続設定 |
| [Conditions](./configurations/resources/conditions.md) | 期待状態の定義 |
| [Sync](./configurations/resources/sync.md) | 収束操作の定義 |
| [Asset](./configurations/resources/asset.md) | データのまとまりの定義。evaluate と sync の実行対象。`kind: Conditions` と `kind: sync` のペアを定義する |
| [Source](./configurations/resources/source.md) | Asset が依存するデータソース |
| [Origin](./configurations/resources/origin.md) | 他のソフトウェア情報からリソースを自動生成するための設定 |

以下は、テーブルの鮮度を監視して dbt model を実行する例です。

```yaml
# resources/connection.yaml
apiVersion: nagi.io/v1alpha1
kind: Connection
metadata:
  name: my-dwh
spec:
  dbtProfile:
    profile: my_profile
    target: dev
```

```yaml
# resources/conditions.yaml
apiVersion: nagi.io/v1alpha1
kind: Conditions
metadata:
  name: freshness-daily-sales
spec:
  - type: Freshness
    name: freshness-check
    interval: 15m
    maxAge: 24h
```

```yaml
# resources/sync.yaml
apiVersion: nagi.io/v1alpha1
kind: Sync
metadata:
  name: my-sync
spec:
  run:
    type: Command
    args: ["dbt", "run", "--select", "{{ asset.name }}"]
```

```yaml
# resources/asset.yaml
apiVersion: nagi.io/v1alpha1
kind: Asset
metadata:
  name: daily-sales
spec:
  sources:
    - raw.sales
  onDrift:
    - sync: my-sync
      conditions: freshness-daily-sales
```

各リソースの詳細は [Resource Configurations](./configurations/resources/index.md) を参照してください。

### 3. Compile

```bash
nagi compile
```

`resources/` のリソースを解決し、`target/` にコンパイル済み Asset と依存グラフ（`graph.json`）を出力します。Origin が定義されている場合は dbt プロジェクトから Asset を自動生成します。

## Manual Operations

以下のコマンドで、データの状態確認と収束操作を手動で行えます。各コマンドの詳細は [CLI Reference](./cli.md) を参照してください。

### Status Check

```bash
nagi evaluate                        # 全 Asset の条件を評価
nagi evaluate --select daily-sales   # 指定 Asset のみ評価
nagi evaluate --dry-run              # 評価対象の条件を確認。Evaluate は実行しない
nagi status                          # キャッシュされた最新の評価結果を確認
```

### Sync

```bash
nagi sync                            # 全 Asset を sync
nagi sync --select daily-sales       # 指定 Asset のみ sync
nagi sync --dry-run                  # 実行されるコマンドを確認。Sync は実行しない
```

`nagi sync` は実行前にプランを表示し、ユーザーの承認を求めます。完了後は自動で evaluate を実行します。

### Resource List

```bash
nagi ls                              # コンパイル済みリソースを一覧表示
```

### Export

[`nagi.yaml`](./configurations/project.md) の `export` を設定すると、実行ログをデータウェアハウスへエクスポートできます。

```bash
nagi export                          # 全テーブルの差分を転送
nagi export --dry-run                # 未エクスポートの行数を確認
nagi export --select sync_logs       # 指定テーブルのみ転送
```

## Continuous Reconciliation

[`nagi serve`](./cli.md#serve) で継続的な reconciliation loop を開始します。起動時に自動で compile を実行するため、`nagi compile` の手動実行は不要です。

```bash
nagi serve
nagi serve --select tag:finance    # 特定の Asset のみ監視
```

リソース定義を変更した場合は `Ctrl-C` で停止し、再度 `nagi serve` を実行してください。停止と再起動の動作については [Serve Architecture](./architecture/serve.md) を参照してください。

### Serve Control

[`nagi serve`](./cli.md#serve) の実行中に Guardrails によって停止された Asset を管理できます。

```bash
nagi serve resume                       # 停止した Asset を対話形式で再開
nagi serve resume --select daily-sales  # 指定 Asset を再開
nagi serve halt                         # 全 Asset を一括停止
```

[nagi.yaml](./configurations/project.md) の `backend.type` でリモートバックエンドを選択すると、[`nagi serve`](./cli.md#serve) が動作しているリモートサーバーと異なる環境からでも実行できます。
