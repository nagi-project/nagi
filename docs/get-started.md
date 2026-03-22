# Get Started

## Install

```bash
pip install nagi-cli
```

## Setup

Nagi を使い始めるには、以下の順序でコマンドを実行します。

### 1. Init

```bash
nagi init
```

対話形式で以下を行います。

- dbt プロジェクトのパスと profile / target の設定
- `resources/` ディレクトリと `nagi.yaml` の生成
- Connection / Origin リソースの生成
- dbt による接続確認

完了すると、`resources/` に Connection と Origin の YAML ファイルが生成されます。

### 2. Compile

```bash
nagi compile
```

`resources/` のリソースを解決し、`target/` にコンパイル済み Asset と依存グラフ（`graph.json`）を出力します。Origin が定義されている場合は dbt プロジェクトから Asset を自動生成します。

## Continuous Reconciliation

[`nagi serve`](./cli.md#serve) で継続的な reconciliation loop を開始します。起動時に自動で compile を実行するため、`nagi compile` の手動実行は不要です。

```bash
nagi serve
nagi serve --select tag:finance    # 特定の Asset のみ監視
```

リソース定義を変更した場合は `Ctrl-C` で停止し、再度 `nagi serve` を実行してください。停止時は graceful shutdown により、新規の sync を開始せず、実行中の sync サブプロセスの完了を待ちます。待機時間の上限は `nagi.yaml` の `terminationGracePeriodSeconds` で設定できます（省略時は無期限）。

## Manual Operations

以下のコマンドで、データの状態確認と収束操作を行えます。

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

### Serve Control

[`nagi serve`](./cli.md#serve) の実行中に Guardrails によって停止された Asset を管理できます。

```bash
nagi serve resume                       # 停止した Asset を対話形式で再開
nagi serve resume --select daily-sales  # 指定 Asset を再開
nagi serve halt                         # 全 Asset を一括停止
```

[nagi.yaml](./configurations/project.md) の `backend.type` でリモートバックエンドを選択すると、[`nagi serve`](./cli.md#serve) が動作しているリモートサーバーと異なる環境からでも実行できます。
