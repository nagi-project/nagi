# Get Started

## Install

```bash
pip install nagi-cli
```

!!! tip
    セットアップなしですぐに試してみたい場合は [Quickstart](./quickstart.md) を参照してください。

## Project Structure

このページでは、`my-project/` ディレクトリで Nagi をセットアップする前提で記載します。

セットアップが完了すると、ディレクトリの構造は以下のようになります。

```text
my-project/
├── nagi.yaml          # Nagi の設定ファイル
├── resources/         # リソース定義を配置するディレクトリ
│   └── ...
└── target/            # Nagi が Sync や Evaluate で参照するディレクトリ
    ├── assets/        # コンパイル後のリソース定義を配置するディレクトリ
    │   └── ...
    └── graph.json     # Asset の依存グラフ
```

## Setup

### 1. Init

```bash
nagi init
```

対話形式で `nagi.yaml` と `resources/` ディレクトリを生成します。

他ソフトウェアと連携する場合は、[Origin](../reference/resources/origin.md) と [Connection](../reference/resources/connection.md) の設定も合わせて行います。

これらを設定すると、連携対象のソフトウェア設定を読み取って、Nagi の定義ファイルを自動生成できます。対応しているソフトウェアは [Integrations](../integrations/index.md) を参照してください。

また、Origin では自動収束の一括設定ができます。目的に応じて設定してください。

| 目的 | 設定する項目 |
| --- | --- |
| 期待状態の評価から始める | `autoSync: false` を設定 |
| 状態評価と収束操作を行う | `autoSync: true` を設定 |

init 完了後に、下記の状態になっていることを確認してください。

```text
my-project/
├── nagi.yaml
└── resources/
    ├── connection.yaml    # Origin 設定時のみ
    └── origin.yaml        # Origin 設定時のみ
```

### 2. Define Resources

`resources/` にリソース定義用の YAML ファイルを配置します。

Origin を設定済みの場合、Asset は `nagi compile` で自動生成されるため、ここでは [Conditions](../reference/resources/conditions.md) を配置します。

Origin を使用しない場合は、[Asset](../reference/resources/asset.md)、[Conditions](../reference/resources/conditions.md)、[Connection](../reference/resources/connection.md) を配置します。

各リソースの定義方法は [Resource Configurations](../reference/resources/index.md) を参照してください。

### 3. Compile

```bash
nagi compile
```

`resources/` のリソース定義を検証・結合し、Evaluate と Sync の設定情報として `target/` へ出力します。

```text
my-project/
├── nagi.yaml
├── resources/
│   └── ...
└── target/                # nagi compile で生成
    ├── assets/
    │   └── *.yaml         # コンパイル済み Asset
    └── graph.json          # 依存グラフ
```

### 4. Evaluate

```bash
nagi evaluate
```

`target/` の Asset に対して期待状態の評価を実行します。すべて満たしていれば Ready、1つでも満たしていなければ Drifted と判定されます。

特定の Asset のみを評価するには `--select` を使います。

```bash
nagi evaluate --select <asset-name>
```

## What's Next

- [Concepts — From Monitoring to Automation](./concepts.md#from-monitoring-to-automation) — 自動収束へ段階的に進める流れを知る
- [`nagi sync`](../reference/cli.md#sync) — 収束操作を手動実行する
- [`nagi serve`](../reference/cli.md#serve) — 評価と収束のループを継続的に実行する
- [Serve Internals](../architecture/serve/internals.md) — ループの仕組みを知る
