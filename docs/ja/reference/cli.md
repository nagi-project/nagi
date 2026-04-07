# CLI

## Install

インストール方法は [Get Started](../overview/get-started.md#install) を参照してください。

## Output

すべてのコマンドの出力形式は JSON です。

## Subcommands

| サブコマンド | 説明 |
| --- | --- |
| `init` | 環境を準備し、`compile` が実行できる状態にする |
| `compile` | Asset 定義をコンパイルし、`target/` に出力する |
| `ls` | コンパイル済みリソースの一覧を表示する |
| `evaluate` | Asset の期待状態を評価する |
| `status` | キャッシュされた評価結果と直近の sync ログを表示する |
| `sync` | Asset の sync を実行する |
| `serve` | Asset をコンパイルし、reconciliation loop を開始する |
| `serve resume` | 停止した Asset を再開する |
| `serve halt` | 全 Asset を一括停止する |
| `export` | 実行ログをデータウェアハウス にエクスポートする |
| `mcp` | MCP サーバーを stdio で起動する |

## init

環境を準備し、`compile` が実行できる状態にします。

```bash
nagi init
```

対話形式で Origin の設定、Connection の生成、接続確認を行います。Origin type を選択し、type に応じた設定を進めます。冪等で、再実行可能です。

## compile

`resources/` のリソース定義をコンパイルし、`target/` に出力します。既存の `target/` がある場合は上書きの確認を求めます。

```bash
nagi compile [OPTIONS]
```

| オプション | デフォルト | 説明 |
| --- | --- | --- |
| `--resources-dir` | `resources` | 入力ディレクトリ |
| `--target-dir` | `target` | 出力ディレクトリ |
| `-y, --yes` | — | 上書き確認をスキップする |

## ls

コンパイル済みの全リソースを JSON で一覧表示します。

```bash
nagi ls [OPTIONS]
```

| オプション | デフォルト | 説明 |
| --- | --- | --- |
| `--target-dir` | `target` | コンパイル済みディレクトリ |

## evaluate

Asset の期待状態を評価し、Ready / Drifted を判定します。

```bash
nagi evaluate [OPTIONS]
```

| オプション | デフォルト | 説明 |
| --- | --- | --- |
| `--select` | — | 評価対象の Asset を指定 |
| `--exclude` | — | 指定セレクターに一致する Asset を除外 |
| `--target-dir` | `target` | コンパイル済みディレクトリ |
| `--cache-dir` | — | キャッシュディレクトリ |
| `--dry-run` | — | 評価対象の期待状態を表示（クエリやコマンドは実行しない） |

## sync

Asset の sync 操作を実行します。実行前にプランを表示し、ユーザーの承認を求めます。

```bash
nagi sync [OPTIONS]
```

| オプション | デフォルト | 説明 |
| --- | --- | --- |
| `--select` | — | 対象の Asset を指定 |
| `--exclude` | — | 指定セレクターに一致する Asset を除外 |
| `--target-dir` | `target` | コンパイル済みディレクトリ |
| `--stage` | — | 実行するステージ（カンマ区切り: `pre`, `run`, `post`）。指定時は完了後の evaluate を行わない |
| `--cache-dir` | — | キャッシュディレクトリ |
| `--dry-run` | — | 実行されるコマンドを表示（副作用なし） |
| `--force` | — | dbt Cloud の実行中ジョブチェックをスキップする |

## status

キャッシュされた評価結果と直近の sync ログを表示します。Evaluate は実行しません。

```bash
nagi status [OPTIONS]
```

| オプション | デフォルト | 説明 |
| --- | --- | --- |
| `--select` | — | 対象の Asset を指定 |
| `--exclude` | — | 指定セレクターに一致する Asset を除外 |
| `--target-dir` | `target` | コンパイル済みディレクトリ |
| `--cache-dir` | — | キャッシュディレクトリ |

## serve

コンパイルと継続的な reconciliation loop を開始します。

```bash
nagi serve [OPTIONS]
```

| オプション | デフォルト | 説明 |
| --- | --- | --- |
| `--select` | — | 対象の Asset を指定 |
| `--exclude` | — | 指定セレクターに一致する Asset を除外 |
| `--resources-dir` | `resources` | リソースディレクトリ |
| `--target-dir` | `target` | コンパイル済みディレクトリ |
| `--cache-dir` | — | キャッシュディレクトリ |
| `--project-dir` | `.` | プロジェクトディレクトリ |

### serve resume

停止した Asset を再開します。

```bash
nagi serve resume [OPTIONS]
```

| オプション | 説明 |
| --- | --- |
| `--select` | 再開する Asset を指定（省略時は対話式で選択） |

### serve halt

全 Asset を一括停止します。

```bash
nagi serve halt [OPTIONS]
```

| オプション | デフォルト | 説明 |
| --- | --- | --- |
| `--reason` | `manual halt` | 停止理由。suspended ファイルと通知メッセージに含まれる |

## export

実行ログ（`logs.db`）をデータウェアハウス にエクスポートします。[`nagi.yaml`](./project.md) の `export` 設定が必要です。

```bash
nagi export [OPTIONS]
```

| オプション | デフォルト | 説明 |
| --- | --- | --- |
| `--select` | — | エクスポート対象のテーブル名を指定（`evaluate_logs`, `sync_logs`, `sync_evaluations`） |
| `--dry-run` | — | 未エクスポートの行数を表示（転送は行わない） |

## mcp

MCP サーバーを stdio で起動します。

```bash
nagi mcp [OPTIONS]
```

| オプション | 説明 |
| --- | --- |
| `--allow-sync` | sync ツールも公開する |

デフォルトでは読み取り専用ツール（`nagi_status`、`nagi_evaluate`）のみ公開します。

## --select syntax

`--select` はセレクター構文をサポートします。

| 構文 | 説明 |
| --- | --- |
| `name` | 指定した Asset（例: `my-project.orders`） |
| `+name` | 指定した Asset とすべての上流 |
| `name+` | 指定した Asset とすべての下流 |
| `+name+` | 指定した Asset と上流・下流すべて |
| `N+name` | 指定した Asset と上流 N 段 |
| `name+N` | 指定した Asset と下流 N 段 |
| `tag:finance` | タグで選択 |
| `+tag:finance` | タグで選択し、上流を含む |
| `tag:finance,tag:daily` | 積集合 — すべての条件に一致する Asset（AND） |

複数の `--select` 引数は和集合（OR）で結合されます。単一引数内のカンマ区切りパターンは積集合（AND）で結合されます。

```bash
# OR: いずれかのセレクターに一致する Asset
nagi evaluate --select daily-sales --select access-stats

# AND: 両方のタグを持つ Asset
nagi evaluate --select "tag:finance,tag:daily"

# 組み合わせ: (tag:finance AND tag:daily) OR access-stats
nagi evaluate --select "tag:finance,tag:daily" --select access-stats
```

## --exclude syntax

`--exclude` は `--select` と同じセレクター構文を使用します。`--select` の適用後に、`--exclude` に一致する Asset が結果から除外されます。

```bash
# monthly-report 以外のすべての Asset を評価
nagi evaluate --exclude monthly-report

# finance タグの Asset から daily タグの Asset を除外して評価
nagi evaluate --select "tag:finance" --exclude "tag:daily"
```
