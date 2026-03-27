# Glossary

## Core Concepts

Reconciliation Loop
: 条件の評価と収束操作を継続的に繰り返すループ。Drifted を検知したら収束操作を実行し、Ready に戻るまで繰り返す。

Evaluate
: データが条件を満たしているか（Ready）、逸脱しているか（Drifted）を判定する操作。

Convergence
: Drifted な状態を Ready に戻す操作。Asset の `onDrift` で条件ごとに異なる Sync を指定できる。

## States

Ready
: Asset のすべての条件が満たされている状態。

Drifted
: Asset の条件が1つ以上満たされていない状態。対応する Sync が実行される。

Suspended
: Guardrails により Sync の実行が停止された状態。Evaluate は継続する。`nagi serve resume` で再開できる。

Halted
: `nagi serve halt` により全 Asset が一括停止された状態。

## Resources

Asset
: データの単位。データウェアハウスのテーブルやビューに対応する。`onDrift` で条件と収束操作のペアを定義する。

Conditions
: 再利用可能な条件のセット。`kind: Conditions` として定義し、複数の Asset から共有できる。

Source
: Asset が依存する上流データ。Connection への参照を持つ。

Connection
: データウェアハウスへの接続情報。dbt の `profiles.yml` を参照する形で定義する。

Sync
: 収束操作の定義。pre → run → post の3ステージで構成される。pre / post は省略可。

Origin
: 外部プロジェクトから Asset を自動生成するリソース。現在は dbt をサポートする。

## Asset Fields

onDrift
: 条件と Sync のペアをリストで定義するフィールド。エントリは上から順に評価され、最初に条件が Drifted のエントリの Sync が実行される（first-match）。

autoSync
: `nagi serve` での自動 Sync 実行を制御するフラグ。`true`（デフォルト）で自動実行、`false` で Evaluate のみ。

## Condition Types

Freshness
: データの鮮度を評価する条件。`maxAge` を超えると Drifted になる。`column` 指定時はカラム値、省略時はテーブルメタデータから最終更新時刻を取得する。

SQL
: SQL クエリの結果で評価する条件。スカラーの boolean を返し、`true` で Ready。

Command
: 外部コマンドを実行して評価する条件。exit code 0 で Ready。argv 形式で指定する。

## Condition Fields

interval
: 条件の定期的な再評価間隔。Freshness では必須、SQL / Command では省略可（省略時は上流伝播のみで評価）。

checkAt
: Freshness 条件に対するオプションの cron 式。`interval` による定期評価に加えて、特定時刻にも鮮度を確認する。

## Serve Architecture

Controller
: `nagi serve` 内の非同期イベントループ。依存グラフの連結成分ごとに1つ起動され、Evaluate と Sync のスケジューリングを管理する。

Guardrails
: Sync 実行後の状態悪化（Ready な条件が減少）や連続失敗を検知し、Asset の Sync を自動停止する安全装置。

Upstream Propagation
: 上流 Asset が Drifted → Ready に遷移したとき、下流 Asset の Sync を Evaluate をスキップして直接起動する仕組み。Sync 完了後に re-evaluate で収束結果を確認する。上流が Drifted の間、下流の Evaluate と Sync はすべてブロックされる。

Source Change Detection
: Source テーブルの統計値（行数・バイト数）が前回から変化していない場合にキャッシュ済みの評価結果を返し、データウェアハウスへのクエリを省略するための最適化。

SyncLock
: 同じ Asset に対する Sync の同時実行を防ぐ排他ロック。TTL 付きでデッドロックを防止する。

Graceful Shutdown
: `Ctrl-C` による停止時に新規タスクの発行を停止し、実行中の Sync サブプロセスの完了を待つ。待機上限は `terminationGracePeriodSeconds` で設定。

## CLI & Compilation

compile
: `resources/` のリソース定義を検証・解決し、`target/` にコンパイル済み Asset と依存グラフを出力するコマンド。

ls
: コンパイル済みリソースを JSON で一覧表示するコマンド。

export
: 実行ログ（`logs.db`）をデータウェアハウスにエクスポートするコマンド。ウォーターマークによる差分転送を行う。

resources
: ユーザーが定義するリソース YAML の配置ディレクトリ。

target
: `nagi compile` の出力ディレクトリ。コンパイル済み Asset YAML と `graph.json` が格納される。

select
: 対象 Asset をフィルタリングする構文。名前指定（`daily-sales`）、上流/下流（`+name` / `name+`）、タグ（`tag:finance`）をサポートする。
