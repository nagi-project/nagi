# Glossary

## Core Concepts

期待状態
: データがどのような状態であるべきかの宣言。Nagi はこの宣言に基づいてデータを評価し、ドリフトを検知したら収束させる。

評価
: データが期待状態を満たしているかを判定すること。結果は Ready（期待状態にある）または Drifted（ドリフトしている）のいずれかになる。

収束
: 期待状態からドリフトしたデータを期待状態へ戻す過程。

Reconciliation Loop
: 評価と収束を継続的に繰り返すことで、データを期待状態に保ち続けるループ。

## Operations

Evaluate
: 評価を実行する操作。データの状態を判定するだけで、データへの変更は行わない。

Sync
: 収束を実現するための具体的なアクション。データを期待状態へ戻すための操作を実行する。

## Resources

Asset
: Nagi が監視するデータウェアハウスのテーブルやビュー。期待状態と、ドリフトしたときに実行する Sync を宣言する。

Conditions
: 期待状態の定義をまとめたリソース。複数の Asset から参照できる。

Connection
: データウェアハウスへの接続情報。

Sync
: 収束アクションの定義。複数の Asset から参照できる。

Origin
: Asset を自動生成するリソース。他のソフトウェアが持つデータの構成情報から Asset を生成する。

## Asset States

Ready
: Asset のすべての期待状態が満たされている状態。

Drifted
: データが期待状態からドリフトした状態。対応する Sync が実行される。

Suspended
: Sync が自動停止された状態。Sync 実行後に状態が悪化した場合や、Sync が連続して失敗した場合に発生する。

Halted
: Asset の Sync を一括停止した状態。

## Serve Architecture

Controller
: Evaluate と Sync のスケジューリングを管理する実行単位。

Guardrails
: Sync による状態悪化の拡大を防ぐ仕組み。状態悪化や連続失敗を検知すると、当該 Asset の Sync を自動停止する。

Graceful Shutdown
: 停止シグナルを受けたとき、実行中の Sync の完了を待ってから終了する動作。
