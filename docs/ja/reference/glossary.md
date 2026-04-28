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

Halt
: すべての Asset を一括で Suspended 状態にする操作（`nagi serve halt`）。

Resume
: 選択した Asset の Suspended 状態を解除する操作（`nagi serve resume`）。

## Resources

Asset
: 期待状態が宣言され、Nagi が evaluate の対象とするデータの単位。ドリフトしたときに対応する Sync を実行する。

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

Cooldown
: Sync 失敗後、次の Sync 起動が一時的に抑制される状態。連続失敗ごとに待機時間が倍増する。時間経過または Sync 成功で自動的に解除される。

Suspended
: Asset の Sync が停止された状態。Evaluate は継続する。

## Serve Architecture

Controller
: Evaluate と Sync のスケジューリングを管理する実行単位。

Guardrails
: Sync による状態悪化や繰り返しの失敗から被害が拡大することを防ぐ仕組み。

Graceful Shutdown
: 停止シグナルを受けたとき、実行中の Sync の完了を待ってから終了する動作。
