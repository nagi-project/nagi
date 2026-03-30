# Notifications

[`nagi serve`](../reference/cli.md#serve) の実行中に発生したイベントを通知します。

## Events

| イベント | 発生条件 |
| --- | --- |
| EvalFailed | Evaluate がエラーで失敗した場合 |
| Suspended | Guardrails が Sync を停止した場合 |
| SyncLockSkipped | Sync のロック取得がリトライ上限に達し、Sync がスキップされた場合 |
| Halted | [`nagi serve halt`](../reference/cli.md#serve-halt) による全 Asset 一括停止 |

!!! tip
    通知が未設定の場合や通知の送信に失敗した場合でも、serve ループの動作には影響しません。送信に失敗した場合は warning レベルのログが出力されます。

## Slack

同一 Asset の通知は Slack スレッドにまとめられます。最初の通知が親メッセージとなり、以降の通知は同じスレッドへ投稿されます。

Slack へ通知するには、以下の準備が必要です。

1. Slack App を作成し、`chat:write` スコープを付与する
2. 通知先チャンネルに App を招待する
3. [nagi.yaml](../reference/project.md) の `notify.slack.channel` に通知先チャンネルを設定する
4. 環境変数 `SLACK_BOT_TOKEN` に Bot Token を設定する
