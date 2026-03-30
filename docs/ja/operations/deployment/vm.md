# VM

VM やオンプレミスサーバーで常駐プロセスとして運用するパターンです。

## Setup

`nagi serve` を systemd や supervisord 等のプロセスマネージャーで管理します。プロセスが異常終了した場合は自動で再起動されます。

再起動時は前回の状態から再開します。詳細は [Serve Restart](../../architecture/serve/restart.md) を参照してください。

## Graceful Shutdown

停止シグナルを受け取ると、新規タスクの発行を停止し、実行中の Sync の完了を待ちます。待機上限は `nagi.yaml` の `terminationGracePeriodSeconds` で設定できます。

## Storage Backend

`local` バックエンドを使用します。

```yaml
# nagi.yaml
backend:
  type: local
```
