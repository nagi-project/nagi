# Container

コンテナ環境で運用するパターンです。

## Setup

`nagi serve` はステートレスに設計されています。コンテナが再起動しても、リモートストレージから前回の状態を読み取って再開します。詳細は [Serve Restart](../../architecture/serve/restart.md) を参照してください。

## Graceful Shutdown

停止シグナルを受け取ると、新規タスクの発行を停止し、実行中の Sync の完了を待ちます。待機上限は `nagi.yaml` の `terminationGracePeriodSeconds` で設定できます。コンテナ環境の停止猶予時間に合わせて設定してください。

## Storage Backend

リモートバックエンド（GCS / S3）を使用します。コンテナの再起動でローカルファイルが失われるため、キャッシュ・ロック・停止フラグをリモートに保存します。

```yaml
# nagi.yaml
backend:
  type: gcs
  bucket: my-nagi-bucket
  prefix: my-project
```

ストレージバックエンドの詳細は [Storage](../../architecture/storage.md) を参照してください。

## Log Export

`logs.db`（実行履歴）は SQLite のためローカルに配置されます。コンテナが再起動するとローカルの `logs.db` は削除されてしまうため、[Export](../../architecture/export.md) を設定して、データウェアハウスへ定期的にエクスポートすることを推奨します。
