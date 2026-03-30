# Deployment

## Server

`nagi serve` は単一プロセスで動作します。実行環境に応じたデプロイパターンを以下にまとめています。

| パターン | 用途 | ストレージバックエンド |
| --- | --- | --- |
| [Local](./local.md) | 開発・検証 | local |
| [VM](./vm.md) | 常駐プロセスとして運用 | local |
| [Container](./container.md) | Cloud Run 等のコンテナ環境 | remote（GCS / S3） |

アーキテクチャの詳細は [Serve Internals](../../architecture/serve/internals.md)、ストレージバックエンドの詳細は [Storage](../../architecture/storage.md) を参照してください。

## Client

リモートストレージバックエンドを使用している場合は、サーバーとは別の環境から CLI をクライアントとして使用できます。

クライアント環境の `nagi.yaml` に、サーバーと同じリモートストレージバックエンドを設定してください。

- [`nagi status`](../../reference/cli.md#status) — 各 Asset の状態を確認
- [`nagi evaluate`](../../reference/cli.md#evaluate) / [`nagi sync`](../../reference/cli.md#sync) — 手動実行
- [`nagi serve resume`](../../reference/cli.md#serve-resume) / [`nagi serve halt`](../../reference/cli.md#serve-halt) — serve の制御
