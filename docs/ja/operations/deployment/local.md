# Local

開発・検証用のパターンです。

## Setup

```bash
nagi serve
```

`nagi serve` は起動時に自動で compile を実行します。リソース定義や `nagi.yaml` を変更した場合は `Ctrl-C` で停止し、再度実行してください。

## Storage Backend

デフォルトの `local` バックエンドを使用します。状態データは `stateDir`（デフォルト: `~/.nagi`）に保存されます。

```yaml
# nagi.yaml
backend:
  type: local
```
