# MCP Server

Nagi は [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) サーバーとして動作できます。AI エージェントが Nagi のデータ状態を確認し、収束操作を実行できるようになります。

## Tools

[`nagi mcp`](../reference/cli.md#mcp) で起動すると、以下のツールが MCP クライアントに公開されます。

| MCP tool | 対応する CLI | 権限 | `--allow-sync` |
| --- | --- | --- | --- |
| `nagi_status` | [`nagi status`](../reference/cli.md#status) | 読み取り専用 | 不要 |
| `nagi_evaluate` | [`nagi evaluate`](../reference/cli.md#evaluate) | 読み取り専用 | 不要 |
| `nagi_sync` | [`nagi sync`](../reference/cli.md#sync) | 書き込み | 必要 |

!!! tip
    デフォルトでは読み取り専用ツール（`nagi_status`、`nagi_evaluate`）のみが公開されます。`--allow-sync` を指定すると、`nagi_sync` も公開されます。

## Setup

```bash
nagi mcp                    # 読み取り専用
nagi mcp --allow-sync       # sync も許可
```

MCP サーバーは stdio で通信します。Claude Desktop や他の MCP クライアントから接続してください。

## Use Cases

AI エージェントが Nagi を操作することで、以下のようなユースケースが実現できます。

- データの状態を自然言語で問い合わせる（「daily-sales は最新ですか？」）
- Drifted な Asset を検出し、原因を分析する
- ユーザーによる承認プロセスを経たうえで sync を実行し、その結果を確認する
- Guardrails で停止された Asset の状況を把握し、次の対応を提案する
