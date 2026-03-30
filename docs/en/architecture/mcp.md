# MCP Server

Nagi can operate as a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server. This enables AI agents to check Nagi's data state and execute convergence operations.

## Tools

When started with [`nagi mcp`](../reference/cli.md#mcp), the following tools are exposed to MCP clients.

| MCP tool | Corresponding CLI | Permission | `--allow-sync` |
| --- | --- | --- | --- |
| `nagi_status` | [`nagi status`](../reference/cli.md#status) | Read-only | Not required |
| `nagi_evaluate` | [`nagi evaluate`](../reference/cli.md#evaluate) | Read-only | Not required |
| `nagi_sync` | [`nagi sync`](../reference/cli.md#sync) | Write | Required |

!!! tip
    By default, only read-only tools (`nagi_status`, `nagi_evaluate`) are exposed. Specifying `--allow-sync` also exposes `nagi_sync`.

## Setup

```bash
nagi mcp                    # read-only
nagi mcp --allow-sync       # also allow sync
```

The MCP server communicates via stdio. Connect from Claude Desktop or other MCP clients.

## Use Cases

AI agents can use Nagi for the following use cases:

- Query data state in natural language ("Is daily-sales up to date?")
- Detect Drifted Assets and analyze the cause
- Execute sync after a user approval process and verify the result
- Investigate Assets stopped by Guardrails and suggest next actions
