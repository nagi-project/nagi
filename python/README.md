# python/

Thin Python glue code that wraps the Rust core (`nagi-core`) as a CLI.

## Structure

- `nagi_cli/main.py` -- CLI entry point. Defines the `cli` Click group and registers all subcommands.
- `nagi_cli/commands/` -- One file per CLI command (`init`, `compile`, `evaluate`, `ls`, `sync`, `serve`, `export`, `status`). Each command delegates to functions in `nagi_cli._nagi_core`.
- `nagi_cli/mcp.py` -- MCP server integration.

The CLI is installed as the `nagi` command via the `[project.scripts]` entry in `pyproject.toml`.
